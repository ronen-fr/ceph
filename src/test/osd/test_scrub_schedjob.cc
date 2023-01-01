// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/// \file testing the ScrubJob object (and the ScrubQueue indirectly)

#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <map>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "include/utime_fmt.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "os/ObjectStore.h"
#include "osd/PG.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/osd_scrub_sched.h"
#include "osd/scrubber/scrub_queue_if.h"
#include "osd/scrubber_common.h"

int main(int argc, char** argv)
{
  std::map<std::string, std::string> defaults = {
      // make sure we have 3 copies, or some tests won't work
      {"osd_pool_default_size", "3"},
      // our map is flat, so just try and split across OSDs, not hosts or
      // whatever
      {"osd_crush_chooseleaf_type", "0"},
  };
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(
      &defaults, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY,
      CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

using sched_conf_t = Scrub::sched_conf_t;
using SchedEntry = Scrub::SchedEntry;
using ScrubJob = Scrub::ScrubJob;
using urgency_t = Scrub::urgency_t;

/**
 *  implementing the ScrubQueue services used by the ScrubJob
 */
struct ScrubQueueOpsImp : public Scrub::ScrubQueueOps {

  // clock issues

  void set_time_for_testing(long faked_now)
  {
    m_time_for_testing = utime_t{timeval{faked_now}};
  }

  void clear_time_for_testing() { m_time_for_testing.reset(); }

  mutable std::optional<utime_t> m_time_for_testing;

  utime_t scrub_clock_now() const final
  {
    if (m_time_for_testing) {
      m_time_for_testing->tv.tv_nsec += 1'000'000;
    }
    return m_time_for_testing.value_or(ceph_clock_now());
  }


  sched_conf_t populate_config_params(const pool_opts_t& pool_conf) override
  {
    return m_config;
  }

  void remove_entry(spg_t pgid, scrub_level_t s_or_d)
  {
    auto it = std::find_if(
	m_queue.begin(), m_queue.end(), [pgid, s_or_d](const SchedEntry& e) {
	  return e.pgid == pgid && e.level == s_or_d && e.is_valid;
	});
    if (it != m_queue.end()) {
      it->is_valid = false;
    }
  }

  /**
   * add both targets to the queue (but only if urgency>off)
   * Note: modifies the entries (setting 'is_valid') before queuing them.
   * \todo when implementing a queue w/o the need for white-out support -
   * restore to const&.
   */
  void queue_entries(spg_t pgid, SchedEntry shallow, SchedEntry deep) override
  {
    ASSERT_TRUE(shallow.level == scrub_level_t::shallow);
    ASSERT_TRUE(deep.level == scrub_level_t::deep);

    if (shallow.urgency == urgency_t::off || deep.urgency == urgency_t::off) {
      return;
    }

    // we should not have these entries in the queue already
    ASSERT_TRUE(!is_in_queue(pgid, scrub_level_t::shallow));
    ASSERT_TRUE(!is_in_queue(pgid, scrub_level_t::deep));

    // set the 'is_valid' flag
    shallow.is_valid = true;
    deep.is_valid = true;

    // and queue
    m_queue.push_back(shallow);
    m_queue.push_back(deep);
  }

  void cp_and_queue_target(SchedEntry t) override
  {
    ASSERT_TRUE(t.urgency != urgency_t::off);
    t.is_valid = true;
    m_queue.push_back(t);
  }

  void set_sched_conf(const sched_conf_t& conf) { m_config = conf; }
  const sched_conf_t& get_sched_conf() const { return m_config; }

 private:
  std::deque<SchedEntry> m_queue;

  sched_conf_t m_config;

  bool is_in_queue(spg_t pgid, scrub_level_t level)
  {
    auto it = std::find_if(
	m_queue.begin(), m_queue.end(), [pgid, level](const SchedEntry& e) {
	  return e.pgid == pgid && e.level == level && e.is_valid;
	});
    return it != m_queue.end();
  }
};


// ///////////////////////////////////////////////////
// ScrubJob

class ScrubJobTestWrapper : public ScrubJob {
 public:
  ScrubJobTestWrapper(ScrubQueueOpsImp& qops, const spg_t& pg, int osd_num)
      : ScrubJob(qops, g_ceph_context, pg, osd_num)
  {}
};

struct level_config_t {
  utime_t history_stamp;
  // std::optional<double> pool_conf_min;
  // std::optional<double> pool_conf_max;
};

struct sjob_config_t {
  spg_t spg;
  bool are_stats_valid;
  sched_conf_t sched_cnf;

  std::array<level_config_t, 2> levels;	 // 0=shallow, 1=deep
  // bool is_must;
  // bool is_need_auto;
  //  ScrubQueue::scrub_schedule_t initial_schedule;
};

struct sjob_dynamic_data_t {
  sjob_config_t initial_config;
  pg_info_t mocked_pg_info;
  pool_opts_t mocked_pool_opts;
  //   requested_scrub_t request_flags;
  //   ScrubQueue::ScrubJobRef job;
};


class TestScrubSchedJob : public ::testing::Test {
 public:
  TestScrubSchedJob() = default;

  void init(const sjob_dynamic_data_t& dyn_data)
  {
    m_dyn_data = dyn_data;

    // the pg-info is queried for stats validity and for the last-scrub-stamp
    pg_info = dyn_data.mocked_pg_info;

    // the pool configuration holds some per-pool scrub timing settings
    pool_opts = dyn_data.mocked_pool_opts;

    // the scrub-job is created with the initial configuration
    m_sjob = std::make_unique<ScrubJobTestWrapper>(m_qops, tested_pg, 1);
  }

  void set_initial_targets(sched_conf_t aconf)
  {
    ASSERT_TRUE(m_sjob);
    m_sjob->init_and_queue_targets(pg_info, aconf, m_qops.scrub_clock_now());
  }

  void init(const sjob_config_t& dt, utime_t starting_at)
  {
    // the pg-info is queried for stats validity and for the last-scrub-stamp
    pg_info = create_pg_info(dt);

    m_qops.set_sched_conf(dt.sched_cnf);
    m_qops.set_time_for_testing(starting_at);

    // the scrub-job is created with the initial configuration
    m_sjob = std::make_unique<ScrubJobTestWrapper>(m_qops, dt.spg, 1);
  }

  void set_initial_targets()
  {
    ASSERT_TRUE(m_sjob);
    m_sjob->init_and_queue_targets(
	pg_info, m_qops.get_sched_conf(), m_qops.scrub_clock_now());
  }

  // sched_conf_t create_sched_conf(const sjob_config_t& s);

  pg_info_t create_pg_info(const sjob_config_t& s)
  {
    pg_info_t pginf;
    pginf.pgid = s.spg;
    pginf.history.last_scrub_stamp = s.levels[0].history_stamp;
    pginf.stats.last_scrub_stamp = s.levels[0].history_stamp;
    pginf.history.last_deep_scrub_stamp = s.levels[1].history_stamp;
    pginf.stats.last_deep_scrub_stamp = s.levels[1].history_stamp;
    pginf.stats.stats_invalid = !s.are_stats_valid;
    return pginf;
  }

 protected:
  int m_num_osds{3};
  int my_osd_id{1};

  sjob_dynamic_data_t m_dyn_data{};

  // FakeOsd m_osds{m_osd_num};

  spg_t tested_pg;
  ScrubQueueOpsImp m_qops;
  std::unique_ptr<ScrubJobTestWrapper> m_sjob;

  /// the pg-info is queried for stats validity and for the last-scrub-stamp
  pg_info_t pg_info{};

  /// the pool configuration holds some per-pool scrub timing settings
  pool_opts_t pool_opts{};
};


// ///////////////////////////////////////////////////////////////////////////
// test data.

namespace {

// the times used during the tests are offset to 1.1.2000, so that
// utime_t formatting will treat them as absolute (not as a relative time)
static const auto epoch_2000 = 946'684'800;

sched_conf_t t1{
    .shallow_interval = 24 * 3600.0,
    .deep_interval = 7 * 24 * 3600.0,
    .max_shallow = 2 * 24 * 3600.0,
    .max_deep = 7 * 24 * 3600.0,
    .interval_randomize_ratio = 0.2};

sjob_config_t sjob_config_1{
    spg_t{pg_t{1, 1}},
    true,
    t1,
    {		    // PG has valid stats
     level_config_t{// shallow
		    utime_t{std::time_t(epoch_2000 + 1'050'000), 0}},
     // deep
     level_config_t{utime_t{std::time_t(epoch_2000 + 1'000'000), 0}}}};

std::vector<sjob_config_t> sjob_configs = {{
    spg_t{pg_t{1, 1}},
    true,
    t1,
    {// PG has valid stats
     level_config_t{
	 // shallow
	 utime_t{std::time_t(epoch_2000 + 1'050'000), 0}  // last-scrub-stamp
							  // 100.0,	 // min
							  // scrub delay in pool
							  // config 200.0,
							  // // max scrub delay
							  // in pool config
     },
     // deep
     level_config_t{
	 utime_t{std::time_t(epoch_2000 + 1'000'000), 0}  // last-scrub-stamp
							  // 500.0,	 // min
							  // scrub delay in pool
							  // config 600.0,
							  // // max scrub delay
							  // in pool config
     }}

    //     {spg_t{pg_t{4, 1}}, true, utime_t{epoch_2000 + 1'000'000, 0}, 100.0,
    //      std::nullopt, true, false},
    //
    //     {spg_t{pg_t{7, 1}}, true, utime_t{}, 1.0, std::nullopt, false,
    //     false},
    //
    //     {spg_t{pg_t{5, 1}}, true, utime_t{epoch_2000 + 1'900'000, 0}, 1.0,
    //      std::nullopt, false, false}
}};

sjob_dynamic_data_t dyn_1{
    sjob_configs[0],


};


}  // anonymous namespace


// //////////////////////////// tests ////////////////////////////////////////


TEST_F(TestScrubSchedJob, targets_creation)
{
  init(sjob_config_1, utime_t{epoch_2000 + 1'000'000, 0});

  set_initial_targets();

  Formatter* f = Formatter::create("json");
  m_sjob->dump(f);
  f->flush(std::cout);
}

TEST_F(TestScrubSchedJob, targets_creation2)
{
  auto dt = sjob_config_1;
  m_qops.set_sched_conf(dt.sched_cnf);
  m_qops.set_time_for_testing(utime_t{epoch_2000 + 1'000'000, 0});
  pg_info_t pginf = create_pg_info(dt);
  tested_pg = dt.spg;
  m_sjob = std::make_unique<ScrubJobTestWrapper>(m_qops, tested_pg, 1);

  set_initial_targets(dt.sched_cnf);

  Formatter* f = Formatter::create("json");
  m_sjob->dump(f);
  f->flush(std::cout);
}
