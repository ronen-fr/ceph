// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

/// \file testing the ScrubJob object (and the ScrubQueue indirectly)

#include <gtest/gtest.h>

#include <algorithm>
#include <deque>
#include <map>
#include <optional>
#include <string>

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
using string = std::string;

struct expected_entry_t {

  std::optional<bool> tst_is_valid;
  std::optional<bool> tst_is_ripe;
  std::optional<scrub_level_t> tst_level;
  std::optional<urgency_t> tst_urgency;
  std::optional<utime_t> tst_target_time;
  std::optional<utime_t> tst_target_time_min;
  std::optional<utime_t> tst_target_time_max;
  std::optional<utime_t> tst_nb_time;
  std::optional<utime_t> tst_nb_time_min;
  std::optional<utime_t> tst_nb_time_max;
};

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

  // test support

  void set_sched_conf(const sched_conf_t& conf) { m_config = conf; }
  const sched_conf_t& get_sched_conf() const { return m_config; }
  int get_queue_size() const { return m_queue.size(); }

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

  int dequeue_targets() { return ScrubJob::dequeue_targets(); }
  Scrub::SchedTarget& dequeue_target(scrub_level_t lvl)
  {
    return ScrubJob::dequeue_target(lvl);
  }

  void verify_sched_entry(const SchedEntry& e, const expected_entry_t& tst)
  {
    EXPECT_EQ(tst.tst_is_valid.value_or(e.is_valid), e.is_valid);
    EXPECT_EQ(
	tst.tst_is_ripe.value_or(e.is_ripe(scrub_queue.scrub_clock_now())),
	e.is_ripe(scrub_queue.scrub_clock_now()));
    EXPECT_EQ(tst.tst_level.value_or(e.level), e.level);
    EXPECT_EQ(tst.tst_urgency.value_or(e.urgency), e.urgency);
    EXPECT_EQ(tst.tst_target_time.value_or(e.target), e.target);
    EXPECT_LE(tst.tst_target_time_min.value_or(e.target), e.target);
    EXPECT_GE(tst.tst_target_time_max.value_or(e.target), e.target);
    EXPECT_EQ(tst.tst_nb_time.value_or(e.not_before), e.not_before);
    EXPECT_LE(tst.tst_nb_time_min.value_or(e.not_before), e.not_before);
    EXPECT_GE(tst.tst_nb_time_max.value_or(e.not_before), e.not_before);
  }
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
    true,	    // PG has valid stats
    t1,		    // sched_conf_t
    {level_config_t{// shallow
		    utime_t{std::time_t(epoch_2000 + 1'050'000), 0}},

     level_config_t{						       // deep
		    utime_t{std::time_t(epoch_2000 + 1'000'000), 0}}}  // fmt
};

expected_entry_t for_t1_shallow{
    .tst_is_valid = true,
    .tst_is_ripe = true,
    .tst_level = scrub_level_t::shallow,
    .tst_urgency = urgency_t::periodic_regular,
    .tst_target_time_min = utime_t{std::time_t(epoch_2000 + 1'050'000), 0}
    // for fmt
};

static const auto t2_base_time = epoch_2000 + 10'000'000;

// invalid stats
sjob_config_t sjob_config_2{
    spg_t{pg_t{3, 1}},
    false,	    // invalid stats
    t1,		    // sched_conf_t
    {level_config_t{// shallow
		    utime_t{std::time_t(epoch_2000 + 1'050'000), 0}},

     level_config_t{						       // deep
		    utime_t{std::time_t(epoch_2000 + 1'000'000), 0}}}  // fmt
};

expected_entry_t for_t2_deep{
    .tst_is_valid = true,
    .tst_is_ripe = false,
    .tst_level = scrub_level_t::deep,
    .tst_urgency = urgency_t::periodic_regular,
    .tst_target_time_min =
	utime_t{std::time_t(t2_base_time + 3 * t1.shallow_interval), 0},
    .tst_nb_time_max = utime_t{std::time_t(t2_base_time + 1'050'000), 0}
    // line-break for fmt
};

static const expected_entry_t for_t2_shallow{
    .tst_is_valid = true,
    .tst_is_ripe = true,
    .tst_level = scrub_level_t::shallow,
    .tst_urgency = urgency_t::must,
    .tst_target_time_min = utime_t{std::time_t(t2_base_time), 0},
    .tst_nb_time_max =
	utime_t{std::time_t(t2_base_time + 3 * t1.shallow_interval), 0}
    // line-break for fmt
};

}  // anonymous namespace


// //////////////////////////// tests ////////////////////////////////////////


TEST_F(TestScrubSchedJob, targets_creation)
{
  utime_t t_at_start{epoch_2000 + 1'000'000, 0};
  init(sjob_config_1, t_at_start);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  set_initial_targets();
  auto f = Formatter::create_unique("json-pretty");

  m_sjob->dump(f.get());
  f.get()->flush(std::cout);

  EXPECT_EQ(m_qops.get_queue_size(), 2);

  // set a time after both targets are due
  utime_t t_after{epoch_2000 + 1'600'000, 0};
  m_qops.set_time_for_testing(t_after);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  // the scheduling state should be:
  std::string exp1 = "queued for scrub";
  EXPECT_EQ(exp1, m_sjob->scheduling_state());

  m_sjob->verify_sched_entry(
      m_sjob->shallow_target.queued_element(), for_t1_shallow);
}


TEST_F(TestScrubSchedJob, invalid_history)
{
  utime_t t_at_start{t2_base_time, 0};
  init(sjob_config_2, t_at_start);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  set_initial_targets();
  auto f = Formatter::create_unique("json-pretty");

  m_sjob->dump(f.get());
  f.get()->flush(std::cout);

  EXPECT_EQ(m_qops.get_queue_size(), 2);

  utime_t t_after{t2_base_time + 100, 0};
  m_qops.set_time_for_testing(t_after);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  auto nrst = m_sjob->closest_target(m_qops.scrub_clock_now());
  m_sjob->verify_sched_entry(nrst.queued_element(), for_t2_shallow);
  m_sjob->verify_sched_entry(m_sjob->deep_target.queued_element(), for_t2_deep);

  // the scheduling state should be:
  std::string exp1 = "queued for scrub";
  EXPECT_EQ(exp1, m_sjob->scheduling_state());
}

namespace {

static const auto resfail_base_time = epoch_2000 + 31 * 24 * 3600;

sjob_config_t sjob_config_resfail{
    spg_t{pg_t{3, 1}},
    false,	    // invalid stats
    t1,		    // sched_conf_t
    {level_config_t{// shallow
		    utime_t{std::time_t(resfail_base_time - 3'600), 0}},

     level_config_t{
	 // deep
	 utime_t{std::time_t(resfail_base_time + 1'000'000), 0}}}  // fmt
};


}  // namespace

TEST_F(TestScrubSchedJob, reservation_fail)
{
  utime_t t_at_start{resfail_base_time, 0};
  init(sjob_config_resfail, t_at_start);
  std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;

  set_initial_targets();
  auto f = Formatter::create_unique("json-pretty");

  m_sjob->dump(f.get());
  f.get()->flush(std::cout);

  EXPECT_EQ(m_qops.get_queue_size(), 2);

  // suppose a shallow scrub was initiated, and failed repl reservation:
  m_sjob->dequeue_target(scrub_level_t::shallow);
  m_sjob->dequeue_target(scrub_level_t::deep);

  EXPECT_EQ(m_qops.get_queue_size(), 2);  // as they were white-out


  //   utime_t t_after{t2_base_time + 100, 0};
  //   m_qops.set_time_for_testing(t_after);
  //   std::cout << "\ntime is now: " << m_qops.scrub_clock_now() << std::endl;
  //
  //   auto nrst = m_sjob->closest_target(m_qops.scrub_clock_now());
  //   m_sjob->verify_sched_entry(nrst.queued_element(), for_t2_shallow);
  //   m_sjob->verify_sched_entry(m_sjob->deep_target.queued_element(),
  //   for_t2_deep);
  //
  //   // the scheduling state should be:
  //   std::string exp1 = "queued for scrub";
  //   EXPECT_EQ(exp1, m_sjob->scheduling_state());
}
