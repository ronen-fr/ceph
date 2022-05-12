// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <gtest/gtest.h>
#include <signal.h>
#include <stdio.h>

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
#include "osd/scrubber_common.h"

/// \file testing the scrub scheduling algorithm

int main(int argc, char** argv)
{
  std::map<std::string, std::string> defaults = {
    // make sure we have 3 copies, or some tests won't work
    {"osd_pool_default_size", "3"},
    // our map is flat, so just try and split across OSDs, not hosts or whatever
    {"osd_crush_chooseleaf_type", "0"},
  };
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(&defaults,
			 args,
			 CEPH_ENTITY_TYPE_CLIENT,
			 CODE_ENVIRONMENT_UTILITY,
			 CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

using schedule_result_t = Scrub::schedule_result_t;
using ScrubJobRef = ScrubQueue::ScrubJobRef;

/// enabling access into ScrubQueue internals
class ScrubSchedWrapper : public ScrubQueue {
 public:
  ScrubSchedWrapper(Scrub::ScrubSchedListener& osds)
      : ScrubQueue(g_ceph_context, osds)
  {}
};

// providing the small number of OSD services used when scheduling
// a scrub:
class FakeOsd : public Scrub::ScrubSchedListener {
 public:
  FakeOsd(int osd_num) : m_osd_num(osd_num) {}

  int get_nodeid() const final { return m_osd_num; }

  schedule_result_t initiate_a_scrub(spg_t pgid,
				     bool allow_requested_repair_only) final
  {
    std::ignore = allow_requested_repair_only;
    auto res = m_next_response.find(pgid);
    if (res == m_next_response.end()) {
      return schedule_result_t::no_such_pg;
    }
    return m_next_response[pgid];
  }

  void set_initiation_response(spg_t pgid, schedule_result_t result)
  {
    m_next_response[pgid] = result;
  }

 private:
  int m_osd_num;
  std::map<spg_t, schedule_result_t> m_next_response;
};

struct sjob_config_t {
  spg_t spg;
  bool are_stats_valid;

  utime_t history_scrub_stamp;
  std::optional<double> pool_conf_min;
  std::optional<double> pool_conf_max;
  bool is_must;
  bool is_need_auto;
  ScrubQueue::scrub_schedule_t initial_schedule;
};

struct sjob_dynamic_data_t {
  sjob_config_t initial_config;
  pg_info_t mocked_pg_info;
  pool_opts_t mocked_pool_opts;
  requested_scrub_t request_flags;
  ScrubQueue::ScrubJobRef job;
};

class TestScrubSched : public ::testing::Test {
 public:
  TestScrubSched() = default;

 protected:
  int m_osd_num{1};
  FakeOsd m_osds{m_osd_num};
  std::unique_ptr<ScrubSchedWrapper> m_sched{new ScrubSchedWrapper(m_osds)};

  /// the pg-info is queried for stats validity and for the last-scrub-stamp
  pg_info_t pg_info{};

  /// the pool configuration holds some per-pool scrub timing settings
  pool_opts_t pool_opts{};

  /**
   * the scrub-jobs created for the tests (in real life - these
   * are owned by the respective PGs)
   */
  std::vector<sjob_dynamic_data_t> m_scrub_jobs;

 protected:
  void SetUp() override {}

  void TearDown() override {}

  sjob_dynamic_data_t create_scrub_job(sjob_config_t sjob_data)
  {
    sjob_dynamic_data_t dyn_data;
    dyn_data.initial_config = sjob_data;

    // create a 'pool options' object with the scrub timing settings
    dyn_data.mocked_pool_opts = pool_opts_t{};
    if (sjob_data.pool_conf_min) {
      dyn_data.mocked_pool_opts.set<double>(pool_opts_t::SCRUB_MIN_INTERVAL,
					    sjob_data.pool_conf_min.value());
    }
    if (sjob_data.pool_conf_max) {
      dyn_data.mocked_pool_opts.set(pool_opts_t::SCRUB_MAX_INTERVAL,
				    sjob_data.pool_conf_max.value());
    }

    // create the 'pg info' object with the stats
    dyn_data.mocked_pg_info = pg_info_t{sjob_data.spg};
    // dyn_data.mocked_pg_info.pgid = sjob_data.spg;

    dyn_data.mocked_pg_info.history.last_scrub_stamp =
      sjob_data.history_scrub_stamp;
    dyn_data.mocked_pg_info.stats.stats_invalid = !sjob_data.are_stats_valid;

    // fake hust the required 'requested-scrub' flags
    std::cout << "request_flags: sjob_data.is_must " << sjob_data.is_must
	      << std::endl;
    dyn_data.request_flags.must_scrub = sjob_data.is_must;
    dyn_data.request_flags.need_auto = sjob_data.is_need_auto;

    // create the scrub job
    dyn_data.job = ceph::make_ref<ScrubQueue::ScrubJob>(g_ceph_context,
							sjob_data.spg,
							m_osd_num);
    m_scrub_jobs.push_back(dyn_data);
    return dyn_data;
  }
};

// test data. Scrub-job creation requires a PG-id, and a set of 'scrub requests'
// flags

namespace {


std::vector<sjob_config_t> sjob_configs = {
  {
    spg_t{pg_t{1, 1}},
    true,			    // PG has valid stats
    utime_t{ceph_clock_now()},	    // last-scrub-stamp
    100.0,			    // min scrub delay in pool config
    std::nullopt,		    // max scrub delay in pool config
    false,			    // must-scrub
    false,			    // need-auto
    ScrubQueue::scrub_schedule_t{}  // initial schedule
  },

  {spg_t{pg_t{4, 1}},
   true,
   utime_t{},
   100.0,
   std::nullopt,
   true,
   false,
   ScrubQueue::scrub_schedule_t{}}};

}  // anonymous namespace

// //////////////////////////// tests ////////////////////////////////////////

TEST_F(TestScrubSched, populate_queue)
{
  ASSERT_EQ(0, m_sched->list_registered_jobs().size());

  auto dynjob_0 = create_scrub_job(sjob_configs[0]);
  auto suggested = m_sched->determine_scrub_time(dynjob_0.request_flags,
						 dynjob_0.mocked_pg_info,
						 dynjob_0.mocked_pool_opts);
  m_sched->register_with_osd(dynjob_0.job, suggested);
  std::cout << "scheduled at: " << dynjob_0.job->get_sched_time() << std::endl;
  std::cout << fmt::format("scheduled at: {}", dynjob_0.job->get_sched_time())
	    << std::endl;

  auto dynjob_1 = create_scrub_job(sjob_configs[1]);
  suggested = m_sched->determine_scrub_time(dynjob_1.request_flags,
					    dynjob_1.mocked_pg_info,
					    dynjob_1.mocked_pool_opts);
  m_sched->register_with_osd(dynjob_1.job, suggested);
  std::cout << fmt::format("scheduled at: {}", dynjob_1.job->get_sched_time())
	    << std::endl;

  // immediate_ut is required due to some EXPECT_EQ issues RRR
  utime_t immediate_ut{1,1};
  EXPECT_EQ(dynjob_1.job->get_sched_time(), immediate_ut);

  EXPECT_EQ(2, m_sched->list_registered_jobs().size());
}