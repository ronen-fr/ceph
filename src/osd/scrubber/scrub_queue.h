// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/expected.hpp"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/scrub_queue_if.h"
#include "osd/scrubber_common.h"

#include "utime.h"


namespace Scrub {
class ScrubSchedListener;
class ScrubJob;
class SchedEntry;
}  // namespace Scrub

// the interface between the OSD's queue management (i.e. ScrubQueue) and the
// specialized container implementation (i.e. XXX)
struct TargetsContainerOps {};

/**
 * the queue of PGs waiting to be scrubbed.
 */
class ScrubQueue : public Scrub::ScrubQueueOps {
 public:
  ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds);
  virtual ~ScrubQueue() = default;

  friend class TestOSDScrub;
  friend class ScrubSchedTestWrapper;  ///< unit-tests structure

  using SchedEntry = Scrub::SchedEntry;
  using SchedulingQueue = std::deque<SchedEntry>;

  std::ostream& gen_prefix(std::ostream& out) const;

  // ///////////////////////////////////////////////////
  // the ScrubQueueOps interface:

  utime_t scrub_clock_now() const override;

  Scrub::sched_conf_t populate_config_params(
      const pool_opts_t& pool_conf) override;

  void remove_entry(spg_t pgid, scrub_level_t s_or_d) final;
  void remove_entries(spg_t pgid, int known_cnt = 2) final;

  void cp_and_queue_target(SchedEntry t) final;

  void queue_entries(
      spg_t pgid,
      const SchedEntry& shallow,
      const SchedEntry& deep) final;


  // ///////////////////////////////////////////////////
  // used by the OSD:


  /**
   * the main entry point for the OSD. Called in OSD::tick_without_osd_lock()
   * to determine if there are PGs that are ready to be scrubbed, and to
   * initiate a scrub of one of those ready.
   */
  void sched_scrub(
      const ceph::common::ConfigProxy& config,
      bool is_recovery_active);

  /**
   * Translate attempt_ values into readable text
   */
  static std::string_view attempt_res_text(Scrub::schedule_result_t v);

  /*
   * handles a change to the configuration parameters affecting the scheduling
   * of scrubs.
   */
  void on_config_times_change();

 public:
  void dump_scrubs(ceph::Formatter* f);

  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() { a_pg_is_reserving = true; }
  void clear_reserving_now() { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }

  // resource reservation management

 public:
  bool can_inc_scrubs() const;
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();
  void dump_scrub_reservations(ceph::Formatter* f) const;


  /// counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg) final;
  void clear_pg_scrub_blocked(spg_t blocked_pg) final;
  int get_blocked_pgs_count() const final;

  /**
   * Pacing the scrub operation by inserting delays (mostly between chunks)
   *
   * Special handling for regular scrubs that continued into "no scrub" times.
   * Scrubbing will continue, but the delays will be controlled by a separate
   * (read - with higher value) configuration element
   * (osd_scrub_extended_sleep).
   */
  std::chrono::milliseconds required_sleep_time(bool high_priority_scrub) const;

  /**
   *  called every heartbeat to update the "daily" load average
   *
   *  @returns a load value for the logger
   */
  [[nodiscard]] std::optional<double> update_load_average();

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& osd_service;

#ifdef WITH_SEASTAR
  auto& conf() const
  {
    return local_conf();
  }
#else
  auto& conf() const
  {
    return cct->_conf;
  }
#endif

  mutable ceph::mutex jobs_lock = ceph::make_mutex("ScrubQueue::jobs_lock");

  SchedulingQueue to_scrub;

  double daily_loadavg{0.0};

  std::string log_prefix;

  tl::expected<Scrub::ScrubPreconds, Scrub::schedule_result_t>
  preconditions_to_scrubbing(
      const ceph::common::ConfigProxy& config,
      bool is_recovery_active,
      utime_t scrub_clock_now);

  bool normalize_the_queue(utime_t scrub_clock_now);

  /// scrub resources management lock (guarding scrubs_local & scrubs_remote)
  mutable ceph::mutex resource_lock =
      ceph::make_mutex("ScrubQueue::resource_lock");

  /// the counters used to manage scrub activity parallelism:
  int scrubs_local{0};
  int scrubs_remote{0};

  /**
   * The scrubbing of PGs might be delayed if the scrubbed chunk of objects is
   * locked by some other operation. A bug might cause this to be an infinite
   * delay. If that happens, the OSDs "scrub resources" (i.e. the
   * counters that limit the number of concurrent scrub operations) might
   * be exhausted.
   * We do issue a cluster-log warning in such occasions, but that message is
   * easy to miss. The 'some pg is blocked' global flag is used to note the
   * existence of such a situation in the scrub-queue log messages.
   */
  std::atomic_int_fast16_t blocked_scrubs_cnt{0};

  std::atomic_bool a_pg_is_reserving{false};

  [[nodiscard]] bool scrub_load_below_threshold() const;
  [[nodiscard]] bool scrub_time_permit() const;

 public:  // used by the unit-tests
  /**
   * unit-tests will override this function to return a mock time
   */
  virtual utime_t time_now() const
  {
    return ceph_clock_now();
  }
};
