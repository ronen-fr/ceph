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
class QSchedTarget;


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

  using QSchedTarget = Scrub::QSchedTarget;
  using SchedulingQueue = std::deque<QSchedTarget>;

  std::ostream& gen_prefix(std::ostream& out) const;

  // ///////////////////////////////////////////////////
  // the ScrubQueueOps interface:

  utime_t scrub_clock_now() const override;

  Scrub::sched_conf_t populate_config_params(
      const pool_opts_t& pool_conf) override;

  void remove_entry(spg_t pgid, scrub_level_t s_or_d) final;
  void remove_entries(spg_t pgid, int known_cnt = 2) final;

  void cp_and_queue_target(QSchedTarget t) final;

  void queue_entries(
      spg_t pgid,
      const QSchedTarget& shallow,
      const QSchedTarget& deep) final;


  // ///////////////////////////////////////////////////
  // used by the OSD:


  /**
   * the main entry point for the OSD. Called in OSD::tick_without_osd_lock()
   * to determine if there are PGs that are ready to be scrubbed, and to
   * initiate a scrub session on one of them.
   */
  void sched_scrub(
      const ceph::common::ConfigProxy& config,
      bool is_recovery_active);

  /**
   * Translate attempt_ values into readable text
   */
  static std::string_view attempt_res_text(Scrub::schedule_result_t v);

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  //void remove_from_osd_queue(Scrub::ScrubJobRef sjob);

  /**
   * @return the list (not std::list!) of all scrub jobs registered
   *   (apart from PGs in the process of being removed)
   */
  SchedulingQueue list_registered_jobs() const;

  /**
   * Add the scrub job to the list of jobs (i.e. list of PGs) to be periodically
   * scrubbed by the OSD.
   * The registration is active as long as the PG exists and the OSD is its
   * primary.
   *
   * See update_job() for the handling of the 'suggested' parameter.
   *
   * locking: might lock jobs_lock
   */
  //void register_with_osd(Scrub::ScrubJobRef sjob);

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

  /**
   * called periodically(*) to select the first scrub-eligible PG
   * and scrub it.
   *
   * (*) by the OSD's tick_without_osd_lock() method, indirectly via
   *    sched_scrub();
   *
   * Selection is affected by:
   * - time of day: scheduled scrubbing might be configured to only happen
   *   during certain hours;
   * - same for days of the week, and for the system load;
   *
   * @param preconds: what types of scrub are allowed, given system status &
   *                  config. Some of the preconditions are calculated here.
   * @return Scrub::schedule_result_t::scrub_initiated if a scrub session was
   *                  successfully initiated. Otherwise - the failure cause.
   *
   * locking: locks jobs_lock
   */
  //Scrub::schedule_result_t select_pg_and_scrub(Scrub::ScrubPreconds& preconds);

  /**
   * clear dead entries (unregistered, or belonging to removed PGs) from a
   * queue. Job state is changed to match new status.
   */
  void rm_unregistered_jobs();

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
