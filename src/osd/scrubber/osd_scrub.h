// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <string_view>

#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"
#include "osd/scrubber/scrub_resources.h"

// needed for the ScrubSchedListener interface, which
// should be moved to a separate file
#include "osd/scrubber/osd_scrub_sched.h"

/**
 *  Off-loading scrubbing initiation logic from the OSD.
 *  Also here: CPU load as pertaining to scrubs, and the scrub
 *  resource counters.
 *
 *  Locking:
 *  (as of this first step in the scheduler refactoring)
 *  - No protected data is maintained directly by the OsdScrub object
 *    (as it is not yet protected by a single OSDservice lock).
 */
class OsdScrub {
 public:
  OsdScrub(
      CephContext* cct,
      Scrub::ScrubSchedListener& osd_svc,
      const ceph::common::ConfigProxy& config);

  ~OsdScrub() = default;

  std::ostream& gen_prefix(std::ostream& out) const;

  /**
   *  select a target from the queue, and initiate a scrub
   */
  void initiate_scrub(bool active_recovery);

  void log_fwd(std::string_view text);

  const Scrub::ScrubResources& resource_bookkeeper() const {
    return m_resource_bookkeeper;
  }

  void dump_scrubs(ceph::Formatter* f) const;  ///< fwd to the queue

  // RRR document
  void on_config_change();

  //
  // implementing the PGs interface to the scrub scheduling objects
  //
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();

  /// counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);

  // update scheduling information for a specific PG
  Scrub::sched_params_t determine_scrub_time(
      const requested_scrub_t& request_flags,
      const pg_info_t& pg_info,
      const pool_opts_t& pool_conf) const;

  /**
   * modify a scrub-job's scheduled time and deadline
   *
   * There are 3 argument combinations to consider:
   * - 'must' is asserted, and the suggested time is 'scrub_must_stamp':
   *   the registration will be with "beginning of time" target, making the
   *   scrub-job eligible to immediate scrub (given that external conditions
   *   do not prevent scrubbing)
   *
   * - 'must' is asserted, and the suggested time is 'now':
   *   This happens if our stats are unknown. The results are similar to the
   *   previous scenario.
   *
   * - not a 'must': we take the suggested time as a basis, and add to it some
   *   configuration / random delays.
   *
   *  ('must' is Scrub::sched_params_t.is_must)
   *
   *  locking: not using the jobs_lock
   */
  void update_job(
      Scrub::ScrubJobRef sjob,
      const Scrub::sched_params_t& suggested);

  // RRR document
  void register_with_osd(
      Scrub::ScrubJobRef sjob,
      const Scrub::sched_params_t& suggested);

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(Scrub::ScrubJobRef sjob);

  /**
   * scrub_sleep_time
   *
   * Returns std::chrono::milliseconds indicating how long to wait between
   * chunks.
   *
   * Implementation Note: Returned value will either osd_scrub_sleep or
   * osd_scrub_extended_sleep depending on must_scrub_param and time
   * of day (see configs osd_scrub_begin*)
   */
  std::chrono::milliseconds scrub_sleep_time(
      bool high_priority_scrub) const;

  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now();
  void clear_reserving_now();

  /**
   * \returns true if the current time is within the scrub time window
   *
   * Using the 'current scrub time' as maintained by the ScrubQueue
   * object (which is the same as the 'current time' used by the OSD -
   * unless we are in a unit-test).
   */
  [[nodiscard]] bool scrub_time_permit() const;
  [[nodiscard]] bool scrub_time_permit(utime_t t) const;

  std::optional<double> update_load_average();

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& m_osd_svc;
  const ceph::common::ConfigProxy& conf;

  std::optional<Scrub::OSDRestrictions> restrictions_on_scrubbing(
      bool is_recovery_active,
      utime_t scrub_clock_now) const;

  Scrub::schedule_result_t initiate_a_scrub(
      spg_t pgid,
      bool allow_requested_repair_only);

  /// resource reservation management
  Scrub::ScrubResources m_resource_bookkeeper;

  // the queue of PGs waiting to be scrubbed
  ScrubQueue m_queue;

  const std::string m_log_prefix{};

  bool is_reserving_now() const;

  /// number of PGs stuck while scrubbing, waiting for objects
  int get_blocked_pgs_count() const;

  /**
   * tracking the average load on the CPU. Used both by the
   * OSD logger, and by the scrub queue (as no scrubbing is allowed if
   * the load is too high).
   */
  class LoadTracker {
    CephContext* cct;
    const ceph::common::ConfigProxy& conf;
    std::string log_prefix;
    double daily_loadavg{0.0};

   public:
    explicit LoadTracker(
	CephContext* cct,
	const ceph::common::ConfigProxy& config,
	int node_id);

    std::optional<double> update_load_average();

    [[nodiscard]] bool scrub_load_below_threshold() const;

    std::ostream& gen_prefix(std::ostream& out) const;
  };
  LoadTracker m_load_tracker;

 protected:
  utime_t m_scrub_tick_time;
};
