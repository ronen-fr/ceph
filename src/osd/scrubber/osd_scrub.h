// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <string_view>

#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"

// needed for the ScrubSchedListener interface, which
// should be moved to a separate file
#include "osd/scrubber/osd_scrub_sched.h"
#include "osd/scrubber/scrub_resources.h"

/// off-loading scrubbing initiation logic from the OSD

/// also - CPU load as pertaining to scrubs, and scrub counters

/*

- locking:
  one global lock, maintained by the OSD service (or here?).
  - the counters have their own private lock.

- maintains number of blocked, etc.. Who updates?


*/

/*
  interface to the queue:
  - discard specific items
  - get me the top item

*/
/*
 Needs from the OSD:
 - the 'scrub services'? (i.e. the scrub queue) - is it needed? maintain it here?


*/
/// OsdScrubPgIF: the interface to the underlying queue provided to the PGs


class OsdScrub : public Scrub::OsdScrubPgIF {

  // ctor needs: conf, log message data, ...

 public:
  OsdScrub(
      CephContext* cct,
      Scrub::ScrubSchedListener& osd_svc,
      const ceph::common::ConfigProxy& config);
  virtual ~OsdScrub();	///< only required for unit-testing

  std::ostream& gen_prefix(std::ostream& out) const;

  /// select a target from the queue, and initiate a scrub
  void initiate_scrub(bool active_recovery);

  void log_fwd(std::string_view text);

  // a mockable ceph_clock_now(), to allow unit-testing of the scrub scheduling
  virtual utime_t effective_scrub_clock_now() const;

  Scrub::ScrubResources& resource_bookkeeper();
  const Scrub::ScrubResources& resource_bookkeeper() const;

  void dump_scrubs(ceph::Formatter* f) const;  ///< fwd to the queue

  void on_config_times_change();

 public:  // implementing the PGs interface to the scrub scheduling objects
  //void set_reserving_now() final;
  //void clear_reserving_now() final;

  bool inc_scrubs_local() final;
  void dec_scrubs_local() final;
  bool inc_scrubs_remote() final;
  void dec_scrubs_remote() final;

  //void mark_pg_scrub_blocked(spg_t blocked_pg) final;
  //void clear_pg_scrub_blocked(spg_t blocked_pg) final;
  int get_blocked_pgs_count() const final;

  Scrub::sched_params_t determine_scrub_time(
      const requested_scrub_t& request_flags,
      const pg_info_t& pg_info,
      const pool_opts_t& pool_conf) const final;

  void update_job(Scrub::ScrubJobRef sjob, const Scrub::sched_params_t& suggested) final;


  void register_with_osd(Scrub::ScrubJobRef sjob, const Scrub::sched_params_t& suggested) final;

  void remove_from_osd_queue(Scrub::ScrubJobRef sjob) final;

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& m_osd_svc;
  const ceph::common::ConfigProxy& conf;

  //auto& conf() const { return cct->_conf; }

  std::optional<Scrub::OSDRestrictions> restrictions_on_scrubbing(
      bool is_recovery_active,
      utime_t scrub_clock_now) const;

  // resource reservation management
  Scrub::ScrubResources m_resource_bookkeeper;

  // the queue itself
  ScrubQueue m_queue;

  std::string m_log_prefix{};

 public:  // RRR should be private? to fix
  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() final { a_pg_is_reserving = true; }
  void clear_reserving_now() final { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }

  // the 'is_reserving_now' status: for now, I am assuming it will be
  // here. Document how the update is done.
  // RRR consider keeping the ID of the reserving PG, and the time it started
  std::atomic_bool a_pg_is_reserving{false};

  /**
   * \returns true if the current time is within the scrub time window
   *
   * Using the 'current scrub time' as maintained by the ScrubQueue
   * object (which is the same as the 'current time' used by the OSD -
   * unless we are in a unit-test).
   */
  [[nodiscard]] bool scrub_time_permit() const;
  [[nodiscard]] bool scrub_time_permit(utime_t t) const;



 public:
  // maintaining the count of PGs blocked by locked objects

  void mark_pg_scrub_blocked(spg_t blocked_pg) final;
  void clear_pg_scrub_blocked(spg_t blocked_pg) final;

  std::optional<double> update_load_average();

 private:
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
