// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
#include <string_view>
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"

// needed for the ScrubSchedListener interface, which
// should be moved to a separate file
#include "osd/scrubber/osd_scrub_sched.h"

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

class OsdScrub {

  // ctor needs: conf, log message data, ...

public:

  OsdScrub(CephContext* cct, Scrub::ScrubSchedListener& osd_svc, const ceph::common::ConfigProxy& config);

  /// select a target from the queue, and initiate a scrub
  void initiate_scrub(bool active_recovery);

  void log_fwd(std::string_view text);

private:
  CephContext* cct;
  const ceph::common::ConfigProxy& conf;
  Scrub::ScrubSchedListener& m_osd_svc;


  std::optional<Scrub::OSDRestrictions> restrictions_on_scrubbing(
      bool is_recovery_active,
      utime_t scrub_clock_now) const;

  int get_blocked_pgs_count() const;


  // resource reservation management
  Scrub::ScrubResources m_resource_bookkeeper;

  // the queue itself
  ScrubQueue m_queue;


  // the 'is_reserving_now' status: for now, I am assuming it will be
  // here. Document how the update is done.
  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() { a_pg_is_reserving = true; }
  void clear_reserving_now() { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }
  // RRR consider keeping the ID of the reserving PG, and the time it started
  std::atomic_bool a_pg_is_reserving{false};


  // maintaining the count of PGs blocked by locked objects
public:
  /// counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);

 private:
  int get_blocked_pgs_count() const;


protected:
  utime_t m_scrub_tick_time;
};



