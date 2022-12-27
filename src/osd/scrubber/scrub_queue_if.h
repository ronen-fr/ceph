// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

#include "utime.h"

namespace Scrub {
class ScrubSchedListener;
class ScrubJob;
class SchedEntry;


/**
 *  the interface used by all but the OSD itself to access the scrub scheduling
 *  functionality.
 */
/**
 *  the interface used by ScrubJob (a component of the PgScrubber) to access
 *  the scrub scheduling functionality.
 */
struct ScrubQueueOps {

  // a mockable ceph_clock_now(), to allow unit-testing of the scrub scheduling
  virtual utime_t scrub_clock_now() const = 0;

  virtual sched_conf_t populate_config_params(const pool_opts_t& pool_conf) = 0;

  virtual void remove_entry(spg_t pgid, scrub_level_t s_or_d) = 0;

  virtual void remove_entries(spg_t pgid, int known_cnt = 2) = 0;

  virtual void queue_entries(
      spg_t pgid,
      const SchedEntry& shallow,
      const SchedEntry& deep) = 0;

  virtual void cp_and_queue_target(SchedEntry t) = 0;

  // setting/clearing OSD-wide state flags affecting scrub scheduling

  /// counting the number of PGs stuck while scrubbing, waiting for objects
  virtual void mark_pg_scrub_blocked(spg_t blocked_pg) = 0;
  virtual void clear_pg_scrub_blocked(spg_t blocked_pg) = 0;
  virtual int get_blocked_pgs_count() const = 0;


  virtual ~ScrubQueueOps() = default;
};

}  // namespace Scrub
