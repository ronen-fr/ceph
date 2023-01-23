// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

#include "utime.h"

namespace Scrub {
class ScrubSchedListener;
class SchedEntry;


/**
 *  the interface used by the PgScrubber and by the ScrubJob (a component
 *  of the PgScrubber) to access the scrub scheduling functionality.
 *  Separated from the actual implementation, mostly due to cyclic dependencies.
 */
struct ScrubQueueOps {

  // a mockable ceph_clock_now(), to allow unit-testing of the scrub scheduling
  virtual utime_t scrub_clock_now() const = 0;

  virtual void scrub_next_in_queue(utime_t loop_id) = 0;

  /**
   * let the ScrubQueue know that it should terminate the
   * current search for a scrub candidate (the search that was initiated
   * from the tick_without_osd_lock()).
   * Will be called once a triggered scrub has past the point of securing
   * replicas.
   */
  virtual void initiation_loop_done(utime_t loop_id) = 0;

  virtual sched_conf_t populate_config_params(
      const pool_opts_t& pool_conf) const = 0;

  virtual void remove_entry(spg_t pgid, scrub_level_t s_or_d) = 0;

  /**
   * add both targets to the queue (but only if urgency>off)
   * Note: modifies the entries (setting 'is_valid') before queuing them.
   * \retval false if the targets were disabled (and were not added to
   * the queue)
   * \todo when implementing a queue w/o the need for white-out support -
   * restore to const&.
   */
  virtual bool
  queue_entries(spg_t pgid, SchedEntry shallow, SchedEntry deep) = 0;

  virtual void cp_and_queue_target(SchedEntry t) = 0;

  virtual ~ScrubQueueOps() = default;
};

/**
 * a wrapper for the 'participation in the scrub scheduling loop' state.
 * A scrubber holding this object is the one currently selected by the OSD
 * (i.e. by the ScrubQueue object) to scrub. The ScrubQueue will not try
 * the next PG in the queue, until and if the current PG releases the object
 * with a 'failure' indication.
 * A success indication as the wrapper object is release will complete
 * the scheduling loop.
 */
class SchedLoopHolder {
 public:
  SchedLoopHolder(ScrubQueueOps& queue, utime_t loop_id)
      : m_loop_id{loop_id}
      , m_queue{queue}
  {}

  /*
   * the dtor will indicate 'success', as in 'do not continue the loop'.
   * It is assumed to all relevant failures call 'failure()' explicitly,
   * and the destruction of a 'loaded' object is a bug. Treating that
   * as a 'do not continue' limits the damage.
   */
  ~SchedLoopHolder();

  /*
   * The loop should be discontinued. Either because of a success
   * (the PG is now scrubbing) or because of a failure that would
   * prevent us from trying more targets
   */
  void conclude_candidates_selection();

  void go_for_next_in_queue();

  /// mostly for debugging
  std::optional<utime_t> loop_id() const { return m_loop_id; }

 private:
  // the ID of the loop (which is also the loop's original creation time)
  std::optional<utime_t> m_loop_id;

  ScrubQueueOps& m_queue;
};

}  // namespace Scrub
