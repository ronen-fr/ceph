// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <compare>
#include <string_view>
#include "include/utime.h"
#include "osd/osd_types.h"

/// \todo consider merging into scrubber_common.h

namespace Scrub {

/**
 * Possible urgency levels for a specific scheduling target (shallow or deep):
 *
 * (note: the 'urgency' attribute conveys both the relative priority for
 * scheduling and the behavior of the scrub). The urgency levels are:
 *                    ^^^^^^^^^^^^^^^^^^^^^
 *
   *** Not all urgency levels are implemented in this commit ***

 *
 * 'off' - the target is not scheduled for scrubbing. This is the initial state.
 *
 * periodic scrubs:
 * ---------------
 *
 * 'periodic_regular' - the "standard" shallow/deep scrub performed
 *      periodically on each PG.
 *
 * 'overdue' - the target is eligible for periodic scrubbing, but has not been
 *      scrubbed for a while, and the time now is past its deadline.
 *      Overdue scrubs are allowed to run even if the OSD is overloaded, and in
 *      the wrong time or day.
 *      Also - their target time is not modified upon a configuration change.
 *
 *
 * priority scrubs (termed 'required' or 'must' in the legacy code):
 * ---------------------------------------------------------------
 * Priority scrubs:
 * - are not subject to times/days/load limitations;
 * - cannot be aborted by 'noscrub'/'nodeep-scrub' flags;
 * - do not have random delays added to their target time;
 * - never have their target time modified by a configuration change;
 * - never subject to 'extended sleep time' (see scrub_sleep_time());
 *
 * 'operator_requested' - the target was manually requested for scrubbing by
 *      an administrator.
 *
 * 'must_repair' - the target is required to be scrubbed, as:
 *      - the scrub was initiated by a message specifying 'do_repair'; or
 *      - the PG info is not valid (i.e. we do not have a valid 'last-scrub' stamp)
 *   or - a deep scrub is required after the previous scrub ended with errors.
 *   'must' scrubs are similar to 'operator_requested', but have a higher
 *      priority (and have a repair flag set).
 *
 * 'after_repair' - triggered immediately after a recovery process
 *      ('m_after_repair_scrub_required' was set). The highest urgency assigned
 *      in this case assure we are not racing against any other type of scrub
 *      (and helps clarifying the PG/scrub status in the logs).
 *      This type of scrub is always deep.
 */
enum class urgency_t {
  off,
  periodic_regular,
  //overdue,
  operator_requested,
  must_repair,
  after_repair,
};


/**
 * SchedEntry holds the scheduling details for scrubbing a specific PG at
 * a specific scrub level. Namely - it identifies the [pg,level] combination,
 * the 'urgency' attribute of the scheduled scrub (which determines most of
 * its behavior and scheduling decisions) and the actual time attributes
 * for scheduling (target, deadline, not_before).
 *
 * In this commit - the 'urgency' attribute is not yet used.
 */
struct SchedEntry {
  constexpr SchedEntry(spg_t pgid, scrub_level_t level)
      : pgid{pgid}
      , level{level}
  {}

  SchedEntry(const SchedEntry&) = default;
  SchedEntry(SchedEntry&&) = default;
  SchedEntry& operator=(const SchedEntry&) = default;
  SchedEntry& operator=(SchedEntry&&) = default;

  spg_t pgid;
  scrub_level_t level;

  urgency_t urgency{urgency_t::off};

  /**
   * the time at which we are allowed to start the scrub. Never
   * decreasing after 'target' is set.
   */
  utime_t not_before{utime_t::max()};

  /**
   * the 'deadline' is the time by which we expect the periodic scrub to
   * complete. It is determined by the SCRUB_MAX_INTERVAL pool configuration
   * and by osd_scrub_max_interval;
   * Once passed, the scrub will be allowed to run even if the OSD is overloaded
   * or during no-scrub hours. It would also have higher priority than other
   * auto-scheduled scrubs.
   */
  utime_t deadline{utime_t::max()};

  /**
   * the 'target' is the time at which we intended the scrub to be scheduled.
   * For periodic (regular) scrubs, it is set to the time of the last scrub
   * plus the scrub interval (plus some randomization). Priority scrubs
   * have their own specific rules for the target time:
   * - for operator-initiated scrubs: 'target' is set to 'now';
   * - same for re-scrubbing (deep scrub after a shallow scrub that ended with
   *   errors;
   * - when requesting a scrub after a repair (the highest priority scrub):
   *   the target is set to '0' (beginning of time);
   */
  utime_t target{utime_t::max()};

  bool is_high_priority() const {
    return urgency == urgency_t::operator_requested ||
           urgency == urgency_t::must_repair ||
           urgency == urgency_t::after_repair;
  }

  /**
   * a SchedEntry is 'ripe' for scrubbing if the current time is past its
   * 'not_before' time (which guarantees it is also past its 'target').
   * It also must not be 'inactive'. i.e. must not have urgency 'off'.
   */
  bool is_ripe(utime_t now_is) const {
    return now_is >= not_before && urgency != urgency_t::off;
  }

  //void dump(std::string_view sect_name, ceph::Formatter* f) const;
};

// a minimal interface to the OSD's scrub queue for queueing/dequeueing:
struct QueueInterface {
  virtual ~QueueInterface() = default;

  //virtual void enqueue(const SchedEntry& entry) = 0;
  //virtual void dequeue(const SchedEntry& entry) = 0;

  // returns the number of entries removed from the queue
  virtual int dequeue_pg(spg_t pgid) = 0;

  virtual std::optional<Scrub::SchedEntry> dequeue_target(spg_t pgid, scrub_level_t level) = 0;

  virtual int count_queued(spg_t pgid) = 0;

  virtual void enqueue_target(spg_t pgid, const SchedEntry& e) = 0;

  virtual void enqueue_targets(
      spg_t pgid,
      const SchedEntry& shallow,
      const SchedEntry& deep) = 0;
};


static inline std::weak_ordering cmp_ripe_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r)
{
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  // the 'utime_t' operator<=> is 'partial_ordering', it seems.
  if (auto cmp = std::weak_order(double(l.target), double(r.target));
      cmp != 0) {
    return cmp;
  }
  if (auto cmp = std::weak_order(double(l.not_before), double(r.not_before));
      cmp != 0) {
    return cmp;
  }
  if (l.level < r.level) {
    return std::weak_ordering::less;
  }
  return std::weak_ordering::greater;
}

static inline std::weak_ordering cmp_future_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r)
{
  if (auto cmp = std::weak_order(double(l.not_before), double(r.not_before));
      cmp != 0) {
    return cmp;
  }
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  if (auto cmp = std::weak_order(double(l.target), double(r.target));
      cmp != 0) {
    return cmp;
  }
  if (l.level < r.level) {
    return std::weak_ordering::less;
  }
  return std::weak_ordering::greater;
}

static inline std::weak_ordering
cmp_entries(utime_t t, const Scrub::SchedEntry& l, const Scrub::SchedEntry& r)
{
  bool l_ripe = l.is_ripe(t);
  bool r_ripe = r.is_ripe(t);
  if (l_ripe) {
    if (r_ripe) {
      return cmp_ripe_entries(l, r);
    }
    return std::weak_ordering::less;
  }
  if (r_ripe) {
    return std::weak_ordering::greater;
  }
  return cmp_future_entries(l, r);
}


}  // namespace Scrub


namespace fmt {

// clang-format off
template <>
struct formatter<Scrub::urgency_t> : formatter<std::string_view> {
  template <typename FormatContext>
  auto format(Scrub::urgency_t urg, FormatContext& ctx)
  {
    using enum Scrub::urgency_t;
    std::string_view desc;
    switch (urg) {
      case after_repair:        desc = "after-repair"; break;
      case must_repair:         desc = "must-repair"; break;
      case operator_requested:  desc = "operator-requested"; break;
      //case overdue:             desc = "overdue"; break;
      case periodic_regular:    desc = "periodic-regular"; break;
      case off:                 desc = "off"; break;
      // better to not have a default case, so that the compiler will warn
    }
    return formatter<string_view>::format(desc, ctx);
  }
};
// clang-format on

template <>
struct formatter<Scrub::SchedEntry> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedEntry& st, FormatContext& ctx)
  {
    return format_to(
	ctx.out(), "{}/{},nb:{:s},({},tr:{:s},dl:{:s})", st.pgid,
	(st.level == scrub_level_t::deep ? "dp" : "sh"), st.not_before,
	st.urgency, st.target, st.deadline);
  }
};
} // namespace fmt

