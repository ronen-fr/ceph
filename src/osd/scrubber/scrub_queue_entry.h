// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <string_view>

#include "include/utime.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

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
 * periodic scrubs:
 * ---------------
 *
 * 'periodic_regular' - the "standard" shallow/deep scrub performed
 *      periodically on each PG.
 * *
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
 *      - the PG info is not valid (i.e. we do not have a valid 'last-scrub'
 *        stamp)
 *   or - a deep scrub is required after the previous scrub ended with errors.
 *   'must' scrubs are similar to 'operator_requested', but have a higher
 *      priority (and have a repair flag set).
 *
 * 'after_repair' - to be added later on.
 */
enum class urgency_t {
  periodic_regular,
  operator_requested,
  must_repair,
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
      : pgid{pgid}, level{level} {}

  SchedEntry(const SchedEntry&) = default;
  SchedEntry(SchedEntry&&) = default;
  SchedEntry& operator=(const SchedEntry&) = default;
  SchedEntry& operator=(SchedEntry&&) = default;

  spg_t pgid;
  scrub_level_t level;

  urgency_t urgency{urgency_t::periodic_regular};

  //// scheduled_at, not-before & the deadline times
  Scrub::scrub_schedule_t schedule;

  bool is_high_priority() const {
    return urgency == urgency_t::operator_requested ||
           urgency == urgency_t::must_repair /*||
           urgency == urgency_t::after_repair*/
        ;
  }

  /**
   * a SchedEntry is 'ripe' for scrubbing if the current time is past its
   * 'not_before' time (which guarantees it is also past its 'target').
   */
  bool is_ripe(utime_t now_is) const { return now_is >= schedule.not_before; }
};

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
      case must_repair:         desc = "must-repair"; break;
      case operator_requested:  desc = "operator-requested"; break;
      case periodic_regular:    desc = "periodic-regular"; break;
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
  auto format(const Scrub::SchedEntry& st, FormatContext& ctx) {
    return format_to(ctx.out(), "{}/{},nb:{:s},({},tr:{:s},dl:{:s})", st.pgid,
                     (st.level == scrub_level_t::deep ? "dp" : "sh"),
                     st.schedule.not_before, st.urgency,
                     st.schedule.scheduled_at, st.schedule.deadline);
  }
};
}  // namespace fmt
