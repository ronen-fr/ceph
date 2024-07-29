// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <compare>
#include <string_view>

#include "include/utime.h"
#include "osd/osd_types.h"

namespace Scrub {

enum class must_scrub_t : uint_fast8_t { not_mandatory, mandatory };

class SchedTarget;

/**
 * SchedEntry holds the scheduling details for scrubbing a specific PG at
 * a specific scrub level. Namely - it identifies the [pg,level] combination,
 * and the actual time attributes for scheduling (target, deadline,
 * not_before).
 *
 * In this commit - the 'urgency' attribute is not yet used.
 * Instead - a separate 'high-priority' flag is used to determine if the
 * scrub is a priority scrub (i.e. 'must_repair', 'operator_requested' or
 * 'after_repair').
 */
class SchedEntry {
  constexpr SchedEntry(spg_t pgid, scrub_level_t level, bool high_priority)
      : pgid{pgid}
      , level{level}
      , high_priority{high_priority}
  {}

  SchedEntry(const SchedEntry&) noexcept = default;
  SchedEntry(SchedEntry&&) noexcept = default;
  SchedEntry& operator=(const SchedEntry&) noexcept = default;
  SchedEntry& operator=(SchedEntry&&) noexcept = default;

  // the Scrubber accesses SchedEntry(s) thru a wrapper:
  friend class SchedTarget;

  // the pgid and level members are immutable.
  spg_t pgid;
  scrub_level_t level;

  bool high_priority;

  /**
   * the time at which we are allowed to start the scrub. Never
   * decreasing after 'target' is set.
   */
  utime_t not_before{utime_t::max()};

  /**
   * the 'deadline' is the time by which we expect the periodic scrub to
   * complete. It is determined by the SCRUB_MAX_INTERVAL pool configuration
   * and by osd_scrub_max_interval;
   * Once passed, the scrub will be allowed to run even if the OSD is
   * overloaded. It would also have higher local priority than other
   * auto-scheduled scrubs scheduled for this OSD as primary.
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

 public:
  utime_t get_not_before() const { return not_before; }
  utime_t get_target() const { return target; }
  utime_t get_deadline() const { return deadline; }

  bool is_high_priority() const { return high_priority; }

  /**
   * a SchedEntry is 'ripe' for scrubbing if the current time is past its
   * 'not_before' time (which guarantees it is also past its 'target').
   */
  bool is_ripe(utime_t now_is) const { return now_is >= not_before; }
};

}  // namespace Scrub


#if 0
namespace {
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
} // namespace
#endif


namespace fmt {

// // clang-format off
// template <>
// struct formatter<Scrub::urgency_t> : formatter<std::string_view> {
//   template <typename FormatContext>
//   auto format(Scrub::urgency_t urg, FormatContext& ctx)
//   {
//     using enum Scrub::urgency_t;
//     std::string_view desc;
//     switch (urg) {
//       case after_repair:        desc = "after-repair"; break;
//       case must_repair:         desc = "must-repair"; break;
//       case operator_requested:  desc = "operator-requested"; break;
//       //case overdue:             desc = "overdue"; break;
//       case periodic_regular:    desc = "periodic-regular"; break;
//       case off:                 desc = "off"; break;
//       // better to not have a default case, so that the compiler will warn
//     }
//     return formatter<string_view>::format(desc, ctx);
//   }
// };
// // clang-format on

template <>
struct formatter<Scrub::SchedEntry> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedEntry& st, FormatContext& ctx)
  {
    return format_to(
	ctx.out(), "{}/{},nb:{:s},({},tr:{:s},dl:{:s})", st.pgid,
	(st.level == scrub_level_t::deep ? "dp" : "sh"), st.not_before,
	high_priority, st.target, st.deadline);
  }
};
}  // namespace fmt
