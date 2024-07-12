// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <atomic>
#include <chrono>
#include <compare>
#include <iostream>
#include <memory>
#include <vector>

#include "common/ceph_atomic.h"
#include "common/fmt_common.h"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"
#include "scrub_queue_entry.h"

/**
 * The ID used to name a candidate to scrub:
 * - in this version: a PG is identified by its spg_t
 * - in the (near) future: a PG + a scrub type (shallow/deep)
 */
using ScrubTargetId = spg_t;


namespace Scrub {

enum class must_scrub_t { not_mandatory, mandatory };

struct sched_params_t {
  utime_t proposed_time{};
  must_scrub_t is_must{must_scrub_t::not_mandatory};
};

/**
 *  A collection of the configuration parameters (pool & OSD) that affect
 *  scrub scheduling.
 */
struct sched_conf_t {
  /// the desired interval between shallow scrubs
  double shallow_interval{0.0};

  /// the desired interval between deep scrubs
  double deep_interval{0.0};

  /**
   * the maximum interval between shallow scrubs, as determined by either the
   * OSD or the pool configuration. Empty if no limit is configured.
   */
  std::optional<double> max_shallow;

  /**
   * the maximum interval between deep scrubs.
   * For deep scrubs - there is no equivalent of scrub_max_interval. Per the
   * documentation, once deep_scrub_interval has passed, we are already
   * "overdue", at least as far as the "ignore allowed load" window is
   * concerned. \todo based on users complaints (and the fact that the
   * interaction between the configuration parameters is clear to no one),
   * this will be revised shortly.
   */
  double max_deep{0.0};

  /**
   * interval_randomize_ratio
   *
   * We add an extra random duration to the configured times when doing
   * scheduling. An event configured with an interval of <interval> will
   * actually be scheduled at a time selected uniformly from
   * [<interval>, (1+<interval_randomize_ratio>) * <interval>)
   */
  double interval_randomize_ratio{0.0};

  /**
   * a randomization factor aimed at preventing 'thundering herd' problems
   * upon deep-scrubs common intervals. If polling a random number smaller
   * than that percentage, the next shallow scrub is upgraded to deep.
   */
  double deep_randomize_ratio{0.0};

  /**
   * must we schedule a scrub with high urgency if we do not have a valid
   * last scrub stamp?
   */
  bool mandatory_on_invalid{true};
};


class ScrubJob;

/**
 * a wrapper around a Scrub::SchedEntry, adding state flags and manipulators
 * to be used only by the Scrubber. Note that the SchedEntry itself is known to
 * multiple objects (and must be kept small in size).
*/
class SchedTarget {
 public:
  constexpr explicit SchedTarget(spg_t pg_id, scrub_level_t scrub_level)
      : sched_info{pg_id, scrub_level}
  {}

  /// resets to the after-construction state
  void reset();

  //friend class ScrubJob;
  //friend struct ::fmt::formatter<Scrub::SchedTarget>;

  void clear_queued() { queued = false; }
  void set_queued() { queued = true; }
  bool is_queued() const { return queued; }

  bool is_high_priority() const { return sched_info.is_high_priority(); }

  void up_urgency_to(urgency_t u);


  /// access that part of the SchedTarget that is queued in the scrub queue
  const SchedEntry& queued_element() const { return sched_info; }

  bool is_deep() const { return sched_info.level == scrub_level_t::deep; }

  bool is_shallow() const { return sched_info.level == scrub_level_t::shallow; }

  scrub_level_t level() const { return sched_info.level; }

  utime_t get_sched_time() const { return sched_info.schedule.not_before; }

  bool was_delayed() const { return last_issue != delay_cause_t::none; }

  bool is_ripe(utime_t now_is) const { return sched_info.is_ripe(now_is); }

  /**
   * periodic scrubs are those with urgency of either periodic_regular or
   * (later) overdue
   */
  bool is_periodic() const;

  // scrub flags
  bool get_auto_repair() const { return auto_repairing; }
  bool get_do_repair() const { return do_repair; }

//   /**
//    * urgency==off is only expected for SchedTarget objects belonging to
//    * PGs that are not eligible for scrubbing (not Primaries, not clean, not
//    * active)
//    */
//   bool is_off() const { return sched_info.urgency == urgency_t::off; }

  bool over_deadline(utime_t now_is) const;

  /// sets 'not-before' to 'now+delay'; updates 'last_issue'
   /**
   * push the 'not_before' time out by 'delay' seconds, so that this scrub target
   * would not be retried before 'delay' seconds have passed.
   * updates 
   */
  void delay_on_failure(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now);

  /// recalculate the scheduling parameters for a periodic shallow scrub
//   void update_periodic_shallow(
//     const Scrub::sched_params_t& suggested,
//     const Scrub::sched_conf_t& app_conf,
//     //const pg_info_t& pg_info,
//     utime_t scrub_clock_now,
//     delay_ready_t modify_ready_targets);
// 
//   void update_periodic_shallow(
//       const pg_info_t& pg_info,
//       const Scrub::sched_conf_t& config,
//       utime_t scrub_time_now);

//   void update_periodic_deep(
//       const pg_info_t& pg_info,
//       const Scrub::sched_conf_t& config,
//       utime_t scrub_time_now);

//   void set_oper_shallow_target(scrub_type_t scrub_type, utime_t scrub_time_now);
// 
//   void set_oper_deep_target(scrub_type_t scrub_type, utime_t scrub_time_now);

  /// used by the fmtlib:
  std::string fmt_print() const;

 //private:
  /// our ID and scheduling parameters
  SchedEntry sched_info;

  /**
   * is this target (meaning - a copy of this specific combination of
   * PG and scrub type) currently in the queue?
   */
  bool queued{false};

  /// either 'none', or the reason for the latest failure/delay (for
  /// logging/reporting purposes)
  delay_cause_t last_issue{delay_cause_t::none};

  // the flags affecting the scrub that will result from this target:

  /**
   * (deep-scrub entries only:)
   * Supporting the equivalent of 'need-auto', which translated into:
   * - performing a deep scrub (taken care of by raising the priority of the
   *   deep target);
   * - marking that scrub as 'do_repair' (the next flag here);
   */
  bool auto_repairing{false};

  /**
   * (deep-scrub entries only:)
   * Set for scrub_requested() scrubs with the 'repair' flag set.
   * Translated (in set_op_parameters()) into a deep scrub with
   * m_is_repair & PG_REPAIR_SCRUB.
   */
  bool do_repair{false};
};


class ScrubJob {
 public:
  /// pg to be scrubbed
  spg_t pgid;

  /// the OSD id (for the log)
  int whoami;

  /*
   * the schedule for the next scrub at the specific level. Also - the
   * urgency and characteristics of the scrub (e.g. - high priority,
   * must-repair, ...)
   */
  SchedTarget shallow_target;
  SchedTarget deep_target;

  /**
   * Set whenever the PG scrubs are managed by the OSD (i.e. - from becoming
   * an active Primary till the end of the interval).
   */
  bool registered{false};

  /**
   * there is a scrub target for this PG in the queue.
   * \attn: temporary. Will be replaced with a pair of flags in the
   * two level-specific scheduling targets.
   */
  bool target_queued{false};

//   bool is_any_target_queued() const
//   {
//     return shallow_target.is_queued() || deep_target.is_queued();
//   }

  /// how the last attempt to scrub this PG ended
  delay_cause_t last_issue{delay_cause_t::none};

  /**
    * the scrubber is waiting for locked objects to be unlocked.
    * Set after a grace period has passed.
    */
  bool blocked{false};
  utime_t blocked_since{};

  CephContext* cct;

  //bool high_priority{false};

  ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  // RRR doc
  std::optional<std::reference_wrapper<SchedTarget>> earliest_eligible(utime_t scrub_clock_now) const;
  SchedTarget& earliest_target() const;

  utime_t get_sched_time() const; // RRR { return schedule.not_before; }

  std::string_view state_desc() const
  {
//     return registered ? (is_any_target_queued() ? "queued" : "registered")
// 		      : "not-registered";
    return registered ? (target_queued ? "queued" : "registered")
		      : "not-registered";
  }

  /**
   * Given a proposed time for the next scrub, and the relevant
   * configuration, adjust_schedule() determines the actual target time,
   * the deadline, and the 'not_before' time for the scrub.
   * The new values are updated into the scrub-job.
   *
   * Specifically:
   * - for high-priority scrubs: n.b. & deadline are set equal to the
   *   (untouched) proposed target time.
   * - for regular scrubs: the proposed time is adjusted (delayed) based
   *   on the configuration; the deadline is set further out (if configured)
   *   and the n.b. is reset to the target.
   */
//   void adjust_schedule(
//     const Scrub::sched_params_t& suggested,
//     const Scrub::sched_conf_t& aconf,
//     utime_t scrub_clock_now,
//     Scrub::delay_ready_t modify_ready_targets);
// 
//   void adjust_shallow_schedule(
//     const Scrub::sched_params_t& suggested,
//     const Scrub::sched_conf_t& app_conf,
//     utime_t scrub_clock_now,
//     delay_ready_t modify_ready_targets);

  void adjust_shallow_schedule2(
    utime_t last_scrub,
    const Scrub::sched_conf_t& app_conf,
    utime_t scrub_clock_now,
    delay_ready_t modify_ready_targets);

  void adjust_deep_schedule2(
    utime_t last_deep,
    const Scrub::sched_conf_t& app_conf,
    utime_t scrub_clock_now,
    delay_ready_t modify_ready_targets);

  /**
   * push the 'not_before' time out by 'delay' seconds, so that this scrub target
   * would not be retried before 'delay' seconds have passed.
   */
  void delay_on_failure(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now);

  /**
   *  Recalculating any possible updates to the scrub schedule, following an
   *  aborted scrub attempt.
   *  Usually - we can use the same schedule that triggered the aborted scrub.
   *  But we must take into account scenarios where "something" caused the
   *  parameters prepared for the *next* scrub to show higher urgency or
   *  priority. "Something" - as in an operator command requiring immediate
   *  scrubbing, or a change in the pool/cluster configuration.
   */
  void merge_and_delay(
      const scrub_schedule_t& aborted_schedule,
      Scrub::delay_cause_t issue,
      requested_scrub_t updated_flags,
      utime_t scrub_clock_now);

 /**
   * recalculate the scheduling parameters for the periodic scrub targets.
   * Used whenever the "external state" of the PG changes, e.g. when made
   * primary - or indeed when the configuration changes.
   *
   * Does not modify ripe targets.
   * (why? for example, a 'scrub pg' command following a 'deepscrub pg'
   * would otherwise push the deep scrub to the future).
   */
  void on_periods_change(
      const sched_params_t& suggested,
      const Scrub::sched_conf_t& aconf,
      utime_t scrub_clock_now) {}

  void dump(ceph::Formatter* f) const;

  bool is_registered() const { return registered; }

  /**
   * is this a high priority scrub job?
   * High priority - (usually) a scrub that was initiated by the operator
   */
  bool is_high_priority() const; // RRR { return high_priority; }

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state(utime_t now_is, bool is_deep_expected) const;

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;
  std::string log_msg_prefix;

  // the comparison operator is used to sort the scrub jobs in the queue.
  // Note that it would not be needed in the next iteration of this code, as
  // the queue would *not* hold the full ScrubJob objects, but rather -
  // SchedTarget(s).
  std::partial_ordering operator<=>(const ScrubJob& rhs) const
  {
    return schedule <=> rhs.schedule;
  };
};

using ScrubQContainer = std::vector<std::unique_ptr<ScrubJob>>;

}  // namespace Scrub

namespace std {
std::ostream& operator<<(std::ostream& out, const Scrub::ScrubJob& pg);
}  // namespace std

namespace fmt {

template <>
struct formatter<Scrub::sched_params_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_params_t& pm, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "proposed:{:s},must:{:c}", pm.proposed_time,
	pm.is_must == Scrub::must_scrub_t::mandatory ? 'y' : 'n');
  }
};

template <>
struct formatter<Scrub::ScrubJob> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "pg[{}]:nb:{:s} / trg:{:s} / dl:{:s} <{}>",
	sjob.pgid, sjob.schedule.not_before, sjob.schedule.scheduled_at,
	sjob.schedule.deadline, sjob.state_desc());
  }
};

template <>
struct formatter<Scrub::sched_conf_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_conf_t& cf, FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(),
	"periods:s:{}/{},d:{}/{},iv-ratio:{},deep-rand:{},on-inv:{}",
	cf.shallow_interval, cf.max_shallow.value_or(-1.0), cf.deep_interval,
	cf.max_deep, cf.interval_randomize_ratio, cf.deep_randomize_ratio,
	cf.mandatory_on_invalid);
  }
};
}  // namespace fmt
