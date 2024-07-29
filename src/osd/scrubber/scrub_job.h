// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <vector>

#include "common/RefCountedObj.h"
#include "common/ceph_atomic.h"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"
#include "osd/scrubber/scrub_queue_entry.h"

/**
 * The ID used to name a candidate to scrub:
 * - in this version: a PG is identified by its spg_t
 * - in the (near) future: a PG + a scrub type (shallow/deep)
 */
using ScrubTargetId = spg_t;
namespace Scrub {

enum class qu_state_t {
  not_registered,  // not a primary, thus not considered for scrubbing by this
		   // OSD (also the temporary state when just created)
  registered,	   // in either of the two queues ('to_scrub' or 'penalized')
  unregistering	   // in the process of being unregistered. Will be finalized
		   // under lock
};

struct scrub_schedule_t {
  utime_t scheduled_at{};
  utime_t deadline{0, 0};
  utime_t not_before{utime_t::max()};
};

struct sched_params_t {
  utime_t proposed_time{};
  double min_interval{0.0};
  double max_interval{0.0};
  must_scrub_t is_must{must_scrub_t::not_mandatory};
};

/**
 * a wrapper around a Scrub::SchedEntry, adding state flags and manipulators
 * to be used only by the Scrubber. Note that the SchedEntry itself is known to
 * multiple objects (also - must be kept small in size).
*/
class SchedTarget {
 public:
  explicit SchedTarget(CephContext* cct, spg_t pg_id, scrub_level_t scrub_level)
      : cct{cct}, sched_info{pg_id, scrub_level, false}
  {}

  /// resets to the after-construction state
  void reset();

  //friend ScrubJob;
  //friend struct ::fmt::formatter<Scrub::SchedTarget>;

  void clear_queued() { in_queue = false; }
  void set_queued() { in_queue = true; }
  bool is_queued() const { return in_queue; }

  bool is_high_priority() const { return sched_info.is_high_priority(); }

  //void up_urgency_to(urgency_t u);


  /// access that part of the SchedTarget that is queued in the scrub queue
  const SchedEntry& queued_element() const { return sched_info; }

  bool is_deep() const { return sched_info.level == scrub_level_t::deep; }

  bool is_shallow() const { return sched_info.level == scrub_level_t::shallow; }

  scrub_level_t level() const { return sched_info.level; }

  utime_t get_sched_time() const { return sched_info.not_before; }

  utime_t get_target_time() const { return sched_info.target; }

  utime_t get_deadline() const { return sched_info.deadline; }

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

  /**
   * urgency==off is only expected for SchedTarget objects belonging to
   * PGs that are not eligible for scrubbing (not Primaries, not clean, not
   * active)
   */
  //bool is_off() const { return sched_info.urgency == urgency_t::off; }

  bool over_deadline(utime_t now_is) const;

  /// sets 'not-before' to 'now+delay'; updates 'last_issue'
  void delay_on_failure(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now);


  /// recalculate the scheduling parameters for a periodic shallow scrub
//   void update_periodic_shallow(
//       const pg_info_t& pg_info,
//       const Scrub::sched_conf_t& config,
//       utime_t scrub_clock_now);
// 
//   void update_periodic_deep(
//       const pg_info_t& pg_info,
//       const Scrub::sched_conf_t& config,
//       utime_t scrub_time_now);
// 
//   void set_oper_shallow_target(scrub_type_t scrub_type, utime_t scrub_time_now);
// 
//   void set_oper_deep_target(scrub_type_t scrub_type, utime_t scrub_time_now);

  /// used by the fmtlib:
  template <typename FormatContext>
  auto fmt_print_ctx(FormatContext& ctx) const
  {
    return fmt::format_to(
	ctx.out(), "{},q:{},RRR", sched_info, in_queue ? "+" : "-");
    //   return fmt::format("{},q:{},ar:{},issue:{}", sched_info,
    //  	in_queue ? "+" : "-", auto_repairing ? "+" : "-", last_issue);
  };

  void dump(std::string_view sect_name, ceph::Formatter* f) const;

 private:

  CephContext* cct; ///< for logging

  /// our ID and scheduling parameters
  SchedEntry sched_info;

  /**
   * is this target (meaning - a copy of this specific combination of
   * PG and scrub type) currently in the queue?
   */
  bool in_queue{false};

  /// the reason for the latest failure/delay (for logging/reporting purposes)
  delay_cause_t last_issue{delay_cause_t::none};

  // the flags affecting the scrub that will result from this target

// RRR why not part of the sched_entry?

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



class ScrubJob final : public RefCountedObject {
 public:
  /**
   * a time scheduled for scrub, and a deadline: The scrub could be delayed
   * if system load is too high (but not if after the deadline),or if trying
   * to scrub out of scrub hours.
   */
  //scrub_schedule_t schedule;
  PgScrubber& m_scrubber;

  // a shorthand for our PG id
  spg_t m_pgid;

  /// the OSD id (for the log)
  int whoami;

  SchedTarget m_shallow_target;
  SchedTarget m_deep_target;

  ceph::atomic<qu_state_t> state{qu_state_t::not_registered};

  /**
   * the old 'is_registered'. Set whenever the job is registered with the OSD,
   * i.e. is in 'to_scrub'.
   */
  std::atomic_bool in_queues{false};

  /// how the last attempt to scrub this PG ended
  //delay_cause_t last_issue{delay_cause_t::none};

  /**
   * 'updated' is a temporary flag, used to create a barrier after
   * 'sched_time' and 'deadline' (or any other job entry) were modified by
   * different task.
   */
  std::atomic_bool updated{false};

  /**
    * the scrubber is waiting for locked objects to be unlocked.
    * Set after a grace period has passed.
    */
  bool blocked{false};
  utime_t blocked_since{};

  CephContext* cct;

  ScrubQueue& osd_scrub_queue;

  bool high_priority{false};

 public:

  ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  utime_t get_sched_time() const;// { return schedule.not_before; }

  static std::string_view qu_state_text(qu_state_t st);

  SchedTarget& closest_target(utime_t scrub_clock_now);

  const SchedTarget& closest_target(utime_t scrub_clock_now) const;




  /**
   * relatively low-cost(*) access to the scrub job's state, to be used in
   * logging.
   *  (*) not a low-cost access on x64 architecture
   */
  std::string_view state_desc() const
  {
    return qu_state_text(state.load(std::memory_order_relaxed));
  }

  int recalc_periodic_targets(
      const Scrub::sched_conf_t& aconf,
      bool modify_ready_tarets,
      utime_t scrub_time_now);

  void init_and_register(
      const Scrub::sched_conf_t& aconf,
      utime_t scrub_time_now);


  /**
   * 'reset_failure_penalty' is used to reset the 'not_before' jo attribute to
   * the updated 'scheduled_at' time. This is used whenever the scrub-job
   * schedule is updated, and the update is not a result of a scrub attempt
   * failure.
   */
  void update_schedule(
      const scrub_schedule_t& adjusted,
      bool reset_failure_penalty);

  /**
   * push the 'not_before' time out by 'delay' seconds, so that this scrub target
   * would not be retried before 'delay' seconds have passed.
   */
   /// Note: a temporary version that must have the level specified
 void delay_on_failure(
      scrub_level_t level,
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now);

  void dump(ceph::Formatter* f, utime_t now_is) const;

  /*
   * as the atomic 'in_queues' appears in many log prints, accessing it for
   * display-only should be made less expensive (on ARM. On x86 the _relaxed
   * produces the same code as '_cs')
   */
  std::string_view registration_state() const
  {
    return in_queues.load(std::memory_order_relaxed) ? "in-queue"
						     : "not-queued";
  }

  /**
   * access the 'state' directly, for when a distinction between 'registered'
   * and 'unregistering' is needed (both have in_queues() == true)
   */
  bool is_state_registered() const { return state == qu_state_t::registered; }

  /**
   * is this a high priority scrub job?
   * High priority - (usually) a scrub that was initiated by the operator
   */
  bool is_high_priority() const { return high_priority; }

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state(utime_t now_is, bool is_deep_expected) const;

  pg_scrubbing_status_t get_job_schedule(utime_t now_is) const;

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;
  const std::string log_msg_prefix;
};

using ScrubJobRef = ceph::ref_t<ScrubJob>;
using ScrubQContainer = std::vector<SchedEntry>;

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
   * upon deep-scrubs common intervals.
   */
  double deep_randomize_ratio{0.0};

  /**
   * must we schedule a scrub with high urgency if we do not have a valid
   * last scrub stamp?
   */
  bool mandatory_on_invalid{true};
};

}  // namespace Scrub

namespace std {
std::ostream& operator<<(std::ostream& out, const Scrub::ScrubJob& pg);
}  // namespace std

namespace fmt {
template <>
struct formatter<Scrub::qu_state_t> : formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const Scrub::qu_state_t& s, FormatContext& ctx)
  {
    auto out = ctx.out();
    out = fmt::formatter<string_view>::format(
	std::string{Scrub::ScrubJob::qu_state_text(s)}, ctx);
    return out;
  }
};

template <>
struct formatter<Scrub::sched_params_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_params_t& pm, FormatContext& ctx)
  {
    return fmt::format_to(
	ctx.out(), "(proposed:{:s} min/max:{:.3f}/{:.3f} must:{:2s})",
        utime_t{pm.proposed_time}, pm.min_interval, pm.max_interval,
        pm.is_must == Scrub::must_scrub_t::mandatory ? "true" : "false");
  }
};


template <>
struct formatter<Scrub::ScrubJob> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx)
  {
    return fmt::format_to(
	ctx.out(), "pg[{}] @ nb:{:s} ({:s}) (dl:{:s}) - <{}> queue state:{:.7}",
	sjob.pgid, sjob.schedule.not_before, sjob.schedule.scheduled_at,
	sjob.schedule.deadline, sjob.registration_state(),
	sjob.state.load(std::memory_order_relaxed));
  }
};

template <>
struct formatter<Scrub::sched_conf_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_conf_t& cf, FormatContext& ctx)
  {
    return fmt::format_to(
	ctx.out(),
	"periods: s:{}/{} d:{}/{} iv-ratio:{} deep-rand:{} on-inv:{}",
	cf.shallow_interval, cf.max_shallow.value_or(-1.0), cf.deep_interval,
	cf.max_deep, cf.interval_randomize_ratio, cf.deep_randomize_ratio,
	cf.mandatory_on_invalid);
  }
};
}  // namespace fmt
