// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <string_view>
#include <vector>

#include "common/ceph_atomic.h"
#include "common/fmt_common.h"
#include "common/RefCountedObj.h"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/scrub_queue_entry.h"
#include "osd/scrubber_common.h"

class PgScrubber;
class OsdScrub;

/**
 * The ID used to name a candidate to scrub:
 * - now - a PG+scrub level
 */
using ScrubTargetId = Scrub::SchedEntry;


/*



Missing - all instances where merge_active_back() & merge_and_delay() are
 needed.





















*/

namespace Scrub {

enum class must_scrub_t { not_mandatory, mandatory };


// struct scrub_schedule_t {
//   utime_t scheduled_at{};
//   utime_t deadline{0, 0};
// };


struct sched_params_t {
  utime_t proposed_time{};
  double min_interval{0.0};
  double max_interval{0.0};
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


/**
 * the result of the last attempt to schedule a scrub for a specific PG.
 * The enum value itself is mostly used for logging purposes.
 */
enum class delay_cause_t {
  none,		    ///< scrub attempt was successful
  replicas,	    ///< failed to reserve replicas
  flags,	    ///< noscrub or nodeep-scrub
  pg_state,	    ///< e.g. snap-trimming
  time,		    ///< time restrictions or busy CPU
  local_resources,  ///< too many scrubbing PGs
  aborted,	    ///< scrub was aborted on no(deep)-scrub
  backend_error,    ///< data access failure reported by the backend
  interval,	    ///< the interval had ended mid-scrub
};

class ScrubJob;

/**
 * a wrapper around a Scrub::SchedEntry, adding state flags and manipulators
 * to be used only by the Scrubber. Note that the SchedEntry itself is known to
 * multiple objects (and must be kept small in size).
*/
class SchedTarget {
 public:
  explicit SchedTarget(spg_t pg_id, scrub_level_t scrub_level)
      : sched_info{pg_id, scrub_level}
  {}

  /// resets to the after-construction state
  void reset();

  friend class ScrubJob;
  friend struct ::fmt::formatter<Scrub::SchedTarget>;

  void clear_queued() { in_queue = false; }
  void set_queued() { in_queue = true; }
  bool is_queued() const { return in_queue; }

  bool is_high_priority() const { return sched_info.is_high_priority(); }

  void up_urgency_to(urgency_t u);


  /// access that part of the SchedTarget that is queued in the scrub queue
  const SchedEntry& queued_element() const { return sched_info; }

  bool is_deep() const { return sched_info.level == scrub_level_t::deep; }

  bool is_shallow() const { return sched_info.level == scrub_level_t::shallow; }

  scrub_level_t level() const { return sched_info.level; }

  utime_t get_sched_time() const { return sched_info.not_before; }

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
  bool is_off() const { return sched_info.urgency == urgency_t::off; }

  bool over_deadline(utime_t now_is) const;

  /// sets 'not-before' to 'now+delay'; updates 'last_issue'
  void push_nb_out(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now);


  /// recalculate the scheduling parameters for a periodic shallow scrub
  void update_periodic_shallow(
      const pg_info_t& pg_info,
      const Scrub::sched_conf_t& config,
      utime_t scrub_time_now);

  void update_periodic_deep(
      const pg_info_t& pg_info,
      const Scrub::sched_conf_t& config,
      utime_t scrub_time_now);

  void set_oper_shallow_target(scrub_type_t scrub_type, utime_t scrub_time_now);

  void set_oper_deep_target(scrub_type_t scrub_type, utime_t scrub_time_now);

  /// used by the fmtlib:
  std::string fmt_print() const;

 private:
  /// our ID and scheduling parameters
  SchedEntry sched_info;

  /**
   * is this target (meaning - a copy of his specific combination of
   * PG and scrub type) currently in the queue?
   */
  bool in_queue{false};

  /// the reason for the latest failure/delay (for logging/reporting purposes)
  delay_cause_t last_issue{delay_cause_t::none};

  // the flags affecting the scrub that will result from this target

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
// the scrub-job can now count on the scrubber to be there

class ScrubJob {
 public:
  friend PgScrubber;

  ScrubJob(PgScrubber& scrubber, const spg_t& pg);

  void reset();

  /**
   * this PG (this ScrubJob) is being scrubbed now.
   * Set to true immediately after set_op_parameters() committed us
   * to a scrub.
   */
  bool scrubbing{false};

  bool in_queue() const;

  void mark_target_dequeued(scrub_level_t scrub_level);

  /// "in-queue" if in_queue() is true, "not-queued" otherwise
  std::string_view registration_state() const;

  SchedTarget& get_target(scrub_level_t lvl);
  SchedTarget& get_other_target(scrub_level_t lvl);

  void init_and_register(
      const Scrub::sched_conf_t& aconf,
      utime_t scrub_time_now);

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
      const Scrub::sched_conf_t& aconf,
      utime_t scrub_time_now);

  void at_scrub_completion(
      const Scrub::sched_conf_t& aconf,
      utime_t scrub_time_now);

  void on_abort(
      SchedTarget&& aborted_target,
      delay_cause_t issue,
      utime_t scrub_time_now);

  /**
  * Both shallow and deep scrub targets are removed from the OSD queue.
  * Note - the local copies of these targets are marked as dequeued, but
  * are not reset. This is done to preserve any high-urgency status they
  * might have, as we do not know at this point whether we will not continue
  * as Primary for this PG.
  */
  void remove_from_queue();

  // RRR doc
  void mark_for_rescrubbing();

  // RRR doc
  void mark_for_after_repair();

  void operator_forced_targets(
      scrub_level_t scrub_level,
      scrub_type_t scrub_type,
      utime_t scrub_time_now);

  SchedTarget& closest_target(utime_t scrub_clock_now);

  const SchedTarget& closest_target(utime_t scrub_clock_now) const;

  // return a concise description of the scheduling state of this PG
  pg_scrubbing_status_t get_schedule(utime_t now_is) const;

  /// used by the fmtlib:
  std::string alt_fmt_print(bool short_output) const;

  static std::string parse_sched_state(const pg_scrubbing_status_t& sched_stat);

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;

  /**
   * restore the named target to the queue.
   * Used after that target was dequeued to be scrubbed, but the scrub has
   * failed to start - but before set_op_parameters() was called.
   * Note that the not-before was probably changed (unless the scrub had
   * failed due to a local resources failure).
   */
  void restore_target(scrub_level_t level);

 private:
  PgScrubber& m_scrubber;

  // a shorthand for our PG id
  const spg_t m_pgid;

  SchedTarget m_shallow_target;
  SchedTarget m_deep_target;


  // -----       shorthand

  //ScrubQueue& m_scrub_queue;

  OsdScrub& m_osd_scrub;

  QueueInterface& m_queue;

  /// pg to be scrubbed
  //const spg_t pgid;

  /// the OSD id (for the log)
  //const int whoami;

  //ceph::atomic<qu_state_t> state{qu_state_t::not_registered};

  /**
   * the old 'is_registered'. Set whenever the job is registered with the OSD,
   * i.e. is in either the 'to_scrub' or the 'penalized' vectors.
   */
  //std::atomic_bool in_queues{false};

  /// last scrub attempt failed to secure replica resources
  // why not handle directly? bool resources_failure{false};

  /**
   * 'updated' is a temporary flag, used to create a barrier after
   * 'sched_time' and 'deadline' (or any other job entry) were modified by
   * different task.
   * 'updated' also signals the need to move a job back from the penalized
   * queue to the regular one.
   */
  //std::atomic_bool updated{false};

  /**
   RRR move to the scrubber itself?
    * the scrubber is waiting for locked objects to be unlocked.
    * Set after a grace period has passed.
    */
  bool blocked{false};
  utime_t blocked_since{};

  //CephContext* cct;

  //ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  //utime_t get_sched_time() const { return schedule.scheduled_at; }

  //   static std::string_view qu_state_text(qu_state_t st);
  //
  //   /**
  //    * relatively low-cost(*) access to the scrub job's state, to be used in
  //    * logging.
  //    *  (*) not a low-cost access on x64 architecture
  //    */
  //   std::string_view state_desc() const
  //   {
  //     return qu_state_text(state.load(std::memory_order_relaxed));
  //   }

  // void update_schedule(const scrub_schedule_t& adjusted);

  SchedTarget get_moved_target(scrub_level_t s_or_d);

  void dump(ceph::Formatter* f) const;

  utime_t get_sched_time(utime_t scrub_clock_now) const;

  /**
   * an aux used by both init_and_register() & on_periods_change().
   * Dequeues the scrub targets from the queue, and updates their scheduling
   * if periodic. Then - requeues them.
   *
   * Returns the number of dequeued targets.
   */
  int recalc_periodic_targets(
      const Scrub::sched_conf_t& aconf,
      bool modify_ready_tarets,
      utime_t scrub_time_now);

  /// giving a proper name to an internal flag affecting
  /// merge_delay_requeue() operation
  enum class delay_both_targets_t { no, yes };

  void merge_delay_requeue(SchedTarget&& aborted_target,
    delay_cause_t issue,
    std::chrono::seconds delay,
    delay_both_targets_t delay_both_targets,
    utime_t scrub_time_now);

  /// an aux used by both XXXX & merge_delay_requeue()
  void merge_and_delay(
    SchedTarget&& aborted_target,
    delay_cause_t issue,
    std::chrono::seconds delay,
    delay_both_targets_t delay_both,
    utime_t scrub_time_now);

  /*
   * as the atomic 'in_queues' appears in many log prints, accessing it for
   * display-only should be made less expensive (on ARM. On x86 the _relaxed
   * produces the same code as '_cs')
   */
  //   std::string_view registration_state() const
  //   {
  //     return in_queues.load(std::memory_order_relaxed) ? "in-queue"
  // 						     : "not-queued";
  //   }

  /**
   * access the 'state' directly, for when a distinction between 'registered'
   * and 'unregistering' is needed (both have in_queues() == true)
   */
  // bool is_state_registered() const { return state == qu_state_t::registered; }

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state() const;

  //  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;
  //  const std::string log_msg_prefix;

  // RRR document
  //   void aux_queue_targets(Scrub::SchedTarget* shallow,
  //                                  Scrub::SchedTarget* deep);
};


#if 0

class ScrubJob final : public RefCountedObject {
 public:
  /**
   * a time scheduled for scrub, and a deadline: The scrub could be delayed
   * if system load is too high (but not if after the deadline),or if trying
   * to scrub out of scrub hours.
   */
  scrub_schedule_t schedule;

  /// pg to be scrubbed
  const spg_t pgid;

  /// the OSD id (for the log)
  const int whoami;

  ceph::atomic<qu_state_t> state{qu_state_t::not_registered};

  /**
   * the old 'is_registered'. Set whenever the job is registered with the OSD,
   * i.e. is in either the 'to_scrub' or the 'penalized' vectors.
   */
  std::atomic_bool in_queues{false};

  /// last scrub attempt failed to secure replica resources
  bool resources_failure{false};

  /**
   * 'updated' is a temporary flag, used to create a barrier after
   * 'sched_time' and 'deadline' (or any other job entry) were modified by
   * different task.
   * 'updated' also signals the need to move a job back from the penalized
   * queue to the regular one.
   */
  std::atomic_bool updated{false};

  /**
    * the scrubber is waiting for locked objects to be unlocked.
    * Set after a grace period has passed.
    */
  bool blocked{false};
  utime_t blocked_since{};

  utime_t penalty_timeout{0, 0};

  CephContext* cct;

  ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  utime_t get_sched_time() const { return schedule.scheduled_at; }

  static std::string_view qu_state_text(qu_state_t st);

  /**
   * relatively low-cost(*) access to the scrub job's state, to be used in
   * logging.
   *  (*) not a low-cost access on x64 architecture
   */
  std::string_view state_desc() const
  {
    return qu_state_text(state.load(std::memory_order_relaxed));
  }

  void update_schedule(const scrub_schedule_t& adjusted);

  void dump(ceph::Formatter* f) const;

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
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state(utime_t now_is, bool is_deep_expected) const;

  std::ostream& gen_prefix(std::ostream& out, std::string_view fn) const;
  const std::string log_msg_prefix;
};

using ScrubJobRef = ceph::ref_t<ScrubJob>;

#endif
using ScrubQContainer = std::vector<ScrubTargetId>;


}  // namespace Scrub

namespace std {
std::ostream& operator<<(std::ostream& out, const Scrub::ScrubJob& pg);
}  // namespace std

namespace fmt {

// clang-format off
template <>
struct formatter<Scrub::delay_cause_t> : ::fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(Scrub::delay_cause_t cause, FormatContext& ctx)
  {
    using enum Scrub::delay_cause_t;
    std::string_view desc;
    switch (cause) {
      case none:        desc = "ok"; break;
      case replicas:    desc = "replicas"; break;
      case flags:       desc = "flags"; break;	 // no-scrub etc'
      case pg_state:    desc = "pg-state"; break;
      case time:        desc = "time"; break;
      case local_resources: desc = "local-cnt"; break;
      case aborted:     desc = "aborted"; break;
      case backend_error: desc = "backend-error"; break;
      case interval:    desc = "interval"; break;
      // better to not have a default case, so that the compiler will warn
    }
    return ::fmt::formatter<string_view>::format(desc, ctx);
  }
};
// clang-format on

// template <>
// struct formatter<Scrub::qu_state_t> : formatter<std::string_view> {
//   template <typename FormatContext>
//   auto format(const Scrub::qu_state_t& s, FormatContext& ctx)
//   {
//     auto out = ctx.out();
//     out = fmt::formatter<string_view>::format(
// 	std::string{Scrub::ScrubJob::qu_state_text(s)}, ctx);
//     return out;
//   }
// };

// template <>
// struct formatter<Scrub::ScrubJob> {
//   template <typename ParseContext>
//   constexpr auto parse(ParseContext& ctx)
//   {
//     auto it = ctx.begin();
//     if (it != ctx.end() && *it == 's') {
//       shortened = true;
//       ++it;
//     }
//     return it;
//   }
//
//   template <typename FormatContext>
//   auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx)
//   {
//     if (shortened) {
//       return ::fmt::format_to(
// 	  ctx.out(), "pg[{}]:reg:{}", sjob.m_pgid, sjob.registration_state());
//     }
//     return ::fmt::format_to(
// 	ctx.out(), "pg[{}]:[t/s:{},t/d:{}],reg:{}", sjob.m_pgid,
// 	sjob.m_shallow_target, sjob.m_deep_target, sjob.registration_state());
//   }
//   bool shortened{false};  ///< true = no detailed targets info
// };
}  // namespace fmt
