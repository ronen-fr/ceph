// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
// clang-format off
/*
┌
 DIAGRAM REMOVED FROM DISPLAY DUE TO RESTORATION EFFORTS

 */
// clang-format on

#include <atomic>
#include <chrono>
#include <compare>
#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

#include "common/ceph_atomic.h"
#include "include/expected.hpp"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"

#include "utime.h"

class PG;
class PgScrubber;
class OSDService;
template <>
struct fmt::formatter<Scrub::SchedTarget>;

namespace Scrub {

using namespace ::std::literals;


/**
 * Possible urgency levels for a specific scheduling target (shallow or deep):
 *
 * 'off' - the target is not scheduled for scrubbing. This is the initial state,
 * 	and is also the state for a target that is not eligible for scrubbing
 * 	(e.g. before removing from the OSD).
 *
 * periodic scrubs:
 * ---------------
 *
 * 'penalized' - our last attempt to scrub the specific PG has failed due to
 *      reservation issues. We will try again, but with a delay:
 *      all other targets will be tried first.
 *      This state is time-limited by the penalty_timeout field in the
 *      owning ScrubJob.
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
 * - are never marked 'penalized' (even if failing to reserve replicas);
 * - do not have random delays added to their target time;
 * - never have their target time modified by a configuration change;
 * - never subject to 'extended sleep time' (see scrub_sleep_time());
 *
 *
 * 'operator_requested' - the target was manually requested for scrubbing by
 *      an administrator.
 *
 * 'must' - the target is required to be scrubbed, as:
 *      - the scrub was initiated by a message specifying 'do_repair'; or
 *      - the PG info is not valid (i.e. we do not have a valid 'last-scrub' stamp)
 *   or - a deep scrub is required after the previous scrub ended with errors.
 *      'must' scrubs are similar to 'operator_requested', but have a higher
 *      priority (and have a repair flag set).
 *
 * 'after_repair' - triggered immediately after a recovery process
 *      ('scrub_after_recovery' was set). The highest urgency assigned
 *      in this case assure we are not racing against any other type of scrub
 *      (and helps clarifying the PG/scrub status in the logs).
 *      This type of scrub is always deep.
 */
enum class urgency_t {
  off,
  penalized,
  periodic_regular,
  overdue,
  operator_requested,
  must,
  after_repair,
};

/**
 * the result of the last attempt to schedule a scrub for a specific PG.
 * The enum value itself is mostly used for logging purposes.
 * For a discussion of the handling of scrub initiation issues and scrub
 * aborts - see for example ScrubJob::delay_on*() & ScrubJob::on_abort()
 */
enum class delay_cause_t {
  none,		    ///< scrub attempt was successful
  replicas,	    ///< failed to reserve replicas
  flags,	    ///< noscrub or nodeep-scrub
  pg_state,	    ///< e.g. snap-trimming
  time,		    ///< time restrictions or busy CPU
  local_resources,  ///< too many scrubbing PGs
  aborted,	    ///< scrub was aborted on no(deep)-scrub
};

struct sched_conf_t {
  /// the desired interval between shallow scrubs
  double shallow_interval{0.0};

  /// the desired interval between deep scrubs
  double deep_interval{0.0};

  /// the maximum interval between shallow scrubs, as determined by either the
  /// OSD or the pool configuration
  std::optional<double> max_shallow;

  double max_deep{0.0};

  /// a randomization factor for the scrub intervals: the relevant interval,
  /// multiplied by this and a random number between 0 and 1, will be the
  /// added to the target time.
  double interval_randomize_ratio{0.0};

  /// a randomization factor aimed at preventing 'thundering herd' problems
  /// upon deep-scrubs common intervals. If polling a random number smaller
  /// than that percentage, the next shallow scrub is upgraded to deep.
  /// Specified here as a percentage (0..100), i.e. 100*conf
  int deep_randomize_pcnt{0};

  /// must we schedule a scrub with high urgency if we do not have a valid
  /// last scrub stamp?
  bool mandatory_on_invalid{true};
};

class ScrubJob;

/*
 * There are two versions of the scheduling-target (the object detailing one
 * of the two scrub types (deep or shallow) for a specific PG):
 * 'SchedEntry' is maintained by the ScrubQueue, and holds just
 *   the scheduling details.
 * 'SchedTarget' is maintained by the PgScrubber, and
 *   holds the same set of scheduling details plus additional information
 *   about the scrub to be performed.
 */

struct SchedEntry {
  static inline constexpr const utime_t eternity{
      time_t{std::numeric_limits<int32_t>::max()}, 0};

  constexpr SchedEntry(spg_t pgid, scrub_level_t level)
      : pgid{pgid}
      , level{level}
  {}

  spg_t pgid;
  scrub_level_t level;

  /// 'white-out' support: if false, the entry was logically removed from
  /// the queue
  bool is_valid{true};

  urgency_t urgency{urgency_t::off};

  /**
   * the time at which we are allowed to start the scrub. Never
   * decreasing after 'target' is set.
   */
  utime_t not_before{eternity};

  /**
   * the 'deadline' is the time by which we expect the periodic scrub to
   * complete. It is determined by the 'max_*scrub' configuration parameters.
   * Once passed, the scrub will be allowed to run even if the OSD is overloaded
   * or during no-scrub hours.
   */
  utime_t deadline{eternity};

  /**
   * the 'target' is the time at which we intended the scrub to be scheduled.
   * For periodic (regular) scrubs, it is set to the time of the last scrub
   * plus the scrub interval (plus some randomization). Priority scrubs
   * have their own specific rules for the target time. Usually it set to
   * 'now' when the scrub is scheduled.
   */
  utime_t target{eternity};

  bool is_ripe(utime_t now_is) const
  {
    return urgency > urgency_t::off && now_is >= not_before;
  }

  void dump(std::string_view sect_name, ceph::Formatter* f) const;
};

std::weak_ordering cmp_ripe_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r);

std::weak_ordering cmp_future_entries(
    const Scrub::SchedEntry& l,
    const Scrub::SchedEntry& r);


class SchedTarget {
 public:
  friend class ::PgScrubber;
  friend ScrubJob;
  friend struct fmt::formatter<Scrub::SchedTarget>;

  SchedTarget(
      spg_t pg_id,
      scrub_level_t scrub_level,
      int osd_num,
      CephContext* cct);


  std::ostream& gen_prefix(std::ostream& out) const;

  utime_t sched_time() const;

 private:
  /// our ID and scheduling parameters
  SchedEntry sched_info;

  /// is this target (meaning - a copy of his specific combination of
  /// PG and scrub type) currently in the queue?
  bool in_queue{false};

  // some more scheduling-related information

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

  // -----       logging support

  CephContext* cct;

  int whoami;  ///< the OSD id

 public:
  /// access that part of the SchedTarget that is queued in the scrub queue
  const SchedEntry& queued_element() const { return sched_info; }

  bool is_deep() const { return sched_info.level == scrub_level_t::deep; }

  bool is_shallow() const { return sched_info.level == scrub_level_t::shallow; }

  scrub_level_t level() const
  {
    return is_deep() ? scrub_level_t::deep : scrub_level_t::shallow;
  }

  bool is_periodic() const { return sched_info.urgency <= urgency_t::overdue; }
  bool is_required() const { return sched_info.urgency > urgency_t::overdue; }
  bool is_viable() const { return sched_info.urgency > urgency_t::off; }
  bool is_queued() const { return in_queue; }

  delay_cause_t delay_cause() const { return last_issue; }
  bool was_delayed() const { return last_issue != delay_cause_t::none; }

  bool is_ripe(utime_t now_is) const
  {
    return sched_info.urgency > urgency_t::off &&
	   now_is >= sched_info.not_before;
  }

  bool over_deadline(utime_t now_is) const
  {
    return sched_info.urgency > urgency_t::off && now_is >= sched_info.deadline;
  }

  urgency_t urgency() const { return sched_info.urgency; }

private:

  /// resets to the after-construction state
  void reset();

  void clear_queued() { in_queue = false; }
  void set_queued() { in_queue = true; }
  void disable() { sched_info.urgency = urgency_t::off; }

  // failures
  void push_nb_out(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now);
  void delay_on_pg_state(utime_t scrub_clock_now);
  void delay_on_level_not_allowed(utime_t scrub_clock_now);
  void delay_on_wrong_time(utime_t scrub_clock_now);
  void delay_on_no_local_resrc(utime_t scrub_clock_now);

  void dump(std::string_view sect_name, ceph::Formatter* f) const;

  void set_oper_deep_target(scrub_type_t rpr, utime_t scrub_clock_now);
  void set_oper_shallow_target(scrub_type_t rpr, utime_t scrub_clock_now);

  void up_urgency_to(urgency_t u)
  {
    sched_info.urgency = std::max(sched_info.urgency, u);
  }

  void depenalize();

  // updating periodic targets:

  void update_as_shallow(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  void update_as_deep(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  std::string m_log_prefix;
};

// Queue-manipulation by a PG:
struct ScrubQueueOps;


// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob -- scrub scheduling & parameters for a specific PG (PgScrubber)

class ScrubJob {
 public:
  ScrubJob(
      ScrubQueueOps& osd_queue,
      CephContext* cct,
      const spg_t& pg,
      int node_id);


  static scrub_level_t the_other_level(scrub_level_t l)
  {
    return (l == scrub_level_t::deep) ? scrub_level_t::shallow
				      : scrub_level_t::deep;
  }

  void remove_from_osd_queue();

  // push a target back to the queue (after having it modified)
  void requeue_entry(scrub_level_t tid);

  /**
   * returns a copy of the named target, and resets the 'left behind'
   * copy (which is either 'shallow_target' or 'deep_target')
   */
  SchedTarget get_moved_target(scrub_level_t s_or_d);

  void dequeue_entry(scrub_level_t s_or_d);

  bool in_queue() const
  {
    return shallow_target.in_queue || deep_target.in_queue;
  }

  void
  on_abort(SchedTarget&& aborted_target, delay_cause_t issue, utime_t now_is);

  bool on_reservation_failure(
      std::chrono::seconds period,
      SchedTarget&& aborted_target);

  void mark_for_after_repair();

  SchedTarget& closest_target(utime_t scrub_clock_now);
  const SchedTarget& closest_target(utime_t scrub_clock_now) const;


 public:
  /// pg to be scrubbed
  spg_t pgid;

  /// the OSD id (for the log)
  int whoami;

  CephContext* cct;

  ScrubQueueOps& scrub_queue;

  SchedTarget shallow_target;
  SchedTarget deep_target;


  SchedTarget& get_target(scrub_level_t lvl);  // up the ref-count


  bool scrubbing{false};

  // failures/issues/aborts-related information

  /**
   * the scrubber is waiting for locked objects to be unlocked.
   * Set after a grace period has passed.
   */
  bool blocked{false};
  utime_t blocked_since{};

  bool penalized{false};

  /*
   * if the PG is 'penalized' (after failing to secure replicas), this is the
   * time at which the penalty is lifted.
   */
  utime_t penalty_timeout{0, 0};

  /**
   * the more consecutive failures - the longer we will delay before
   * retrying the scrub job
   */
  int consec_aborts{0};


  utime_t get_sched_time(utime_t scrub_clock_now) const
  {
    return closest_target(scrub_clock_now).queued_element().not_before;
  }

  /**
   * the operator faked the timestamp. Reschedule the
   * relevant target.
   *
   * Locks the 'targets_lock' mutex.
   */
  void operator_periodic_targets(
      scrub_level_t level,
      utime_t upd_stamp,
      const pg_info_t& pg_info,
      const sched_conf_t& sched_configs,
      utime_t now_is);

  /**
   * the operator instructed us to scrub. The urgency is set to (at least)
   * 'operator_requested', or (if the request is for a repair-scrub) - to
   * 'must'
   *
   * Locks the 'targets_lock' mutex.
   */
  void operator_forced_targets(
      scrub_level_t level,
      scrub_type_t scrub_type,
      utime_t now_is);

  // deep scrub is marked for the next scrub cycle for this PG
  // The equivalent of must_scrub & must_deep_scrub
  void mark_for_rescrubbing();

  void init_and_queue_targets(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  void at_scrub_completion(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  /**
   * Following a change in the 'scrub period' parameters -
   * recomputing the targets.
   */
  void on_periods_change(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  void un_penalize();

  void dump(ceph::Formatter* f) const;

  std::string_view registration_state() const
  {
    return in_queue() ? "in-queue" : "not-queued";
  }

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state() const;

  friend std::ostream& operator<<(std::ostream& out, const ScrubJob& pg);
  std::ostream& gen_prefix(std::ostream& out) const;
  std::string m_log_msg_prefix;

 protected:  // made public in u-tests
  // aux - dequeue targets
  // returns the number of targets that were previously marked as "in queue"
  int dequeue_targets();

  SchedTarget& dequeue_target(scrub_level_t lvl);
};
}  // namespace Scrub


class PgLockWrapper;

namespace Scrub {

class ScrubSchedListener {
 public:
  virtual int get_nodeid() const = 0;  // returns the OSD number ('whoami')

  virtual PgLockWrapper get_locked_pg(spg_t pgid) = 0;

  virtual void send_sched_recalc_to_pg(spg_t pgid) = 0;

  virtual ~ScrubSchedListener() {}
};

}  // namespace Scrub

// clang-format off
template <>
struct fmt::formatter<Scrub::urgency_t>
    : fmt::formatter<std::string_view> {
  template <typename FormatContext>
  auto format(Scrub::urgency_t urg, FormatContext& ctx)
  {
    using enum Scrub::urgency_t;
    std::string_view desc;
    switch (urg) {
      case after_repair:        desc = "after-repair"; break;
      case must:                desc = "must"; break;
      case operator_requested:  desc = "operator-requested"; break;
      case overdue:             desc = "overdue"; break;
      case periodic_regular:    desc = "periodic-regular"; break;
      case penalized:           desc = "reservation-failure"; break;
      case off:                 desc = "off"; break;
      // better to not have a default case, so that the compiler will warn
    }
    return formatter<string_view>::format(desc, ctx);
  }
};
// clang-format on

// clang-format off
template <>
struct fmt::formatter<Scrub::delay_cause_t> : fmt::formatter<std::string_view> {
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
      // better to not have a default case, so that the compiler will warn
    }
    return formatter<string_view>::format(desc, ctx);
  }
};
// clang-format on

template <>
struct fmt::formatter<Scrub::SchedEntry> {
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

template <>
struct fmt::formatter<Scrub::SchedTarget> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedTarget& st, FormatContext& ctx)
  {
    return format_to(
	ctx.out(), "{},q:{},ar:{},issue:{}", st.sched_info,
	st.in_queue ? "+" : "-", st.auto_repairing ? "+" : "-", st.last_issue);
  }
};

template <>
struct fmt::formatter<Scrub::ScrubJob> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 's') {
      shortened = true;	 // no 'nearest target' info
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx)
  {
    if (shortened) {
      return fmt::format_to(
	  ctx.out(), "pg[{}]:reg:{}", sjob.pgid, sjob.registration_state());
    }
    return fmt::format_to(
	ctx.out(), "pg[{}]:[t/s:{},t/d:{}],reg:{}", sjob.pgid,
	sjob.shallow_target, sjob.deep_target, sjob.registration_state());
  }
  bool shortened{false};
};

template <>
struct fmt::formatter<Scrub::sched_conf_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_conf_t& cf, FormatContext& ctx)
  {
    return format_to(
	ctx.out(), "periods: s:{}/{} d:{}/{} iv-ratio:{} on-inv:{}",
	cf.shallow_interval, cf.max_shallow.value_or(-1.0), cf.deep_interval,
	cf.max_deep, cf.interval_randomize_ratio, cf.mandatory_on_invalid);
  }
};
