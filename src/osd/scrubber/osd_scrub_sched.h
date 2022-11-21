// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once
// clang-format off
/*
┌───────────────────────┐
│ OSD                   │
│ OSDService           ─┼───┐
│                       │   │
│                       │   │
└───────────────────────┘   │   Ownes & uses the following
                            │   ScrubQueue interfaces:
                            │
                            │
                            │   - resource management (*1)
                            │
                            │   - environment conditions (*2)
                            │
                            │   - scrub scheduling (*3)
                            │
                            │
                            │
                            │
                            │
                            │
 ScrubQueue                 │
┌───────────────────────────▼────────────┐
│                                        │
│                                        │
│  ScrubQContainer    to_scrub <>────────┼────────┐
│  ScrubQContainer    penalized          │        │
│                                        │        │
│                                        │        │
│  OSD_wide resource counters            │        │
│                                        │        │
│                                        │        │
│  "env scrub conditions" monitoring     │        │
│                                        │        │
│                                        │        │
│                                        │        │
│                                        │        │
└─▲──────────────────────────────────────┘        │
  │                                               │
  │                                               │
  │uses interface <4>                             │
  │                                               │
  │                                               │
  │            ┌──────────────────────────────────┘
  │            │                 shared ownership of jobs
  │            │
  │      ┌─────▼──────┐
  │      │ScrubJob    │
  │      │            ├┐
  │      │            ││
  │      │            │┼┐
  │      │            │┼│
  └──────┤            │┼┤◄──────┐
         │            │┼│       │
         │            │┼│       │
         │            │┼│       │
         └┬───────────┼┼│       │shared ownership
          └─┼┼┼┼┼┼┼┼┼┼┼┼│       │
            └───────────┘       │
                                │
                                │
                                │
                                │
┌───────────────────────────────┼─┐
│                               <>│
│PgScrubber                       │
│                                 │
│                                 │
│                                 │
│                                 │
│                                 │
└─────────────────────────────────┘


ScrubQueue interfaces (main functions):

<1> - OSD/PG resources management:

  - can_inc_scrubs()
  - {inc/dec}_scrubs_{local/remote}()
  - dump_scrub_reservations()
  - {set/clear/is}_reserving_now()

<2> - environment conditions:

  - update_loadavg()

  - scrub_load_below_threshold()
  - scrub_time_permit()

<3> - scheduling scrubs:

  - select_pg_and_scrub()
  - dump_scrubs()

<4> - manipulating a job's state:

  - register_with_osd()
  - remove_from_osd_queue()
  - update_job()

 */
// clang-format on

#include <atomic>
#include <chrono>
#include <compare>
#include <memory>
#include <optional>
#include <vector>

//#include "common/RefCountedObj.h"
#include "common/ceph_atomic.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"
#include "include/utime_fmt.h"
#include "osd/osd_types_fmt.h"
#include "utime.h"

//#include "osd/PG.h"
class PG;
class PgScrubber;
class OSDService;

namespace Scrub {

using namespace ::std::literals;

struct scrub_schedule_t {
  utime_t scheduled_at{};
  utime_t deadline{0, 0};
};

enum class urgency_t {
  off,
  penalized,  //< replica reservation failure
  periodic_regular,
  overdue,
  operator_requested,
  must,
  after_repair,
};

enum class delay_cause_t {
  none,
  replicas,
  flags,
  pg_state,
  time,
  local_resources,
  aborted,  //< scrub was aborted on no(deep)-scrub
  environment,
};

struct sched_conf_t {
  double shallow_interval{0.0};
  double deep_interval{0.0};
  std::optional<double> max_shallow;
  double max_deep{0.0};
  double interval_randomize_ratio{0.0};
  // double deep_randomize_ratio{0.0};
  bool mandatory_on_invalid{true};
};

struct ScrubJob;
using ScrubJobRef = ceph::ref_t<ScrubJob>;


struct SchedTarget {
  static constexpr auto eternity =
      utime_t{std::numeric_limits<uint32_t>::max(), 0};

  friend class ::PgScrubber;

  SchedTarget(
      ScrubJob& parent_job,
      scrub_level_t base_type,
      std::string dbg_val);

  // note that we do not try to copy the job reference:
  // well - we couldn't do it anyway. But it's not needed, as
  // we will only copy targets of the same ScrubJob.
  SchedTarget& operator=(const SchedTarget& r);

  urgency_t urgency{urgency_t::off};

  /// the time at which we are allowed to start the scrub. Never
  /// decreasing after 'target' is set.
  utime_t not_before{eternity};

  /// affecting the priority and the allowed times for the scrub
  std::optional<utime_t> deadline;

  /// the time at which we intended the scrub to be scheduled
  utime_t target{eternity};

  // RRR consider atomic or lock
  bool scrubbing{false};  // must match 'is_queued_or_active()'

  /**
   * 'randomly selected' for shallow->deep for our next scrub.
   * "Freezing" the value of 'upgradable' when consulted.
   * Always set for 'deep' objects.
   */
  bool deep_or_upgraded{false};

  bool is_deep() const { return deep_or_upgraded; }
  scrub_level_t level() const
  {
    return is_deep() ? scrub_level_t::deep : scrub_level_t::shallow;
  }
  bool is_periodic() const { return urgency <= urgency_t::overdue; }
  bool is_viable() const { return urgency > urgency_t::off; }

  /**
   * the result of the a 'coin flip' for the next time we consider
   * upgrading a shallow scrub to a deep scrub.
   */
  bool upgradeable{false};

  // an ephemeral flag used when sorting the targets. We use different
  // sorting criteria for ripe vs future targets. See discussion in <=>
  // operator.
  bool eph_ripe_for_sort{false};

  /// the reason for the latest failure/delay
  delay_cause_t last_issue{delay_cause_t::none};

  /// a back-link to the job. Used to update the job's state.
  // ScrubJobRef job;
  // ScrubJob& parent_job;

  // copied from the parent job, to avoid having to rely on a backlink
  spg_t pgid;
  CephContext* cct;

  /**
   * the original scheduling object type. Note that for the shallow
   * scheduling target objects - overridden by 'deep_or_upgraded'
   */
  scrub_level_t base_target_level;  // 'const' in semantics

  /**
   * (deep-scrub entries only:)
   * Supporting the equivalent of 'need-auto', which translated into:
   * - performing a deep scrub (taken care of by raising the priority of the
   *   deep target);
   * - marking that scrub as 'do_repair' (the next flag here);
   * - no random delays (RRR - does the 'urgency' field cover this?)
   */
  bool auto_repairing{false};

  /**
   * (deep-scrub entries only:)
   * Set for scrub_requested() scrubs with the 'repair' flag set.
   * Translated (in set_op_parameters()) into a 'deep scrub' with
   * m_is_repair & PG_REPAIR_SCRUB.
   */
  bool do_repair{false};

  /// marked for de-queue, as the PG is no longer eligible for scrubbing
  bool marked_for_dequeue{false};

  std::string dbg_val;

  /**
   * For sched-targets, lower is better.
   * The 'urgency' field (reversed) is the primary key.
   *
   * Note: 'partial order' due to strange utime_t::operator<=>()
   */
  std::partial_ordering operator<=>(const SchedTarget&) const;

  bool is_ripe(utime_t now_is) const
  {
    return urgency > urgency_t::off && !scrubbing && now_is >= not_before;
  }

  void update_ripe_for_sort(utime_t now_is)
  {
    eph_ripe_for_sort = is_ripe(now_is);
  }

  bool over_deadline(utime_t now_is) const
  {
    return urgency > urgency_t::off && now_is >= deadline;
  }

  // status
  void set_scrubbing()
  {
    scrubbing = true;
    push_nb_out(5s);
  }
  void clear_scrubbing() { scrubbing = false; }

  // failures
  void push_nb_out(std::chrono::seconds delay);
  void push_nb_out(std::chrono::seconds delay, delay_cause_t delay_cause);
  void pg_state_failure();
  void wrong_time();
  void on_local_resources();

  void dump(ceph::Formatter* f) const;

  // consult the current value of the 'random upgrade" flag, and
  // redraw the 'deep_or_upgraded' flag for the next run.
  bool check_and_redraw_upgrade();

  void set_oper_deep_target(scrub_type_t rpr);
  void set_oper_shallow_target(scrub_type_t rpr);

  // RRR why differ from the regular handling of periodic scrubs schedule?
  // seems that for some very minor issues, only needed for testing.
  void set_oper_period_sh(
      utime_t stamp,
      const pg_info_t& info,
      const Scrub::sched_conf_t& aconf,
      utime_t now_is);
  void set_oper_period_dp(
      utime_t stamp,
      const pg_info_t& info,
      const Scrub::sched_conf_t& aconf,
      utime_t now_is);

//   void update_target(
//       const pg_info_t& info,
//       const sched_conf_t& aconf,
//       const requested_scrub_t& request_flags);

  // updating periodic targets:

  void update_as_shallow(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);
  void update_as_deep(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);
};

// note: not a shared_ptr, as the statically-allocated target is owned by the
// job
using TargetRef = SchedTarget&;
using TargetRefW = std::reference_wrapper<SchedTarget>;


struct PossibleScrubMode {
  TargetRef selected_target;
  std::optional<requested_scrub_t> updated_flags;
  Scrub::schedule_result_t outcome{schedule_result_t::scrub_initiated};
};

enum class must_scrub_t { not_mandatory, mandatory };

enum class qu_state_t {
  not_registered,  // not a primary, thus not considered for scrubbing by this
		   // OSD (also the temporary state when just created)
  registered,	   // in either of the two queues ('to_scrub' or 'penalized')
  unregistering	   // in the process of being unregistered. Will be finalized
		   // under lock
};

struct sched_params_t {
  utime_t proposed_time{};
  double min_interval{0.0};
  double max_interval{0.0};
  must_scrub_t is_must{Scrub::must_scrub_t::not_mandatory};
};

struct ScrubJob final : public RefCountedObject {
  /// pg to be scrubbed
  spg_t pgid;

  /// the OSD id (for the log)
  int whoami;

  CephContext* cct;

  ceph::atomic<qu_state_t> state{qu_state_t::not_registered};

  /**
   *  a time scheduled for scrub, and a deadline: The scrub could be delayed
   * if system load is too high (but not if after the deadline),or if trying
   * to scrub out of scrub hours.
   */


  SchedTarget shallow_target;
  SchedTarget deep_target;
  // and a 'current' target, pointing to one of the above:
  // (mostly used for general schedule queries)
  TargetRefW closest_target;  // always updated to the closest target

  SchedTarget next_shallow;  // only used when currently s-scrubbing
  SchedTarget next_deep;     // only used when currently d-scrubbing

  // update 'closest_target':
  void determine_closest();

  /// the implementation of the 'req_scrub' notion: the operator is
  /// requesting a high-priority deep-scrub, but we are already
  /// deep-scrubbing - possibly with a different 'repair' flag or
  /// without the guarantees provided by 'must'. We remember the
  /// specific request for once this scrub is done.
  // std::optional<TargetRef> req_next_scrub;

  void disable_scheduling();  // == reset all targets to 'off'
  void mark_for_dequeue();
  void clear_marked_for_dequeue();
  bool verify_targets_disabled() const;

  // note: guaranteed to return the entry that's possibly in the to_scrub queue
  TargetRef get_current_trgt(scrub_level_t lvl);
  TargetRef get_modif_trgt(scrub_level_t lvl);
  TargetRef get_next_trgt(scrub_level_t lvl);

  // void activate_next_targets();

  /**
   * the old 'is_registered'. Set whenever the job is registered with the OSD,
   * i.e. is in either the 'to_scrub' or the 'penalized' vectors.
   */
  std::atomic_bool in_queues{false};

  /// last scrub attempt failed to secure replica resources. A temporary
  /// flag, signalling the need to modify both targets under lock.
  bool resources_failure{false};  // atomic?

  bool penalized{false};

  /// deep scrub after repair - a type of 'scrub immediately'

  /**
   *  'updated' is a temporary flag, used to create a barrier after
   *  'sched_time' and 'deadline' (or any other job entry) were modified by
   *  different task.
   */
  std::atomic_bool updated{false};  // RRR needed anymore?

  /**
   * the scrubber is waiting for locked objects to be unlocked.
   * Set after a grace period has passed.
   */
  bool blocked{false};
  utime_t blocked_since{};

  utime_t penalty_timeout{0, 0};


  ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  utime_t get_sched_time() const { return closest_target.get().not_before; }

  bool is_ripe(utime_t now_is) const
  {
    return closest_target.get().is_ripe(now_is);
  }

  void initial_shallow_target(
      const pg_info_t& pg_info,
      const sched_conf_t& sched_configs,
      utime_t now_is);

  void initial_deep_target(
      const pg_info_t& pg_info,
      const sched_conf_t& sched_configs,
      utime_t now_is);

  // the operator faked the timestamp. Reschedule the
  // relevant target.
  void operator_periodic_targets(
      scrub_level_t level,
      utime_t upd_stamp,
      const pg_info_t& pg_info,
      const sched_conf_t& sched_configs,
      utime_t now_is);

  // 'need_auto' is set;
  // deep scrub is marked for the next scrub cycle for this PG
  // The equivalent of must_scrub & must_deep_scrub
  void mark_for_rescrubbing(requested_scrub_t& request_flags);

  void set_initial_targets(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  void at_scrub_completion(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  // retval: true if a change was made
  bool on_periods_change(
      const pg_info_t& info,
      const sched_conf_t& aconf,
      utime_t now_is);

  /* needed? */ void merge_deep_target(TargetRef&& candidate);

  void merge_targets(scrub_level_t lvl);

  void un_penalize(utime_t now_is);

  /**
   * relatively low-cost(*) access to the scrub job's state, to be used in
   * logging.
   *  (*) not a low-cost access on x64 architecture
   */
  std::string_view state_desc() const;

  void update_schedule(const Scrub::scrub_schedule_t& adjusted);

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
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state(utime_t now_is, bool is_deep_expected) const;

  friend std::ostream& operator<<(std::ostream& out, const ScrubJob& pg);
};

// what the OSD is using to schedule scrubs:
struct SchedEntry {
  ScrubJobRef job;
  scrub_level_t s_or_d;

  SchedEntry(ScrubJobRef j, scrub_level_t s) : job(j), s_or_d(s) {}
  TargetRef target()
  {
    {  // RRR bug:
      if (s_or_d == scrub_level_t::deep) {
	auto& x1 = job->get_current_trgt(s_or_d);
	auto& x2 = (s_or_d == scrub_level_t::deep) ? job->deep_target
						   : job->shallow_target;
	ceph_assert(
	    reinterpret_cast<uintptr_t>(&x1) ==
	    reinterpret_cast<uintptr_t>(&x2));
      }
    }

    return job->get_current_trgt(s_or_d);
  }

  TargetRef target() const
  {
    {  // RRR bug:
      if (s_or_d == scrub_level_t::deep) {
	auto& x1 = job->get_current_trgt(s_or_d);
	auto& x2 = (s_or_d == scrub_level_t::deep) ? job->deep_target
						   : job->shallow_target;
	ceph_assert(
	    reinterpret_cast<uintptr_t>(&x1) ==
	    reinterpret_cast<uintptr_t>(&x2));
      }
    }

    return job->get_current_trgt(s_or_d);
  }

  bool is_scrubbing() const
  {
    return job->get_current_trgt(scrub_level_t::shallow).scrubbing ||
	   job->get_current_trgt(scrub_level_t::deep).scrubbing;
  }

  // smaller is better (i.e. the '<' is more urgent);
  std::partial_ordering operator<=>(const SchedEntry& r) const
  {
    if (auto cmp = r.s_or_d <=> s_or_d; cmp != 0) {
      return cmp;
    }
    return job->get_current_trgt(s_or_d) <=> r.job->get_current_trgt(s_or_d);
  }
};


class ScrubSchedListener;

// the OSD services provided to the scrub scheduler
// class ScrubSchedListener {
//  public:
//   virtual int get_nodeid() const = 0;  // returns the OSD number ('whoami')
// 
//   /**
//    * A callback used by the ScrubQueue object to initiate a scrub on a specific
//    * PG.
//    *
//    * The request might fail for multiple reasons, as ScrubQueue cannot by its
//    * own check some of the PG-specific preconditions and those are checked here.
//    * See attempt_t definition.
//    *
//    * @return a Scrub::attempt_t detailing either a success, or the failure
//    * reason.
//    */
//   virtual schedule_result_t initiate_a_scrub(
//     spg_t pgid,
//     bool allow_requested_repair_only) = 0;
// 
//   virtual schedule_result_t initiate_a_scrub(
//     spg_t pgid,
//     bool allow_requested_repair_only) = 0;
// 
//   virtual ~ScrubSchedListener() {}
// };

}  // namespace Scrub

/**
 * the queue of PGs waiting to be scrubbed.
 * Main operations are scheduling/unscheduling a PG to be scrubbed at a certain
 * time.
 *
 * A "penalty" queue maintains those PGs that have failed to reserve the
 * resources of their replicas. The PGs in this list will be reinstated into the
 * scrub queue when all eligible PGs were already handled, or after a timeout
 * (or if their deadline has passed [[disabled at this time]]).
 */
class ScrubQueue {
 public:
  ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds);
  virtual ~ScrubQueue() = default;


  friend class TestOSDScrub;
  friend class ScrubSchedTestWrapper; ///< unit-tests structure

  using ScrubQContainer = std::vector<Scrub::ScrubJobRef>;
  using SchedulingQueue = std::vector<Scrub::SchedEntry>;

  static std::string_view qu_state_text(Scrub::qu_state_t st);


  void sched_scrub(
    const ceph::common::ConfigProxy& config,
    bool is_recovery_active);


  /**
   * called periodically by the OSD to select the first scrub-eligible PG
   * and scrub it.
   *
   * Selection is affected by:
   * - time of day: scheduled scrubbing might be configured to only happen
   *   during certain hours;
   * - same for days of the week, and for the system load;
   *
   * @param preconds: what types of scrub are allowed, given system status &
   *                  config. Some of the preconditions are calculated here.
   * @return Scrub::attempt_t::scrubbing if a scrub session was successfully
   *         initiated. Otherwise - the failure cause.
   *
   * locking: locks jobs_lock
   */
  Scrub::schedule_result_t select_pg_and_scrub(Scrub::ScrubPreconds& preconds);

  /**
   * Translate attempt_ values into readable text
   */
  static std::string_view attempt_res_text(Scrub::schedule_result_t v);

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(Scrub::ScrubJobRef sjob);

  /**
   * @return the list (not std::list!) of all scrub jobs registered
   *   (apart from PGs in the process of being removed)
   */
  SchedulingQueue list_registered_jobs() const;

  /**
   * Add the scrub job to the list of jobs (i.e. list of PGs) to be periodically
   * scrubbed by the OSD.
   * The registration is active as long as the PG exists and the OSD is its
   * primary.
   *
   * See update_job() for the handling of the 'suggested' parameter.
   *
   * locking: might lock jobs_lock
   */
  //void register_with_osd(Scrub::ScrubJobRef sjob, const Scrub::sched_params_t& suggested);
  void register_with_osd(Scrub::ScrubJobRef sjob);

  /**
   * modify a scrub-job's scheduled time and deadline
   *
   * There are 3 argument combinations to consider:
   * - 'must' is asserted, and the suggested time is 'scrub_must_stamp':
   *   the registration will be with "beginning of time" target, making the
   *   scrub-job eligible to immediate scrub (given that external conditions
   *   do not prevent scrubbing)
   *
   * - 'must' is asserted, and the suggested time is 'now':
   *   This happens if our stats are unknown. The results are similar to the
   *   previous scenario.
   *
   * - not a 'must': we take the suggested time as a basis, and add to it some
   *   configuration / random delays.
   *
   *  ('must' is sched_params_t.is_must)
   *
   *  locking: not using the jobs_lock
   */
  void update_job(Scrub::ScrubJobRef sjob, const Scrub::sched_params_t& suggested);

  // can only be moved here if we give the ScrubQueue the ability to lock PGs
  void on_config_times_change();

  Scrub::sched_params_t determine_scrub_time(const requested_scrub_t& request_flags,
				      const pg_info_t& pg_info,
				      const pool_opts_t pool_conf) const;

 public:
  void dump_scrubs(ceph::Formatter* f);

  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() { a_pg_is_reserving = true; }
  void clear_reserving_now() { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }

  bool can_inc_scrubs() const;
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();
  void dump_scrub_reservations(ceph::Formatter* f) const;

  /// counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);
  int get_blocked_pgs_count() const;

  /**
   * Pacing the scrub operation by inserting delays (mostly between chunks)
   *
   * Special handling for regular scrubs that continued into "no scrub" times.
   * Scrubbing will continue, but the delays will be controlled by a separate
   * (read - with higher value) configuration element
   * (osd_scrub_extended_sleep).
   */
  double scrub_sleep_time(bool is_mandatory) const;  /// \todo (future) return
						   /// milliseconds

  /**
   *  called every heartbeat to update the "daily" load average
   *
   *  @returns a load value for the logger
   */
  [[nodiscard]] std::optional<double> update_load_average();


//   void set_initial_targets(
//     Scrub::ScrubJobRef sjob,
//     const pg_info_t& pg_info,
//     const Scrub::sched_conf_t& sched_configs);

  Scrub::sched_conf_t populate_config_params(const pool_opts_t& pool_conf);

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& osd_service;

#ifdef WITH_SEASTAR
  auto& conf() const { return local_conf(); }
#else
  auto& conf() const { return cct->_conf; }
#endif

  /**
   *  jobs_lock protects the job containers and the relevant scrub-jobs state
   *  variables. Specifically, the following are guaranteed:
   *  - 'in_queues' is asserted only if the job is in one of the queues;
   *  - a job will only be in state 'registered' if in one of the queues;
   *  - no job will be in the two queues simultaneously
   *
   *  Note that PG locks should not be acquired while holding jobs_lock.
   */
  mutable ceph::mutex jobs_lock = ceph::make_mutex("ScrubQueue::jobs_lock");

  //ScrubQContainer all_pgs;  ///< *not* used for scheduling work

  bool restore_penalized{false};

  SchedulingQueue to_scrub;

  double daily_loadavg{0.0};

  static inline constexpr auto registered_job = [](const auto& jobref) -> bool {
    return jobref->state == Scrub::qu_state_t::registered;
  };

  static inline constexpr auto invalid_state = [](const auto& jobref) -> bool {
    return jobref->state == Scrub::qu_state_t::not_registered;
  };

  /**
   * Are there scrub jobs that should be reinstated?
   */
  void scan_penalized(bool forgive_all, utime_t time_now);

  /**
   * clear dead entries (unregistered, or belonging to removed PGs) from a
   * queue. Job state is changed to match new status.
   */
  //void rm_unregistered_jobs(ScrubQContainer& group);
  void rm_unregistered_jobs();

  /**
   * the set of the first N scrub jobs in 'group' which are ready to be
   * scrubbed (ready = their scheduled time has passed).
   * The scrub jobs in the new collection are sorted according to
   * their urgency, not-before etc'.
   *
   * Note that the returned container holds independent refs to the
   * scrub jobs.
   */
  SchedulingQueue collect_ripe_jobs(SchedulingQueue& group, utime_t time_now);

  /// scrub resources management lock (guarding scrubs_local & scrubs_remote)
  mutable ceph::mutex resource_lock =
    ceph::make_mutex("ScrubQueue::resource_lock");

  // the counters used to manage scrub activity parallelism:
  int scrubs_local{0};
  int scrubs_remote{0};

  /**
   * The scrubbing of PGs might be delayed if the scrubbed chunk of objects is
   * locked by some other operation. A bug might cause this to be an infinite
   * delay. If that happens, the OSDs "scrub resources" (i.e. the
   * counters that limit the number of concurrent scrub operations) might
   * be exhausted.
   * We do issue a cluster-log warning in such occasions, but that message is
   * easy to miss. The 'some pg is blocked' global flag is used to note the
   * existence of such a situation in the scrub-queue log messages.
   */
  std::atomic_int_fast16_t blocked_scrubs_cnt{0};

  std::atomic_bool a_pg_is_reserving{false};

  [[nodiscard]] bool scrub_load_below_threshold() const;
  [[nodiscard]] bool scrub_time_permit(utime_t now) const;

  /**
   * If the scrub job was not explicitly requested, we postpone it by some
   * random length of time.
   * And if delaying the scrub - we calculate, based on pool parameters, a
   * deadline we should scrub before.
   *
   * @return a pair of values: the determined scrub time, and the deadline
   */
  Scrub::scrub_schedule_t adjust_target_time(
    const Scrub::sched_params_t& recomputed_params) const;

  /**
   * Look for scrub jobs that have their 'resources_failure' set. These jobs
   * have failed to acquire remote resources last time we've initiated a scrub
   * session on them. They are now moved from the 'to_scrub' queue to the
   * 'penalized' set.
   *
   * locking: called with job_lock held
   */
  void move_failed_pgs(utime_t now_is);

  Scrub::schedule_result_t select_n_scrub(
    SchedulingQueue& group,
    const Scrub::ScrubPreconds& preconds,
    utime_t now_is);

public: // used by the unit-tests
  /**
   * unit-tests will override this function to return a mock time
   */
  virtual utime_t time_now() const { return ceph_clock_now(); }
};

class PgLockWrapper;

namespace Scrub {

class ScrubSchedListener {
 public:
  virtual int get_nodeid() const = 0;  // returns the OSD number ('whoami')

  virtual PgLockWrapper get_locked_pg(spg_t pgid) = 0;

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
      case flags:       desc = "flags"; break;	 // RRR
      case pg_state:    desc = "pg-state"; break;
      case time:        desc = "time"; break;
      case local_resources: desc = "local-cnt"; break;
      case aborted:     desc = "noscrub"; break;
      case environment: desc = "environment"; break;
      // better to not have a default case, so that the compiler will warn
    }
    return formatter<string_view>::format(desc, ctx);
  }
};
// clang-format on

template <>
struct fmt::formatter<Scrub::SchedTarget> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::SchedTarget& st, FormatContext& ctx)
  {
    const std::string_view effective_lvl =
      (st.base_target_level == scrub_level_t::shallow)
	? (st.deep_or_upgraded ? "up" : "sh")
	: "dp";
    return format_to(
      ctx.out(), "{}/{}: {}nb:{:s},({},tr:{:s},dl:{:s},a-r:{}{}),issue:{},{}",
      (st.base_target_level == scrub_level_t::deep ? "dp" : "sh"),
      effective_lvl,
      st.scrubbing ? "ACTIVE " : "",
      st.not_before,
      st.urgency, st.target, st.deadline.value_or(utime_t{}),
      st.auto_repairing ? "+" : "-",
      st.marked_for_dequeue ? "XXX" : "",
      st.last_issue,
      st.dbg_val);
  }
};

template <>
struct fmt::formatter<Scrub::ScrubJob> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 's') {
      shorted = true;  // no 'nearest target' info
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx)
  {
    if (shorted) {
      return fmt::format_to(
	ctx.out(), "pg[{}]:reg:{},rep-fail:{},queue-state:{}", sjob.pgid,
	sjob.registration_state(), sjob.resources_failure,
	ScrubQueue::qu_state_text(sjob.state));
    }
    return fmt::format_to(
      ctx.out(), "pg[{}]:[t:{}],reg:{},rep-fail:{},queue-state:{}", sjob.pgid,
      sjob.closest_target.get(), sjob.registration_state(), sjob.resources_failure,
      ScrubQueue::qu_state_text(sjob.state));
  }
  bool shorted{false};
};

template <>
struct fmt::formatter<Scrub::sched_conf_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const Scrub::sched_conf_t& cf, FormatContext& ctx)
  {
    return format_to(
      ctx.out(), "periods: s:{}/{} d:{}/{} iv-ratio:{} on-inv:{}",
      cf.shallow_interval, cf.max_shallow.value_or(-1.0),
      cf.deep_interval, cf.max_deep, cf.interval_randomize_ratio,
      cf.mandatory_on_invalid);
  }
};

