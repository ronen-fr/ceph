// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <set>
#include <vector>
#include <atomic>

#include "common/RefCountedObj.h"
#include "osd/osd_types.h"
#include "osd/scrubber_common.h"

#include "utime.h"

class PG;

/**
 * the ordered queue of PGs waiting to be scrubbed.
 * Main operations are scheduling/unscheduling a PG to be scrubbed at a certain time.
 *
 * A "penalty" queue maintains those PGs that have failed to reserve the resources
 * of their replicas. The PGs in this list will be reinstated into the scrub queue when
 * all eligible PGs were already handled, or after a timeout (or if their deadline
 * has passed).
 */
class ScrubQueue {
 public:
  enum class must_scrub_t { not_mandatory, mandatory };

  ScrubQueue(CephContext* cct, OSDService& osds);

  using TimeAndDeadline = std::pair<utime_t, utime_t>;

  struct ScrubJob final : public RefCountedObject {

    utime_t get_sched_time() const { return sched_time; }  // might need to be elaborated

    /// a time scheduled for scrub. but the scrub could be delayed if system
    /// load is too high or it fails to fall in the scrub hours
    utime_t sched_time;

    /// the hard upper bound of scrub time
    utime_t deadline;

    /// pg to be scrubbed
    spg_t pgid;

    /// the OSD id (for the log)
    int whoami;

    /// the old 'is_registered'. Set whenever the job is registered with the OSD, i.e.
    /// is in either the 'to_scrub' or the 'penalized' vectors.
    bool m_in_queue{false};

    bool m_pg_being_removed{false};  ///< set to allow safe removal of the ref'ed PG

    // last scrub attempt failed in securing replica resources
    bool m_resources_failure{false};
    //std::atomic_bool m_resources_failure{false};

    /// m_updated is a temporary flag, used to create a barrier after sched_time
    /// and 'deadline', and possibly other job entries, were modified by a possibly
    /// different task.
    /// m_updated also signals the need to move a job back from the penalized to the
    /// regular queue.
    std::atomic_bool m_updated{false};

    utime_t penalty_timeout{0, 0};

    CephContext* cct;

    ScrubJob(CephContext* cct,
	     const spg_t& pg,
	     int node_id);  // used when a PgScrubber is created

    void set_time_n_deadline(TimeAndDeadline ts)
    {
      sched_time = ts.first;
      deadline = ts.second;
    }

    void dump(ceph::Formatter* f) const;

    FRIEND_MAKE_REF(ScrubJob);
  };

  friend class TestOSDScrub;

  using ScrubJobRef = ceph::ref_t<ScrubJob>;
  using ScrubQContainer = std::vector<ceph::ref_t<ScrubJob>>;

  /**
   * called periodically by the OSD to select the first scrub-eligible PG
   * and scrub it.
   *
   * \todo complete notes re order of selection
   *
   * @param preconds
   * @return
   *
   * locking: locks jobs_lock
   */
  Scrub::attempt_t select_pg_and_scrub(Scrub::ScrubPreconds& preconds);

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(ceph::ref_t<ScrubJob> sjob);

  /**
   * @return the list (not std::set!) of all scrub-jobs registered
   *   (apart from PGs in the process of being removed)
   */
  ScrubQContainer list_all_valid() const;

  struct sched_params_t {
    utime_t suggested_stamp{};
    double min_interval{0.0};
    double max_interval{0.0};
    must_scrub_t is_must{ScrubQueue::must_scrub_t::not_mandatory};
  };

  /**
   * add to the OSD's to_scrub queue
   *
   * Pre-conds:
   * - m_in_queue is checked to be false. The insertion is rejected otherwise
   *   (note comment in re why the locking scheme prevents this interface from
   *   being used as insert-or-update);
   * - m_pg_being_removed is false;
   * -
   *
   * locking: locks job_lock
   */
  void register_with_osd(ceph::ref_t<ScrubJob> sjob, const sched_params_t& suggested);

  /**
   * modify the scrub-job of a specific PG
   *
   * The (possibly modified) scrub-job is added to the list of PGs to be periodically
   * scrubbed by the OSD.
   * The registration is active as long as the PG exists and the OSD is its primary.
   *
   * There are 3 argument combinations to consider:
   * - 'must' is asserted, and the suggested time is 'scrub_must_stamp':
   *   the registration will be with "beginning of time" target, making the
   *   PG eligible to immediate scrub (given that external conditions do not
   *   prevent scrubbing)
   *
   * - 'must' is asserted, and the suggested time is 'now':
   *   This happens if our stats are unknown. The results is similar to the previous
   *   scenario.
   *
   * - not a 'must': we take the suggested time as a basis, and add to it some
   *   configuration / random delays.
   *
   *   locking: not using the jobs_lock
   */
  void update_scrub_job(ceph::ref_t<ScrubJob> sjob, const sched_params_t& suggested);

  /**
   * Add/modify the scrub-job of a specific PG
   *
   * The (possibly modified) scrub-job is added to the list of PGs to be periodically
   * scrubbed by the OSD.
   * The registration is active as long as the PG exists and the OSD is its primary.
   *
   * There are 3 argument combinations to consider:
   * - 'must' is asserted, and the suggested time is 'scrub_must_stamp':
   *   the registration will be with "beginning of time" target, making the
   *   PG eligible to immediate scrub (given that external conditions do not
   *   prevent scrubbing)
   *
   * - 'must' is asserted, and the suggested time is 'now':
   *   This happens if our stats are unknown. The results is similar to the previous
   *   scenario.
   *
   * - not a 'must': we take the suggested time as a basis, and add to it some
   *   configuration / random delays.
   */
  //void update_scrub_job(ceph::ref_t<ScrubJob> sjob, const sched_params_t& suggested);

 private:
  /**
   * Are there scrub-jobs that should be re-instated?
   */
  void scan_penalized(bool forgive_all, utime_t time_now);

 public:
  void dumps_scrub(ceph::Formatter* f) const;

  /**
   * No new scrub session will start while a scrub was initiate on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() { a_pg_is_reserving = true; }
  void clear_reserving_now() { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }

  bool can_inc_scrubs();
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();
  void dump_scrub_reservations(ceph::Formatter* f);

  double scrub_sleep_time(bool must_scrub) const;  // \todo return milliseconds


  /**
   *  called every heartbeat to update the "daily" load average
   *
   *  @returns a load value for the logger
   */
  [[nodiscard]] std::optional<double> update_load_average();

 private:
  CephContext* m_cct;
  OSDService& m_osds;

  /// scrub resources management lock
  mutable ceph::mutex resource_lock = ceph::make_mutex("ScrubQueue::resource_lock");

  /// job queues lock
  //mutable ceph::mutex jobs_lock = ceph::make_mutex("ScrubQueue::jobs_lock");
  mutable std::timed_mutex jobs_lock; // = ceph::make_mutex("ScrubQueue::jobs_lock");


  ScrubQContainer to_scrub;   ///< scrub-jobs (i.e. PGs) to scrub
  ScrubQContainer penalized;  ///< those that failed to reserve resources
  bool resort_needed{false};
  bool penal_list_changed{false};
  bool restore_penalized{false};

  double daily_loadavg{0.0};

  struct sched_order_t {
    bool operator()(const ScrubJob& lhs, const ScrubJob& rhs) const
    {
      if (!lhs.m_pg_being_removed) {

	if (rhs.m_pg_being_removed)
	  return true;

	return lhs.sched_time < rhs.sched_time;

      } else {
	return rhs.m_pg_being_removed;
      }
    }
  };

  struct indirect_order_t {
    bool operator()(const ceph::ref_t<ScrubJob>& lhs,
		    const ceph::ref_t<ScrubJob>& rhs) const
    {
      return sched_order_t{}(*lhs, *rhs);
    }
  };

  struct time_order_t {
    bool operator()(const ScrubJob& lhs, const ScrubJob& rhs) const
    {
      return lhs.sched_time < rhs.sched_time;
    }
  };

  struct time_indirect_t {
    bool operator()(const ceph::ref_t<ScrubJob>& lhs,
		    const ceph::ref_t<ScrubJob>& rhs) const
    {
      return lhs->sched_time < rhs->sched_time;
    }
  };

  static inline constexpr auto valid_job = [](const auto& jobref) {
    return !jobref->m_pg_being_removed;
  };

  static inline constexpr auto invalid_job = [](const auto& jobref) {
    return jobref->m_pg_being_removed;
  };

  static inline constexpr auto clear_upd_flag = [](const auto& jobref) -> void {
    jobref->m_updated = false;
  };

  // the counters used to manage scrub activity parallelism:
  int m_scrubs_local{0};
  int m_scrubs_remote{0};

  bool a_pg_is_reserving{false};

  [[nodiscard]] bool scrub_load_below_threshold() const;
  [[nodiscard]] bool scrub_time_permit(utime_t now) const;

  /**
   * If the scrub job was not explicitly requested, we postpone it by some
   * random length of time.
   * And if delaying the scrub - we calculate, based on pool parameters, a deadline
   * we should scrub before.
   *
   * @return a pair of values: the determined scrub time, and the deadline
   */
  TimeAndDeadline adjust_target_time(const sched_params_t& recomputed_params) const;

  /**
   *
   *
   * locking: called with job_lock held
   */
  void move_failed_pgs(utime_t now_is);

  Scrub::attempt_t select_from_group(ScrubQContainer& group,
				     const Scrub::ScrubPreconds& preconds,
				     utime_t now_is);
};
