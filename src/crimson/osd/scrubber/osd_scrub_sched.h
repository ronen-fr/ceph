// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <chrono>
#include <memory>
#include <set>

#include "crimson/common/type_helpers.h"
//#include "crimson/osd/osd_operations/pg_scrub_event.h"
#include "crimson/osd/osd_operations/scrub_event.h"
#include "crimson/osd/scrubber_common_cr.h"
#include "osd/osd_types.h"

#include "utime.h"

/// \todo replace the 'set' with small-set

namespace crimson::osd {

class ShardServices;

/**
 * the ordered queue of PGs waiting to be scrubbed.
 * Main operations are scheduling/unscheduling a PG to be scrubbed at a certain time.
 */
class ScrubQueue {
 public:
  enum class must_scrub_t { not_mandatory, mandatory };

  ScrubQueue(CephContext* cct, ShardServices& osds) : m_cct{cct}, m_osds{osds} {}
  // ScrubQueue(ShardServices& osds) : m_cct{new crimson::common::CephContext()},
  // m_osds{osds} {}
  using TimeAndDeadline = std::pair<utime_t, utime_t>;

  struct ScrubJob {

    CephContext* cct;

    /// pg to be scrubbed
    spg_t pgid;

    /// a time scheduled for scrub. but the scrub could be delayed if system
    /// load is too high or it fails to fall in the scrub hours
    utime_t sched_time;

    /// the hard upper bound of scrub time
    utime_t deadline;

    /// configuration values for this pool
    /// \todo handle value changes
    double pool_scrub_min_interval{0.0};		// change to ms or seconds
    double pool_scrub_max_interval{1'000'000.0};

    ScrubJob() : cct(nullptr) {} // needed?

    ScrubJob(CephContext* cct, const spg_t& pg, const utime_t& timestamp);

    ScrubJob(CephContext* cct,
	     const spg_t& pg,
	     const utime_t& timestamp,
	     double pool_scrub_min_interval,
	     double pool_scrub_max_interval,
	     must_scrub_t must);

    /// order the jobs by sched_time
    bool operator<(const ScrubJob& rhs) const;

    void set_time_n_deadline(TimeAndDeadline ts) {
      sched_time = ts.first;
      deadline = ts.second;
    }
  };

  using ScrubQContainer = std::set<ScrubJob>;

  struct ScrubQueueIter {
    using iterator_category = std::forward_iterator_tag;
    using difference_type =
      typename std::iterator_traits<ScrubQContainer::const_iterator>::difference_type;
    using value_type = ScrubJob;
    using pointer = ScrubJob const*;
    using reference = ScrubJob const&;

    explicit ScrubQueueIter(ScrubQueue const& sq) : sq_{sq}
    {
      lock_guard l{sq_.sched_scrub_lock};
      it_ = sq_.sched_scrub_pg.begin();
    }
    explicit ScrubQueueIter(ScrubQueue const& sq, bool) : sq_{sq}
    {
      lock_guard l{sq_.sched_scrub_lock};
      it_ = sq_.sched_scrub_pg.end();
    }

    bool operator!=(ScrubQueueIter const& r) const
    {
      lock_guard l{sq_.sched_scrub_lock};
      return it_ != r.it_;
    }
    ScrubQueueIter::value_type operator*() const
    {
      lock_guard l{sq_.sched_scrub_lock};
      return *it_;
    }
    ScrubQueueIter const& operator++()
    {
      lock_guard l{sq_.sched_scrub_lock};
      ++it_;
      return *this;
    }

    ScrubQueue const& sq_;
    ScrubQContainer::const_iterator it_;
  };

  ScrubQueueIter begin() const
  {
    return ScrubQueueIter{*this};
  }
  ScrubQueueIter end() const
  {
    return ScrubQueueIter{*this, true};
  }

  std::set<ScrubJob> sched_scrub_pg; // can be moved now to private?

  // the revised interface:

  /**
   * Add a PG to the list of PGs to be periodically scrubbed by the OSD.
   *
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
   * @return the scheduled time for scrubbing, to be used as a key to manage the
   * 	registration.
   */
  utime_t add_to_osd_queue(/*CephContext* cct, */const spg_t& pgid, utime_t timestamp,
			   double pool_scrub_min_interval,
			   double pool_scrub_max_interval,
			   must_scrub_t must);


  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  void remove_from_osd_queue(const spg_t& pg);

  struct sched_params_t {
    utime_t suggested_stamp{};
    double min_interval{0.0};
    double max_interval{0.0};
    must_scrub_t is_must{ScrubQueue::must_scrub_t::not_mandatory};
    bool ok_to_scrub{false};
  };

  using urgency_manip_t = std::function<sched_params_t ()>;

  utime_t update_scrub_job(const spg_t& pgid, utime_t key, utime_t new_time, must_scrub_t must);

  utime_t update_scrub_job(const spg_t& pgid, utime_t key, urgency_manip_t&&);


  // the legacy interface:

  /// @returns the scrub_reg_stamp used for unregistering the scrub job
  utime_t reg_pg_scrub(spg_t pgid,
		       utime_t t,
		       double pool_scrub_min_interval,
		       double pool_scrub_max_interval,
		       must_scrub_t must);

  void unreg_pg_scrub(spg_t pgid, utime_t t);

  void dumps_scrub(ceph::Formatter* f);

  bool can_inc_scrubs();
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();
  void dump_scrub_reservations(ceph::Formatter* f);

  double scrub_sleep_time(bool must_scrub);  // \todo return milliseconds

  void queue_scrub_event(PG const* pg,
			 MessageRef scrub_msg,
			 Scrub::scrub_prio_t with_priority);

  // public for now, until we moved osd::sched_scrub() here

  [[nodiscard]] bool scrub_load_below_threshold() const;
  [[nodiscard]] bool scrub_time_permit(utime_t now) const;

 private:
  CephContext* m_cct;
  ShardServices& m_osds;
  mutable ceph::mutex sched_scrub_lock = ceph::make_mutex("ScrubQueue::sched_scrub_lock");


  // the counters used to manage scrub activity parallelism:
  int m_scrubs_local{0};
  int m_scrubs_remote{0};
  static inline constexpr int temp_max_scrubs_local{2};
  static inline constexpr int temp_max_scrubs_remote{10};


  /**
   * If the scrub job was not explicitly requested, we postpone it by some
   * random length of time.
   * And if delaying the scrub - we calculate, based on pool parameters, a deadline
   * we should scrub before.
   *
   * @param suggested
   * @param pool_scrub_min_interval
   * @param pool_scrub_max_interval
   * @param must is this scrub operator-requested or oetherwise a 'must'?
   * @return a pair of values: the determined scrub time, and the deadline
   */
  TimeAndDeadline adjust_target_time(utime_t suggested,
			     double pool_scrub_min_interval,
			     double pool_scrub_max_interval,
			     must_scrub_t must);

  TimeAndDeadline adjust_target_time(const sched_params_t& recomputed_params);
};

#ifdef NOTYET
class ScrubServices {
 public:
  template <class MSG_TYPE>
  void queue_scrub_event_msg(PG* pg, Scrub::scrub_prio_t with_priority)
  {
    const auto epoch = pg->get_osdmap_epoch();
    auto msg = new MSG_TYPE(pg->get_pgid(), epoch);
    dout(15) << "queue a scrub event (" << *msg << ") for " << *pg << ". Epoch: " << epoch
	     << dendl;

    enqueue_back(OpSchedulerItem(
      unique_ptr<OpSchedulerItem::OpQueueable>(msg), cct->_conf->osd_scrub_cost,
      pg->scrub_requeue_priority(with_priority), ceph_clock_now(), 0, epoch));
  }


 private:
};
#endif

}  // namespace crimson::osd