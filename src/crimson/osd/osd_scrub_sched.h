// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include <set>

#include "crimson/common/type_helpers.h"
#include "osd/osd_types.h"
#include <chrono>
#include "crimson/osd/scrubber_common_cr.h"
#include "crimson/osd/osd_operations/pg_scrub_event.h"
#include "crimson/osd/osd_operations/scrub_event.h"

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

  ScrubQueue(CephContext* cct, ShardServices& osds): m_cct{cct}, m_osds{osds} {}
  ScrubQueue(ShardServices& osds) : m_cct{new crimson::common::CephContext()}, m_osds{osds} {}

  struct ScrubJob {
    CephContext* cct;
    /// pg to be scrubbed
    spg_t pgid;
    /// a time scheduled for scrub. but the scrub could be delayed if system
    /// load is too high or it fails to fall in the scrub hours
    utime_t sched_time;
    /// the hard upper bound of scrub time
    utime_t deadline;
    ScrubJob() : cct(nullptr) {}

    ScrubJob(CephContext* cct, const spg_t& pg, const utime_t& timestamp);

    ScrubJob(CephContext* cct,
	     const spg_t& pg,
	     const utime_t& timestamp,
	     double pool_scrub_min_interval,
	     double pool_scrub_max_interval,
	     must_scrub_t must);
    /// order the jobs by sched_time
    bool operator<(const ScrubJob& rhs) const;
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

  ScrubQueueIter begin() const  { return ScrubQueueIter{*this}; }
  ScrubQueueIter end() const { return ScrubQueueIter{*this, true};  }

  std::set<ScrubJob> sched_scrub_pg;

  /// @returns the scrub_reg_stamp used for unregistering the scrub job
  utime_t reg_pg_scrub(spg_t pgid,
		       utime_t t,
		       double pool_scrub_min_interval,
		       double pool_scrub_max_interval,
		       must_scrub_t must);

  void unreg_pg_scrub(spg_t pgid, utime_t t);

#if 0
  std::optional<ScrubJob> first_scrub_stamp()
  {
    std::lock_guard l(sched_scrub_lock);
    if (sched_scrub_pg.empty())
      return std::nullopt;
    return *sched_scrub_pg.begin();
  }


  bool next_scrub_stamp(const ScrubJob& next, ScrubJob* out)
  {
    std::lock_guard l(sched_scrub_lock);
    if (sched_scrub_pg.empty())
      return false;
    std::set<ScrubJob>::const_iterator iter = sched_scrub_pg.lower_bound(next);
    if (iter == sched_scrub_pg.cend())
      return false;
    ++iter;
    if (iter == sched_scrub_pg.cend())
      return false;
    *out = *iter;
    return true;
  }
#endif

  void dumps_scrub(ceph::Formatter* f);

  bool can_inc_scrubs();
  bool inc_scrubs_local();
  void dec_scrubs_local();
  bool inc_scrubs_remote();
  void dec_scrubs_remote();
  void dump_scrub_reservations(ceph::Formatter* f);

  double scrub_sleep_time(bool must_scrub); // \todo return milliseconds

  void queue_scrub_event(PG const* pg, MessageRef scrub_msg, Scrub::scrub_prio_t with_priority);

  // public for now, until we moved osd::sched_scrub() here

  [[nodiscard]] bool scrub_load_below_threshold() const;
  [[nodiscard]] bool scrub_time_permit(utime_t now) const;

 private:
  CephContext* m_cct;
  ShardServices& m_osds;
  mutable ceph::mutex sched_scrub_lock = ceph::make_mutex("ScrubQueue::sched_scrub_lock");
  int m_scrubs_local{10};
  int m_scrubs_remote{10};
};

#ifdef NOTYET
class ScrubServices {
 public:

  template <class MSG_TYPE>
  void queue_scrub_event_msg(PG* pg, Scrub::scrub_prio_t with_priority)
  {
    const auto epoch = pg->get_osdmap_epoch();
    auto msg = new MSG_TYPE(pg->get_pgid(), epoch);
    dout(15) << "queue a scrub event (" << *msg << ") for " << *pg << ". Epoch: " << epoch << dendl;

    enqueue_back(OpSchedulerItem(
      unique_ptr<OpSchedulerItem::OpQueueable>(msg), cct->_conf->osd_scrub_cost,
      pg->scrub_requeue_priority(with_priority), ceph_clock_now(), 0, epoch));
  }


 private:


};
#endif

}  // namespace crimson::osd