// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/osd_scrub_sched.h"

#include "crimson/osd/osd.h"
//#include "crimson/osd/pg.h"
#include "crimson/osd/scrubber.h"

namespace crimson::osd {

// RRR RRR test that the if(must) below makes sense. also in classic.


ScrubQueue::ScrubJob::ScrubJob(CephContext* cct,
			       const spg_t& pg,
			       const utime_t& timestamp,
			       double pool_scrub_min_interval,
			       double pool_scrub_max_interval,
			       ScrubQueue::must_scrub_t must)
    : cct{cct}, pgid{pg}, sched_time{timestamp}, deadline{timestamp}
{
  // if not explicitly requested, postpone the scrub with a random delay
  if (must == ScrubQueue::must_scrub_t::not_mandatory) {
    double scrub_min_interval = pool_scrub_min_interval > 0
				  ? pool_scrub_min_interval
				  : cct->_conf->osd_scrub_min_interval;
    double scrub_max_interval = pool_scrub_max_interval > 0
				  ? pool_scrub_max_interval
				  : cct->_conf->osd_scrub_max_interval;

    sched_time += scrub_min_interval;
    double r = rand() / (double)RAND_MAX;
    sched_time += scrub_min_interval * cct->_conf->osd_scrub_interval_randomize_ratio * r;
    if (scrub_max_interval == 0) {
      deadline = utime_t();
    } else {
      deadline += scrub_max_interval;
    }
  }
}

ScrubQueue::ScrubJob::ScrubJob(CephContext* cct,
			       const spg_t& pg,
			       const utime_t& timestamp)
    : ScrubJob{cct, pg, timestamp, 0.0, 0.0, ScrubQueue::must_scrub_t::not_mandatory}
{}

bool ScrubQueue::ScrubJob::ScrubJob::operator<(const ScrubQueue::ScrubJob& rhs) const
{
  if (sched_time < rhs.sched_time)
    return true;
  if (sched_time > rhs.sched_time)
    return false;
  return pgid < rhs.pgid;
}

utime_t ScrubQueue::reg_pg_scrub(spg_t pgid,
				 utime_t t,
				 double pool_scrub_min_interval,
				 double pool_scrub_max_interval,
				 must_scrub_t must)
{
  ScrubJob scrub_job{m_cct, pgid, t, pool_scrub_min_interval, pool_scrub_max_interval,
		     must};
  std::lock_guard l(sched_scrub_lock);
  sched_scrub_pg.emplace(scrub_job);
  return scrub_job.sched_time;
}

void ScrubQueue::unreg_pg_scrub(spg_t pgid, utime_t t)
{
  std::lock_guard l(sched_scrub_lock);
  size_t removed = sched_scrub_pg.erase(ScrubJob(m_cct, pgid, t));
  ceph_assert(removed);
}


double ScrubQueue::scrub_sleep_time(bool must_scrub)
{
  if (must_scrub) {
    return m_cct->_conf->osd_scrub_sleep;
  }
  utime_t now = ceph_clock_now();
  if (scrub_time_permit(now)) {
    return m_cct->_conf->osd_scrub_sleep;
  }
  double normal_sleep = m_cct->_conf->osd_scrub_sleep;
  double extended_sleep = m_cct->_conf->osd_scrub_extended_sleep;
  return std::max(extended_sleep, normal_sleep);
}

bool ScrubQueue::scrub_load_below_threshold() const
{
  return true;
}

bool ScrubQueue::scrub_time_permit(utime_t now) const
{
  return true;
}


void ScrubQueue::dumps_scrub(ceph::Formatter* f)
{
  ceph_assert(f != nullptr);
  std::lock_guard l(sched_scrub_lock);

  f->open_array_section("scrubs");
  for (const auto& i : sched_scrub_pg) {
    f->open_object_section("scrub");
    f->dump_stream("pgid") << i.pgid;
    f->dump_stream("sched_time") << i.sched_time;
    f->dump_stream("deadline") << i.deadline;
    f->dump_bool("forced", i.sched_time == PgScrubber::scrub_must_stamp());
    f->close_section();
  }
  f->close_section();
}

void ScrubQueue::queue_scrub_event(PG const* pg,
				   MessageRef scrub_msg,
				   Scrub::scrub_prio_t with_priority)
{}

bool ScrubQueue::can_inc_scrubs()
{
  return true;
}
bool ScrubQueue::inc_scrubs_local()
{
  return true;
}
void ScrubQueue::dec_scrubs_local() {}
bool ScrubQueue::inc_scrubs_remote()
{
  return true;
}
void ScrubQueue::dec_scrubs_remote() {}
void ScrubQueue::dump_scrub_reservations(ceph::Formatter* f) {}


}  // namespace crimson::osd
