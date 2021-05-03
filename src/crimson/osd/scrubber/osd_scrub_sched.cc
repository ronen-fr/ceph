// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/scrubber/osd_scrub_sched.h"

#include "crimson/osd/osd.h"
#include "crimson/osd/scrubber/scrubber.h"


namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

namespace crimson::osd {

// RRR RRR test that the if(must) below makes sense. also in classic.

// RRR are we still using the ctor with any meaningful parameters?

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

  logger().debug("Scrubber: {}: scrub-job with {} created", __func__, sched_time);
}

ScrubQueue::ScrubJob::ScrubJob(CephContext* cct,
			       const spg_t& pg,
			       const utime_t& timestamp)
    : ScrubJob{cct, pg, timestamp, 0.0, 0.0, ScrubQueue::must_scrub_t::mandatory}
{}

bool ScrubQueue::ScrubJob::ScrubJob::operator<(const ScrubQueue::ScrubJob& rhs) const
{
  if (sched_time < rhs.sched_time)
    return true;
  if (sched_time > rhs.sched_time)
    return false;
  return pgid < rhs.pgid;
}

utime_t ScrubQueue::add_to_osd_queue(/*CephContext* cct,*/ const spg_t& pgid,
				     utime_t timestamp,
				     double pool_scrub_min_interval,
				     double pool_scrub_max_interval,
				     must_scrub_t must)
{
  ScrubJob scrub_job{
    m_cct, pgid, timestamp, pool_scrub_min_interval, pool_scrub_max_interval, must};
  std::lock_guard l(sched_scrub_lock);
  sched_scrub_pg.emplace(scrub_job);

  logger().warn("Scrubber: {}: pg({}) added to OSD scrubbing queue ({})", __func__, pgid,
		scrub_job.sched_time);
  return scrub_job.sched_time;
}


void ScrubQueue::remove_from_osd_queue(const spg_t& pgid)
{
  logger().debug("Scrubber: {}: removing pg({}) from OSD scrubbing queue", __func__,
		 pgid);

  std::lock_guard l(sched_scrub_lock);
  for (auto i = sched_scrub_pg.begin(); i != sched_scrub_pg.end();) {

    if (i->pgid == pgid) {
      i = sched_scrub_pg.erase(i);
    } else {
      i++;
    }
  }
}

// note that updates both 'sched_time' and 'deadline'
utime_t ScrubQueue::update_scrub_job(/*CephContext* cct,*/ const spg_t& pgid,
				     utime_t key,
				     utime_t new_time,
				     must_scrub_t must)
{
  std::lock_guard l(sched_scrub_lock);

  // the PG is supposed to be in the queue, but for now - we'll handle both situations
  auto job = sched_scrub_pg.extract(ScrubJob(m_cct, pgid, key));

  if (job.empty()) {

    // (verify that that's not a bug RRR)
    ScrubJob scrub_job{m_cct, pgid, new_time, 0.0, 0.0, must};
    sched_scrub_pg.emplace(scrub_job);
    logger().debug("Scrubber: {}: pg({}) scrub-job with {} created", __func__, pgid,
		   scrub_job.sched_time);
    return scrub_job.sched_time;

  } else {

    auto adjusted_time = adjust_target_time(new_time, 0.0, 0.0, must);
    job.value().set_time_n_deadline(adjusted_time);
    sched_scrub_pg.insert(std::move(job));
    // sched_scrub_lock.unlock();
    logger().debug("Scrubber: {}: adjust pg({}) scrub to {}", __func__, pgid,
		   adjusted_time.first);
    return adjusted_time.first;
  }
}

// here: assume the PG is already in the queue
utime_t ScrubQueue::update_scrub_job(const spg_t& pgid,
				     utime_t key,
				     urgency_manip_t&& manip)
{
  std::lock_guard l(sched_scrub_lock);

  auto job = sched_scrub_pg.extract(ScrubJob(m_cct, pgid, key)); // RRR this is wasteful (creating a temp job). Can we do better?
  // for now ceph_assert(!job.empty());

  // the PG is out of the scrub-jobs queue for now. Call the closure argument to
  // (usually) manipulate the next-scrub flags, and to determine its priority

  ScrubQueue::sched_params_t next_time =
    manip();  // later on - change to return a structure that includes the pool intervals

  auto adjusted_time = adjust_target_time(next_time);
  job.value().set_time_n_deadline(adjusted_time);
  sched_scrub_pg.insert(std::move(job));

  logger().debug("Scrubber: {}: adjust pg({}) scrub to {} <{}>", __func__, pgid,
		 adjusted_time.first, sched_scrub_pg.size());
  return adjusted_time.first;
}

#if 0
// here: assume the PG is already in the queue
utime_t ScrubQueue::update_scrub_job(const spg_t& pgid, utime_t key, utime_t new_time, urgency_manip_t&& manip)
{
  std::lock_guard l(sched_scrub_lock);

  auto job = sched_scrub_pg.extract(ScrubJob(m_cct, pgid, key));
  ceph_assert(!job.empty());

  // the PG is out of the scrub-jobs queue for now. Call the closure argument to
  // (usually) manipulate the next-scrub flags, and to determine its priority

  auto urgency = manip(); // later on - change to return a structure that includes the pool intervals

  auto adjusted_time = adjust_target_time(new_time, 0.0, 0.0, urgency);
  job.value().set_time_n_deadline(adjusted_time);
  sched_scrub_pg.insert(std::move(job));

  logger().debug("Scrubber: {}: adjust pg({}) scrub to {}", __func__, pgid, adjusted_time.first);
  return adjusted_time.first;
}
#endif

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
  logger().debug("Scrubber: ScrubQueue::{}: unreg {} with {}", __func__, pgid, t);

  std::lock_guard l(sched_scrub_lock);
  size_t removed = sched_scrub_pg.erase(ScrubJob(m_cct, pgid, t));
  logger().debug("Scrubber: ScrubQueue::{}: {} unreg compared with {}", __func__, removed,
		 ScrubJob{m_cct, pgid, t}.sched_time);
  // RRR restore ceph_assert(removed);
}


ScrubQueue::TimeAndDeadline ScrubQueue::adjust_target_time(utime_t suggested,
							   double pool_scrub_min_interval,
							   double pool_scrub_max_interval,
							   must_scrub_t must)
{
  ScrubQueue::TimeAndDeadline sched_n_dead{suggested, suggested};

  if (must == ScrubQueue::must_scrub_t::not_mandatory) {

    // if not explicitly requested, postpone the scrub with a random delay

    double scrub_min_interval = pool_scrub_min_interval > 0
				  ? pool_scrub_min_interval
				  : m_cct->_conf->osd_scrub_min_interval;
    double scrub_max_interval = pool_scrub_max_interval > 0
				  ? pool_scrub_max_interval
				  : m_cct->_conf->osd_scrub_max_interval;

    sched_n_dead.first += scrub_min_interval;
    double r = rand() / (double)RAND_MAX;
    sched_n_dead.first += scrub_min_interval *
			  m_cct->_conf->osd_scrub_interval_randomize_ratio * r;

    if (scrub_max_interval == 0) {
      sched_n_dead.second = utime_t();
    } else {
      sched_n_dead.second += scrub_max_interval;
    }
  }

  return sched_n_dead;
  // logger().debug("Scrubber: {}: scrub time calculated: {}", __func__, suggested);
}

ScrubQueue::TimeAndDeadline ScrubQueue::adjust_target_time(const sched_params_t& times)
{
  return adjust_target_time(times.suggested_stamp, times.min_interval, times.max_interval,
			    times.is_must);
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


static inline bool isbetween_modulo(int from, int till, int p)
{
  // the 1st condition is because we have defined from==till as "always true"
  return (till == from) || ((till >= from) ^ (p >= from) ^ (p <= till));
}

// modify the arguments to consider 'must' here?
bool ScrubQueue::scrub_time_permit(utime_t now) const
{
  struct tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);

  bool day_permit = isbetween_modulo(m_cct->_conf->osd_scrub_begin_week_day,
				     m_cct->_conf->osd_scrub_end_week_day, bdt.tm_wday);
  if (!day_permit) {
    logger().debug("scrubber: {}: should scrub in week days {} to {}. Now ({}) - no",
		   __func__, m_cct->_conf->osd_scrub_begin_week_day,
		   m_cct->_conf->osd_scrub_end_week_day, bdt.tm_wday);
    return false;
  }

  bool time_permit = isbetween_modulo(m_cct->_conf->osd_scrub_begin_hour,
				      m_cct->_conf->osd_scrub_end_hour, bdt.tm_hour);
  logger().debug("scrubber: {}: should scrub between {} and {}. Now ({}) - {}", __func__,
		 m_cct->_conf->osd_scrub_begin_hour, m_cct->_conf->osd_scrub_end_hour,
		 bdt.tm_hour, (time_permit ? "yes" : "no"));
  return time_permit;
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
  logger().debug("Scrubber: {}: local now {} v", __func__, m_scrubs_local);

  if (m_scrubs_local >= temp_max_scrubs_local)
    return false;

  //++m_scrubs_local;
  // logger().debug("dbg Scrubber: {}: local now {} ^", __func__, m_scrubs_local);
  return true;
}

bool ScrubQueue::inc_scrubs_local()
{
  logger().debug("Scrubber: {}: local now {}", __func__, m_scrubs_local);
  if (m_scrubs_local >= temp_max_scrubs_local)
    return false;

  ++m_scrubs_local;
  logger().debug("dbg Scrubber: {}: local now {} ^", __func__, m_scrubs_local);
  return true;
}

void ScrubQueue::dec_scrubs_local()
{
  logger().debug("Scrubber: {}: local now {} v", __func__, m_scrubs_local);
  m_scrubs_local = std::max(0, m_scrubs_local - 1);
  logger().debug("dbg Scrubber: {}: local now {} ^", __func__, m_scrubs_local);
}

bool ScrubQueue::inc_scrubs_remote()
{
  return true;
}

void ScrubQueue::dec_scrubs_remote() {}
void ScrubQueue::dump_scrub_reservations(ceph::Formatter* f) {}


}  // namespace crimson::osd
