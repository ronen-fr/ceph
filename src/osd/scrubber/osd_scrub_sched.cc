// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_scrub_sched.h"

#include "include/utime.h"
#include "osd/OSD.h"

#include "pg_scrubber.h"


#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd." << whoami << "  "

// ////////////////////////////////////////////////////////////////////////////////// //
// ScrubJob

ScrubQueue::ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    : RefCountedObject{cct}
    , sched_time{}
    , deadline{0, 0}
    , pgid{pg}
    , whoami{node_id}
    , cct{cct}
{}


// ////////////////////////////////////////////////////////////////////////////////// //
// ScrubQueue

#undef dout_context
#define dout_context (m_cct)
#undef dout_prefix
#define dout_prefix \
  *_dout << "osd." << m_osds.whoami << " scrub-queue::" << __func__ << " "


ScrubQueue::ScrubQueue(CephContext* cct, OSDService& osds) : m_cct{cct}, m_osds{osds}
{
  // initialize the daily loadavg with current 15min loadavg
  if (double loadavgs[3]; getloadavg(loadavgs, 3) == 3) {
    daily_loadavg = loadavgs[2];
  } else {
    derr << "OSD::init() : couldn't read loadavgs\n" << dendl;
    daily_loadavg = 1.0;
  }
}

std::optional<double> ScrubQueue::update_load_average()
{
  int hb_interval = m_cct->_conf->osd_heartbeat_interval;
  int n_samples = 86400;
  if (hb_interval > 1) {
    n_samples /= hb_interval;
    if (n_samples < 1)
      n_samples = 1;
  }

  // get CPU load avg
  double loadavgs[1];
  if (getloadavg(loadavgs, 1) == 1) {
    daily_loadavg = (daily_loadavg * (n_samples - 1) + loadavgs[0]) / n_samples;
    dout(17) << "heartbeat: daily_loadavg " << daily_loadavg << dendl;
    return 100 * loadavgs[0];
  }

  return std::nullopt;
}

void ScrubQueue::remove_from_osd_queue(ceph::ref_t<ScrubJob> scrub_job)
{
  if (scrub_job->m_in_queue) {
    dout(10) << " removing pg(" << scrub_job->pgid << ") from OSD scrub queue" << dendl;

    scrub_job->sched_time = {};	 // appearing in logs/asok

    const auto target_pg = scrub_job->pgid;
    auto same_pg = [&target_pg](const auto& jobref) { return jobref->pgid == target_pg; };

    // search for this entry in both queues
    // std::lock_guard l(jobs_lock);
    std::unique_lock l(jobs_lock, 100ms);
    ceph_assert(l.owns_lock());

    auto e = std::find_if(to_scrub.begin(), to_scrub.end(), same_pg);
    if (e == to_scrub.end()) {
      auto p = std::find_if(penalized.begin(), penalized.end(), same_pg);
      if (p != penalized.end()) {
	penalized.erase(p);
      }
    } else {
      to_scrub.erase(e);
    }

    scrub_job->m_in_queue = false;
  }
}

void ScrubQueue::register_with_osd(ceph::ref_t<ScrubJob> scrub_job,
				   const ScrubQueue::sched_params_t& suggested)
{
  // a temporary work-around until I figure why we had to call on_maybe_registration_change()
  if (scrub_job->m_in_queue) {
    // must not lock
    dout(10) << "called while already queued" << dendl;
    update_scrub_job(scrub_job, suggested);
    return;
  }

  std::unique_lock l(jobs_lock, 100ms);
  ceph_assert(l.owns_lock());

  if (scrub_job->m_in_queue) {
    // (checked now with the lock held)
    dout(5) << "already registered!!!" << dendl;
    scrub_job->m_updated = true;
    return;
  }

  // adjust the suggested scrub time according to OSD-wide status
  auto adjusted = adjust_target_time(suggested);
  scrub_job->set_time_n_deadline(adjusted);

  to_scrub.push_back(scrub_job);
  scrub_job->m_in_queue = true;
  scrub_job->m_updated = true;
  l.unlock();

  dout(10) << " pg(" << scrub_job->pgid << ") inserted: " << scrub_job->sched_time
	   << dendl;
}

// look mommy - no locks!
void ScrubQueue::update_scrub_job(ceph::ref_t<ScrubJob> scrub_job,
				  const ScrubQueue::sched_params_t& suggested)
{
  // adjust the suggested scrub time according to OSD-wide status
  auto adjusted = adjust_target_time(suggested);
  scrub_job->set_time_n_deadline(adjusted);

  dout(10) << " pg(" << scrub_job->pgid << ") adjusted: " << scrub_job->sched_time
	   << " RC " << scrub_job->get_nref() << "  (inQ:" << scrub_job->m_in_queue << ")"
	   << dendl;

  scrub_job->penalty_timeout = utime_t(0, 0);  // helps with debugging
  scrub_job->m_updated = true;
}


// used under lock
void ScrubQueue::move_failed_pgs(utime_t now_is)
{
  int debug_cnt{0};

  for (auto jb = to_scrub.begin(); jb != to_scrub.end();) {

    if ((*jb)->m_resources_failure) {

      auto sjob = *jb;

      dout(15) << " moving " << sjob->pgid << " rc: " << (*jb)->get_nref() << dendl;

      // determine the penalty time, after which the job should be reinstated
      utime_t after = now_is;
      after += m_cct->_conf->osd_scrub_sleep * 2 + utime_t{300'000ms};
      // if (sjob->deadline > 0.0) {
      // sjob->penalty_timeout = std::min(sjob->deadline, after);
      //} else {
      sjob->penalty_timeout = after;
      //}
      sjob->m_resources_failure = false;
      sjob->m_updated = false;	// as otherwise will immediately be pardoned

      // place in the penalty list, and remove from the to-scrub group
      penalized.push_back(sjob);
      jb = to_scrub.erase(jb);
      debug_cnt++;

    } else {
      jb++;
    }
  }

  if (debug_cnt) {
    dout(15) << "jobs punished #" << debug_cnt << dendl;
  }
}


Scrub::attempt_t ScrubQueue::select_pg_and_scrub(Scrub::ScrubPreconds& preconds)
{
  dout(10) << " reg./pen. sizes: " << to_scrub.size() << " / " << penalized.size()
	   << dendl;

  utime_t now_is = ceph_clock_now();

  preconds.time_permit = scrub_time_permit(now_is);
  preconds.load_is_low = scrub_load_below_threshold();
  preconds.only_deadlined = !preconds.time_permit || !preconds.load_is_low;

  // std::lock_guard l(jobs_lock);
  std::unique_lock l(jobs_lock, 100ms);
  ceph_assert(l.owns_lock());

  // pardon all penalized jobs that have deadlined (or were updated)
  scan_penalized(restore_penalized, now_is);

  // remove the 'updated' flag from all entries
  std::for_each(to_scrub.begin(), to_scrub.end(), clear_upd_flag);

  // add failed scrub attempts to the penalized list
  move_failed_pgs(now_is);

  // try the regular queue first
  auto res = select_from_group(to_scrub, preconds, now_is);

  // in the sole scenario in which we've gone over all ripe jobs without success - we
  // will try the penalized
  if (res == Scrub::attempt_t::none_ready && !penalized.empty()) {
    res = select_from_group(penalized, preconds, now_is);
    dout(10) << "tried the penalized. Res: " << (int)res << dendl;
    restore_penalized = true;
  }

  dout(15) << dendl;
  return res;
}

// reminder: 'group' here is either 'to_scrub' or 'penalized'
Scrub::attempt_t ScrubQueue::select_from_group(ScrubQContainer& group,
					       const Scrub::ScrubPreconds& preconds,
					       utime_t now_is)
{
  dout(15) << dendl;

  auto first_bad = std::partition(group.begin(), group.end(), valid_job);
  group.erase(first_bad, group.end());

  std::sort(group.begin(), group.end(), time_indirect_t{});

  for (auto jb = group.begin(); jb != group.end(); ++jb) {

    // usually - we expect the first job in the list to be a good candidate
    auto candidate = *jb;

    dout(20) << "try initiating scrub for " << candidate->pgid << dendl;

    if (candidate->sched_time > now_is) {
      dout(20) << "candidate not ripe " << candidate->pgid << " @ "
	       << candidate->sched_time << dendl;
      // finished with non-penalized. Now's a good time to restore all penalized.
      return Scrub::attempt_t::none_ready;
    }

    if (preconds.only_deadlined &&
	(candidate->deadline.is_zero() || candidate->deadline >= now_is)) {
      dout(15) << " not scheduling scrub for " << candidate->pgid << " due to "
	       << (preconds.time_permit ? "high load" : "time not permitting") << dendl;
      continue;
    }

    // we have a candidate to scrub. We turn to the OSD to verify that the PG configuration
    // allows the specified type of scrub, and to initiate the scrub.
    switch (
      m_osds.initiate_a_scrub(candidate->pgid, preconds.allow_requested_repair_only)) {

      case Scrub::attempt_t::scrubbing:
	// the happy path. We are done
	dout(20) << " initiated for " << candidate->pgid << dendl;
	return Scrub::attempt_t::scrubbing;

      case Scrub::attempt_t::already_started:
      case Scrub::attempt_t::preconditions:
      case Scrub::attempt_t::pg_state:
	// continue with the next job
	dout(20) << " failed (state/cond/started) " << candidate->pgid << dendl;
	break;

      case Scrub::attempt_t::no_pg:
	// Couldn't lock the pg. Shouldn't really happen
	dout(20) << " failed (no pg) " << candidate->pgid << dendl;
	break;

      case Scrub::attempt_t::local_resources:
	// failure to secure local resources. No point in trying the other
	// PGs at this time. Note that this is not the same as replica resources
	// failure!
	dout(20) << " failed (local) " << candidate->pgid << dendl;
	return Scrub::attempt_t::local_resources;

      case Scrub::attempt_t::none_ready:
	// can't happen. Just for the compiler.
	dout(20) << " failed !!! " << candidate->pgid << dendl;
	return Scrub::attempt_t::none_ready;
    }
  }

  dout(20) << " returning 'none ready' " << dendl;
  return Scrub::attempt_t::none_ready;
}

ScrubQueue::TimeAndDeadline ScrubQueue::adjust_target_time(
  const sched_params_t& times) const
{
  ScrubQueue::TimeAndDeadline sched_n_dead{times.suggested_stamp, times.suggested_stamp};

  if (times.is_must == ScrubQueue::must_scrub_t::not_mandatory) {

    // if not explicitly requested, postpone the scrub with a random delay

    double scrub_min_interval =
      times.min_interval > 0 ? times.min_interval : m_cct->_conf->osd_scrub_min_interval;
    double scrub_max_interval =
      times.max_interval > 0 ? times.max_interval : m_cct->_conf->osd_scrub_max_interval;

    sched_n_dead.first += scrub_min_interval;
    double r = rand() / (double)RAND_MAX;
    sched_n_dead.first +=
      scrub_min_interval * m_cct->_conf->osd_scrub_interval_randomize_ratio * r;

    if (scrub_max_interval <= 0) {
      sched_n_dead.second = utime_t{};
    } else {
      sched_n_dead.second += scrub_max_interval;
    }
  }

  return sched_n_dead;
}

double ScrubQueue::scrub_sleep_time(bool must_scrub) const
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
  double loadavgs[3];
  if (getloadavg(loadavgs, 3) != 3) {
    dout(10) << __func__ << " couldn't read loadavgs\n" << dendl;
    return false;
  }

  // allow scrub if below configured threshold
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  double loadavg_per_cpu = cpus > 0 ? loadavgs[0] / cpus : loadavgs[0];
  if (loadavg_per_cpu < m_cct->_conf->osd_scrub_load_threshold) {
    dout(20) << __func__ << " loadavg per cpu " << loadavg_per_cpu << " < max "
	     << m_cct->_conf->osd_scrub_load_threshold << " = yes" << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (loadavgs[0] < daily_loadavg && loadavgs[0] < loadavgs[2]) {
    dout(20) << __func__ << " loadavg " << loadavgs[0] << " < daily_loadavg "
	     << daily_loadavg << " and < 15m avg " << loadavgs[2] << " = yes" << dendl;
    return true;
  }

  dout(20) << __func__ << " loadavg " << loadavgs[0] << " >= max "
	   << m_cct->_conf->osd_scrub_load_threshold << " and ( >= daily_loadavg "
	   << daily_loadavg << " or >= 15m avg " << loadavgs[2] << ") = no" << dendl;
  return false;
}


// note: called with jobs_lock held
void ScrubQueue::scan_penalized(bool forgive_all, utime_t time_now)
{
  dout(20) << time_now << (forgive_all ? " all " : " - ") << penalized.size() << dendl;

  // clear dead entries
  penalized.erase(std::remove_if(penalized.begin(), penalized.end(), invalid_job),
		  penalized.end());

  if (forgive_all) {

    std::copy(penalized.begin(), penalized.end(), std::back_inserter(to_scrub));
    penalized.clear();

  } else {

    auto forgiven_last =
      std::partition(penalized.begin(), penalized.end(), [time_now](const auto& e) {
	return (*e).m_updated || ((*e).penalty_timeout <= time_now);
      });

    std::copy(penalized.begin(), forgiven_last, std::back_inserter(to_scrub));
    penalized.erase(penalized.begin(), forgiven_last);
    dout(20) << "penalized after screening: " << penalized.size() << dendl;
  }
}

// checks for half-closed ranges. Modify the (p<till)to '<=' to check for closed.
static inline bool isbetween_modulo(int64_t from, int64_t till, int p)
{
  // the 1st condition is because we have defined from==till as "always true"
  return (till == from) || ((till >= from) ^ (p >= from) ^ (p < till));
}

bool ScrubQueue::scrub_time_permit(utime_t now) const
{
  tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);

  bool day_permit = isbetween_modulo(m_cct->_conf->osd_scrub_begin_week_day,
				     m_cct->_conf->osd_scrub_end_week_day, bdt.tm_wday);
  if (!day_permit) {
    dout(20) << __func__ << " should run between week day "
	     << m_cct->_conf->osd_scrub_begin_week_day << " - "
	     << m_cct->_conf->osd_scrub_end_week_day << " now " << bdt.tm_wday << " - no"
	     << dendl;
    return false;
  }

  bool time_permit = isbetween_modulo(m_cct->_conf->osd_scrub_begin_hour,
				      m_cct->_conf->osd_scrub_end_hour, bdt.tm_hour);
  dout(20) << __func__ << " should run between " << m_cct->_conf->osd_scrub_begin_hour
	   << " - " << m_cct->_conf->osd_scrub_end_hour << " now (" << bdt.tm_hour
	   << ") = " << (time_permit ? "yes" : "no") << dendl;
  return time_permit;
}

void ScrubQueue::ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << sched_time;
  f->dump_stream("deadline") << deadline;
  f->dump_bool("forced", sched_time == PgScrubber::scrub_must_stamp());
  f->close_section();
}

void ScrubQueue::dumps_scrub(ceph::Formatter* f) const
{
  ceph_assert(f != nullptr);
  std::lock_guard l(jobs_lock);

  f->open_array_section("scrubs");

  for_each(to_scrub.cbegin(), to_scrub.cend(),
	   [&f](const ScrubJobRef& j) { j->dump(f); });

  for_each(penalized.cbegin(), penalized.cend(),
	   [&f](const ScrubJobRef& j) { j->dump(f); });

  f->close_section();
}

ScrubQueue::ScrubQContainer ScrubQueue::list_all_valid() const
{
  ScrubQueue::ScrubQContainer all_jobs;
  all_jobs.reserve(to_scrub.size() + penalized.size());

  // lock_guard l{jobs_lock};
  std::unique_lock l(jobs_lock, 100ms);
  ceph_assert(l.owns_lock());

  std::copy_if(to_scrub.begin(), to_scrub.end(), std::back_inserter(all_jobs), valid_job);
  std::copy_if(penalized.begin(), penalized.end(), std::back_inserter(all_jobs),
	       valid_job);

  return all_jobs;
}

// ////////////////////////////////////////////////////////////////////////////////// //
// ScrubJob - scrub resource management

bool ScrubQueue::can_inc_scrubs()
{
  // consider removing the lock here. Caller code already handles delayed
  // inc_scrubs_local() failures
  std::lock_guard l{resource_lock};

  if (m_scrubs_local + m_scrubs_remote < m_cct->_conf->osd_max_scrubs) {
    return true;
  }

  dout(20) << __func__ << " == false " << m_scrubs_local << " local + " << m_scrubs_remote
	   << " remote >= max " << m_cct->_conf->osd_max_scrubs << dendl;
  return false;
}

bool ScrubQueue::inc_scrubs_local()
{
  std::lock_guard l{resource_lock};

  if (m_scrubs_local + m_scrubs_remote < m_cct->_conf->osd_max_scrubs) {
    ++m_scrubs_local;
    return true;
  }

  dout(20) << __func__ << " " << m_scrubs_local << " local + " << m_scrubs_remote
	   << " remote >= max " << m_cct->_conf->osd_max_scrubs << dendl;
  return false;
}

void ScrubQueue::dec_scrubs_local()
{
  dout(20) << __func__ << " " << m_scrubs_local << " -> " << (m_scrubs_local - 1)
	   << " (max " << m_cct->_conf->osd_max_scrubs << ", remote " << m_scrubs_remote
	   << ")" << dendl;

  std::lock_guard l{resource_lock};
  --m_scrubs_local;
  ceph_assert(m_scrubs_local >= 0);
}

bool ScrubQueue::inc_scrubs_remote()
{
  std::lock_guard l{resource_lock};

  if (m_scrubs_local + m_scrubs_remote < m_cct->_conf->osd_max_scrubs) {
    dout(20) << __func__ << " " << m_scrubs_remote << " -> " << (m_scrubs_remote + 1)
	     << " (max " << m_cct->_conf->osd_max_scrubs << ", local " << m_scrubs_local
	     << ")" << dendl;
    ++m_scrubs_remote;
    return true;
  }

  dout(20) << __func__ << " " << m_scrubs_local << " local + " << m_scrubs_remote
	   << " remote >= max " << m_cct->_conf->osd_max_scrubs << dendl;
  return false;
}

void ScrubQueue::dec_scrubs_remote()
{
  dout(20) << __func__ << " " << m_scrubs_remote << " -> " << (m_scrubs_remote - 1)
	   << " (max " << m_cct->_conf->osd_max_scrubs << ", local " << m_scrubs_local
	   << ")" << dendl;
  std::lock_guard l{resource_lock};
  --m_scrubs_remote;
  ceph_assert(m_scrubs_remote >= 0);
}

void ScrubQueue::dump_scrub_reservations(ceph::Formatter* f)
{
  std::lock_guard l{resource_lock};
  f->dump_int("scrubs_local", m_scrubs_local);
  f->dump_int("scrubs_remote", m_scrubs_remote);
  f->dump_int("osd_max_scrubs", m_cct->_conf->osd_max_scrubs);
}
