// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd_scrub_sched.h"

#include "include/utime.h"
#include "osd/OSD.h"

#include "pg_scrubber.h"


// ////////////////////////////////////////////////////////////////////////////////// //
// ScrubJob

#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd." << whoami << "  "

ScrubQueue::ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    : RefCountedObject{cct}
    , m_sched_time{}
    , m_deadline{0, 0}
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
  int n_samples = 60 * 24 * 24;
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

/*
 * Modify the scrub-job state:
 * - if 'registsred' (as expected): mark as 'unregistering'. The job will be
 *   dequeued the next time sched_scrub() is called.
 * - if already 'not_registered': shouldn't really happen, but not a problem.
 *   The state will not be modified.
 * - same for 'unregistering'.
 *
 * Note: not holding the jobs lock
 */
void ScrubQueue::remove_from_osd_queue(ceph::ref_t<ScrubJob> scrub_job)
{
  dout(10) << "removing pg(" << scrub_job->pgid << ") from OSD scrub queue" << dendl;

  qu_state_t expected_state{qu_state_t::registered};
  auto ret =
    scrub_job->m_state.compare_exchange_strong(expected_state, qu_state_t::unregistering);

  if (ret) {

    dout(10) << "pg(" << scrub_job->pgid << ") sched-state changed from "
	     << qu_state_text(expected_state) << " to "
	     << qu_state_text(scrub_job->m_state) << dendl;

  } else {

    // job wasn't in state 'registered' coming in
    dout(5) << "removing pg(" << scrub_job->pgid
	    << ") failed. State was: " << qu_state_text(expected_state) << dendl;
  }
}

void ScrubQueue::register_with_osd(ceph::ref_t<ScrubJob> scrub_job,
				   const ScrubQueue::sched_params_t& suggested)
{
  qu_state_t verif = scrub_job->m_state.load();

  switch (scrub_job->m_state) {
    case qu_state_t::registered:
      // just updating the schedule?
      update_job(scrub_job, suggested);
      break;

    case qu_state_t::not_registered:
      // insertion under lock
      {
	std::unique_lock l(jobs_lock, 2'000ms);
	ceph_assert(l.owns_lock());
	if (verif != scrub_job->m_state) {
	  l.unlock();
	  dout(5) << " scrub-job state changed" << dendl;
	  // retry
	  register_with_osd(scrub_job, suggested);
	  break;
	}

	update_job(scrub_job, suggested);
	to_scrub.push_back(scrub_job);
	scrub_job->m_in_queues = true;
	scrub_job->m_state = qu_state_t::registered;
      }

      break;

    case qu_state_t::unregistering:
      // restore to the to_sched queue
      {
	// must be under lock, as the job might be removed from the q at any minute
	std::unique_lock l(jobs_lock, 2'000ms);
	ceph_assert(l.owns_lock());

	update_job(scrub_job, suggested);
	if (scrub_job->m_state == qu_state_t::not_registered) {
	  dout(5) << " scrub-job state changed to 'not registered'" << dendl;
	  to_scrub.push_back(scrub_job);
	}
	scrub_job->m_in_queues = true;
	scrub_job->m_state = qu_state_t::registered;
      }
      break;
  }

  dout(10) << "pg(" << scrub_job->pgid << ") sched-state changed from "
	   << qu_state_text(verif) << " to " << qu_state_text(scrub_job->m_state)
	   << dendl;
  dout(15) << "pg(" << scrub_job->pgid << ") inserted: " << scrub_job->m_sched_time
	   << dendl;
}


// look mommy - no locks!
void ScrubQueue::update_job(ceph::ref_t<ScrubJob> scrub_job,
			    const ScrubQueue::sched_params_t& suggested)
{
  // adjust the suggested scrub time according to OSD-wide status
  auto adjusted = adjust_target_time(suggested);
  scrub_job->set_time_n_deadline(adjusted);

  dout(10) << " pg(" << scrub_job->pgid << ") adjusted: " << scrub_job->m_sched_time
	   << "  " << scrub_job->registration_state() << dendl;

  scrub_job->m_penalty_timeout = utime_t(0, 0);	 // helps with debugging

  // m_updated is changed here while not holding jobs_lock. That's OK, as
  // the (atomic) flag will only be cleared by select_pg_and_scrub() after
  // scan_penalized() is called and the job was moved to the to_scrub queue.
  scrub_job->m_updated = true;
}

// used under jobs_lock
void ScrubQueue::move_failed_pgs(utime_t now_is)
{
  int punished_cnt{0};	// for log/debug only

  for (auto jb = to_scrub.begin(); jb != to_scrub.end();) {

    if ((*jb)->m_resources_failure) {

      auto sjob = *jb;

      // last time it was scheduled for a scrub, this PG failed in securing remote
      // resources. Move it to the secondary scrub queue.

      dout(15) << "moving " << sjob->pgid
	       << " state: " << ScrubQueue::qu_state_text(sjob->m_state) << dendl;

      // determine the penalty time, after which the job should be reinstated
      utime_t after = now_is;
      after += m_cct->_conf->osd_scrub_sleep * 2 + utime_t{300'000ms};

      // note: currently - not taking 'deadline' into account when determining
      // 'm_penalty_timeout'.
      sjob->m_penalty_timeout = after;
      sjob->m_resources_failure = false;
      sjob->m_updated = false;	// as otherwise will immediately be pardoned

      // place in the penalty list, and remove from the to-scrub group
      penalized.push_back(sjob);
      jb = to_scrub.erase(jb);
      punished_cnt++;

    } else {
      jb++;
    }
  }

  if (punished_cnt) {
    dout(15) << "# of jobs penalized: " << punished_cnt << dendl;
  }
}

// clang-format off
/*
 * Implementation note:
 * Clang (10 & 11) produces here efficient table-based code, comparable to using
 * a direct index into an array of strings.
 * Gcc (11, trunk) is almost as efficient.
 */
string_view ScrubQueue::attempt_res_text(Scrub::attempt_t v)
{
  switch (v) {
    case Scrub::attempt_t::scrubbing: return "scrubbing"sv;
    case Scrub::attempt_t::none_ready: return "no ready job"sv;
    case Scrub::attempt_t::local_resources: return "local resources shortage"sv;
    case Scrub::attempt_t::already_started: return "already started"sv;
    case Scrub::attempt_t::no_pg: return "pg not found"sv;
    case Scrub::attempt_t::pg_state: return "prevented by pg state"sv;
    case Scrub::attempt_t::preconditions: return "preconditions not met"sv;
  }
  // g++ (unlike CLANG), requires an extra 'return' here
  return "(unknown)"sv;
}

string_view ScrubQueue::qu_state_text(qu_state_t st)
{
  switch (st) {
    case qu_state_t::not_registered: return "not registered w/ OSD"sv;
    case qu_state_t::registered: return "registered"sv;
    case qu_state_t::unregistering: return "unregistering"sv;
  }
  // g++ (unlike CLANG), requires an extra 'return' here
  return "(unknown)"sv;
}
// clang-format on


Scrub::attempt_t ScrubQueue::select_pg_and_scrub(Scrub::ScrubPreconds& preconds)
{
  dout(10) << " reg./pen. sizes: " << to_scrub.size() << " / " << penalized.size()
	   << dendl;

  utime_t now_is = ceph_clock_now();

  preconds.time_permit = scrub_time_permit(now_is);
  preconds.load_is_low = scrub_load_below_threshold();
  preconds.only_deadlined = !preconds.time_permit || !preconds.load_is_low;

  //  create a list of candidates (copying, as otherwise creating a deadlock):
  //  - possibly restore penalized
  //  - (if we didn't handle directly) remove invalid jobs
  //  - create a copy of the to_scrub (possibly up to first not-ripe)
  //  - same for the penalized (although that usually be a waste)
  //  unlock, then try the lists

  std::unique_lock l(jobs_lock, 2'000ms);
  ceph_assert(l.owns_lock());

  // pardon all penalized jobs that have deadlined (or were updated)
  scan_penalized(restore_penalized, now_is);
  restore_penalized = false;

  // remove the 'updated' flag from all entries
  std::for_each(to_scrub.begin(), to_scrub.end(), clear_upd_flag);

  // add failed scrub attempts to the penalized list
  move_failed_pgs(now_is);

  //  collect all valid & ripe jobs from the two lists. Note that we must copy,
  //  as when we use the lists we will not be holding jobs_lock (see explanation above)

  auto to_scrub_copy = collect_ripe_jobs(to_scrub, now_is);
  auto penalized_copy = collect_ripe_jobs(penalized, now_is);
  l.unlock();

  // try the regular queue first
  auto res = select_from_group(to_scrub_copy, preconds, now_is);

  // in the sole scenario in which we've gone over all ripe jobs without success - we
  // will try the penalized
  if (res == Scrub::attempt_t::none_ready && !penalized_copy.empty()) {
    res = select_from_group(penalized_copy, preconds, now_is);
    dout(10) << "tried the penalized. Res: " << ScrubQueue::attempt_res_text(res)
	     << dendl;
    restore_penalized = true;
  }

  dout(15) << dendl;
  return res;
}

// must be called under lock
void ScrubQueue::clear_dead_jobs(ScrubQContainer& group)
{
  std::for_each(group.begin(), group.end(), [](auto& jb) {
    if (jb->m_state == qu_state_t::unregistering) {
      jb->m_in_queues = false;
      jb->m_state = qu_state_t::not_registered;
    } else if (jb->m_state == qu_state_t::not_registered) {
      jb->m_in_queues = false;
    }
  });

  group.erase(std::remove_if(group.begin(), group.end(), invalid_state), group.end());
}

// called under lock
ScrubQueue::ScrubQContainer ScrubQueue::collect_ripe_jobs(ScrubQContainer& group,
							  utime_t time_now)
{
  clear_dead_jobs(group);

  // copy ripe jobs
  ScrubQueue::ScrubQContainer ripes;
  ripes.reserve(group.size());

  std::copy_if(
    group.begin(), group.end(), std::back_inserter(ripes),
    [time_now](const auto& jobref) -> bool { return jobref->m_sched_time <= time_now; });
  std::sort(ripes.begin(), ripes.end(), time_indirect_t{});

  {
    // development only: list those that are not ripe
    for (const auto& jobref : group) {
      if (jobref->m_sched_time > time_now) {
	dout(20) << "  not ripe: " << jobref->pgid << " @ " << jobref->m_sched_time
		 << dendl;
      }
    }
  }

  return ripes;
}

// not holding jobs_lock. 'group' is a copy of the actual list.
Scrub::attempt_t ScrubQueue::select_from_group(ScrubQContainer& group,
					       const Scrub::ScrubPreconds& preconds,
					       utime_t now_is)
{
  dout(15) << "sz: " << group.size() << dendl;

  for (auto& candidate : group) {

    // we expect the first job in the list to be a good candidate (if any)

    dout(20) << "try initiating scrub for " << candidate->pgid << dendl;

    if (preconds.only_deadlined &&
	(candidate->m_deadline.is_zero() || candidate->m_deadline >= now_is)) {
      dout(15) << " not scheduling scrub for " << candidate->pgid << " due to "
	       << (preconds.time_permit ? "high load" : "time not permitting") << dendl;
      continue;
    }

    // we have a candidate to scrub. We turn to the OSD to verify that the PG
    // configuration allows the specified type of scrub, and to initiate the scrub.
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
	dout(20) << "failed (state/cond/started) " << candidate->pgid << dendl;
	break;

      case Scrub::attempt_t::no_pg:
	// The pg is no longer there
	dout(20) << "failed (no pg) " << candidate->pgid << dendl;
	break;

      case Scrub::attempt_t::local_resources:
	// failure to secure local resources. No point in trying the other
	// PGs at this time. Note that this is not the same as replica resources
	// failure!
	dout(20) << "failed (local) " << candidate->pgid << dendl;
	return Scrub::attempt_t::local_resources;

      case Scrub::attempt_t::none_ready:
	// can't happen. Just for the compiler.
	dout(5) << "failed !!! " << candidate->pgid << dendl;
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

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    // debug
    dout(20) << "min t: " << times.min_interval
	     << " osd: " << m_cct->_conf->osd_scrub_min_interval
	     << " max t: " << times.max_interval
	     << " osd: " << m_cct->_conf->osd_scrub_max_interval << dendl;

    dout(20) << "at " << sched_n_dead.scheduled_at << " ratio "
	     << m_cct->_conf->osd_scrub_interval_randomize_ratio << dendl;
  }

  if (times.is_must == ScrubQueue::must_scrub_t::not_mandatory) {

    // if not explicitly requested, postpone the scrub with a random delay

    double scrub_min_interval =
      times.min_interval > 0 ? times.min_interval : m_cct->_conf->osd_scrub_min_interval;
    double scrub_max_interval =
      times.max_interval > 0 ? times.max_interval : m_cct->_conf->osd_scrub_max_interval;

    sched_n_dead.scheduled_at += scrub_min_interval;
    double r = rand() / (double)RAND_MAX;
    sched_n_dead.scheduled_at +=
      scrub_min_interval * m_cct->_conf->osd_scrub_interval_randomize_ratio * r;

    if (scrub_max_interval <= 0) {
      sched_n_dead.deadline = utime_t{};
    } else {
      sched_n_dead.deadline += scrub_max_interval;
    }
  }

  dout(17) << "at (final) " << sched_n_dead.scheduled_at << " - " << sched_n_dead.deadline
	   << dendl;
  return sched_n_dead;
}

double ScrubQueue::scrub_sleep_time(bool must_scrub) const
{
  double regular_sleep_period = m_cct->_conf->osd_scrub_sleep;

  if (must_scrub || scrub_time_permit(ceph_clock_now())) {
    return regular_sleep_period;
  }

  // relevant if scrubbing started during allowed time, but continued into forbidden
  // hours
  double extended_sleep = m_cct->_conf->osd_scrub_extended_sleep;
  dout(20) << "w/ extended sleep (" << extended_sleep << ")" << dendl;
  return std::max(extended_sleep, regular_sleep_period);
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
    dout(20) << "loadavg per cpu " << loadavg_per_cpu << " < max "
	     << m_cct->_conf->osd_scrub_load_threshold << " = yes" << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (loadavgs[0] < daily_loadavg && loadavgs[0] < loadavgs[2]) {
    dout(20) << "loadavg " << loadavgs[0] << " < daily_loadavg " << daily_loadavg
	     << " and < 15m avg " << loadavgs[2] << " = yes" << dendl;
    return true;
  }

  dout(20) << "loadavg " << loadavgs[0] << " >= max "
	   << m_cct->_conf->osd_scrub_load_threshold << " and ( >= daily_loadavg "
	   << daily_loadavg << " or >= 15m avg " << loadavgs[2] << ") = no" << dendl;
  return false;
}


// note: called with jobs_lock held
void ScrubQueue::scan_penalized(bool forgive_all, utime_t time_now)
{
  dout(20) << time_now << (forgive_all ? " all " : " - ") << penalized.size() << dendl;

  // clear dead entries (deleted PGs, or those PGs we are no longer their primary)
  clear_dead_jobs(penalized);

  if (forgive_all) {

    std::copy(penalized.begin(), penalized.end(), std::back_inserter(to_scrub));
    penalized.clear();

  } else {

    auto forgiven_last =
      std::partition(penalized.begin(), penalized.end(), [time_now](const auto& e) {
	return (*e).m_updated || ((*e).m_penalty_timeout <= time_now);
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
    dout(20) << "should run between week day " << m_cct->_conf->osd_scrub_begin_week_day
	     << " - " << m_cct->_conf->osd_scrub_end_week_day << " now " << bdt.tm_wday
	     << " - no" << dendl;
    return false;
  }

  bool time_permit = isbetween_modulo(m_cct->_conf->osd_scrub_begin_hour,
				      m_cct->_conf->osd_scrub_end_hour, bdt.tm_hour);
  dout(20) << "should run between " << m_cct->_conf->osd_scrub_begin_hour << " - "
	   << m_cct->_conf->osd_scrub_end_hour << " now (" << bdt.tm_hour
	   << ") = " << (time_permit ? "yes" : "no") << dendl;
  return time_permit;
}

void ScrubQueue::ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << m_sched_time;
  f->dump_stream("deadline") << m_deadline;
  f->dump_bool("forced", m_sched_time == PgScrubber::scrub_must_stamp());
  f->close_section();
}

void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
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

  std::unique_lock l(jobs_lock, 2'000ms);
  ceph_assert(l.owns_lock());

  std::copy_if(to_scrub.begin(), to_scrub.end(), std::back_inserter(all_jobs),
	       registered_job);
  std::copy_if(penalized.begin(), penalized.end(), std::back_inserter(all_jobs),
	       registered_job);

  return all_jobs;
}

// ////////////////////////////////////////////////////////////////////////////////// //
// ScrubJob - scrub resource management

bool ScrubQueue::can_inc_scrubs() const
{
  // consider removing the lock here. Caller already handles delayed
  // inc_scrubs_local() failures
  std::lock_guard l{resource_lock};

  if (m_scrubs_local + m_scrubs_remote < m_cct->_conf->osd_max_scrubs) {
    return true;
  }

  dout(20) << " == false. " << m_scrubs_local << " local + " << m_scrubs_remote
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

  dout(20) << ": " << m_scrubs_local << " local + " << m_scrubs_remote
	   << " remote >= max " << m_cct->_conf->osd_max_scrubs << dendl;
  return false;
}

void ScrubQueue::dec_scrubs_local()
{
  dout(20) << ": " << m_scrubs_local << " -> " << (m_scrubs_local - 1) << " (max "
	   << m_cct->_conf->osd_max_scrubs << ", remote " << m_scrubs_remote << ")"
	   << dendl;

  std::lock_guard l{resource_lock};
  --m_scrubs_local;
  ceph_assert(m_scrubs_local >= 0);
}

bool ScrubQueue::inc_scrubs_remote()
{
  std::lock_guard l{resource_lock};

  if (m_scrubs_local + m_scrubs_remote < m_cct->_conf->osd_max_scrubs) {
    dout(20) << ": " << m_scrubs_remote << " -> " << (m_scrubs_remote + 1) << " (max "
	     << m_cct->_conf->osd_max_scrubs << ", local " << m_scrubs_local << ")"
	     << dendl;
    ++m_scrubs_remote;
    return true;
  }

  dout(20) << ": " << m_scrubs_local << " local + " << m_scrubs_remote
	   << " remote >= max " << m_cct->_conf->osd_max_scrubs << dendl;
  return false;
}

void ScrubQueue::dec_scrubs_remote()
{
  dout(20) << ": " << m_scrubs_remote << " -> " << (m_scrubs_remote - 1) << " (max "
	   << m_cct->_conf->osd_max_scrubs << ", local " << m_scrubs_local << ")"
	   << dendl;
  std::lock_guard l{resource_lock};
  --m_scrubs_remote;
  ceph_assert(m_scrubs_remote >= 0);
}

void ScrubQueue::dump_scrub_reservations(ceph::Formatter* f) const
{
  std::lock_guard l{resource_lock};
  f->dump_int("scrubs_local", m_scrubs_local);
  f->dump_int("scrubs_remote", m_scrubs_remote);
  f->dump_int("osd_max_scrubs", m_cct->_conf->osd_max_scrubs);
}
