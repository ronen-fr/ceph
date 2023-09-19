// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./osd_scrub.h"

#include "osd/OSD.h"
#include "osdc/Objecter.h"

#include "pg_scrubber.h"

using namespace ::std::chrono;
using namespace ::std::chrono_literals;
using namespace ::std::literals;


#define dout_subsys ceph_subsys_osd
#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}

OsdScrub::OsdScrub(
    CephContext* cct,
    Scrub::ScrubSchedListener& osd_svc,
    const ceph::common::ConfigProxy& config)
    : cct{cct}
    , m_osd_svc{osd_svc}
    , conf{config}
    , m_resource_bookkeeper{[this](std::string msg) { log_fwd(msg); }, conf}
    , m_queue{cct, m_osd_svc}
    , m_log_prefix{fmt::format("osd.{}: osd-scrub:", m_osd_svc.get_nodeid())}
{}

std::ostream& OsdScrub::gen_prefix(std::ostream& out, std::string_view fn) const
{
  return out << m_log_prefix << fn << ": ";
}

void OsdScrub::dump_scrubs(ceph::Formatter* f) const
{
  m_queue.dump_scrubs(f);
}

void OsdScrub::log_fwd(std::string_view text)
{
  dout(20) << text << dendl;
}

bool OsdScrub::scrub_random_backoff() const
{
  if (random_bool_with_probability(conf->osd_scrub_backoff_ratio)) {
    dout(20) << fmt::format(
		    "lost coin flip, randomly backing off (ratio: {:.3f})",
		    conf->osd_scrub_backoff_ratio)
	     << dendl;
    return true;  // backing off
  }
  return false;
}

void OsdScrub::initiate_scrub(bool is_recovery_active)
{
  if (scrub_random_backoff()) {
    // dice-roll says we should not scrub now
    return;
  }

  if (auto blocked_pgs = get_blocked_pgs_count(); blocked_pgs > 0) {
    // some PGs managed by this OSD were blocked by a locked object during
    // scrub. This means we might not have the resources needed to scrub now.
    dout(10)
	<< fmt::format(
	       "PGs are blocked while scrubbing due to locked objects ({} PGs)",
	       blocked_pgs)
	<< dendl;
  }

  // fail fast if no resources are available
  if (!m_resource_bookkeeper.can_inc_scrubs()) {
    dout(20) << "too many scrubs already running on this OSD" << dendl;
    return;
  }

  // if there is a PG that is just now trying to reserve scrub replica resources -
  // we should wait and not initiate a new scrub
  if (is_reserving_now()) {
    dout(10) << "scrub resources reservation in progress" << dendl;
    return;
  }

  Scrub::OSDRestrictions env_conditions;

  if (is_recovery_active && !conf->osd_scrub_during_recovery) {
    if (!conf->osd_repair_during_recovery) {
      dout(15) << "not scheduling scrubs due to active recovery" << dendl;
      return;
    }
    dout(10) << "will only schedule explicitly requested repair due to active "
		"recovery"
	     << dendl;
    env_conditions.allow_requested_repair_only = true;
  }

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    dout(20) << "scrub scheduling (@tick) starts" << dendl;
    auto all_jobs = list_registered_jobs();
    for (const auto& sj : all_jobs) {
      dout(20) << fmt::format("\tscrub-queue jobs: {}", *sj) << dendl;
    }
  }

  auto was_started = select_pg_and_scrub(env_conditions);
  dout(20) << fmt::format(
		  "scrub scheduling done ({})",
		  ScrubQueue::attempt_res_text(was_started))
	   << dendl;
}

// ////////////////////////////////////////////////////////////////////////// //
// scrub initiation - OSD code temporarily moved here from OSD.cc

// temporary dout() support for OSD members:
static ostream& _prefix(std::ostream* _dout, int whoami, epoch_t epoch) {
  return *_dout << "osd." << whoami << " " << epoch << " ";
}
#undef dout_prefix
#define dout_prefix _prefix(_dout, whoami, get_osdmap_epoch())

Scrub::schedule_result_t OSDService::initiate_a_scrub(spg_t pgid,
						      bool allow_requested_repair_only)
{
  dout(20) << __func__ << " trying " << pgid << dendl;

  // we have a candidate to scrub. We need some PG information to know if scrubbing is
  // allowed

  PGRef pg = osd->lookup_lock_pg(pgid);
  if (!pg) {
    // the PG was dequeued in the short timespan between creating the candidates list
    // (collect_ripe_jobs()) and here
    dout(5) << __func__ << " pg  " << pgid << " not found" << dendl;
    return Scrub::schedule_result_t::no_such_pg;
  }

  // This has already started, so go on to the next scrub job
  if (pg->is_scrub_queued_or_active()) {
    pg->unlock();
    dout(20) << __func__ << ": already in progress pgid " << pgid << dendl;
    return Scrub::schedule_result_t::already_started;
  }
  // Skip other kinds of scrubbing if only explicitly requested repairing is allowed
  if (allow_requested_repair_only && !pg->get_planned_scrub().must_repair) {
    pg->unlock();
    dout(10) << __func__ << " skip " << pgid
	     << " because repairing is not explicitly requested on it" << dendl;
    return Scrub::schedule_result_t::preconditions;
  }

  auto scrub_attempt = pg->sched_scrub();
  pg->unlock();
  return scrub_attempt;
}


void OSD::resched_all_scrubs()
{
  dout(10) << __func__ << ": start" << dendl;
  auto all_jobs = service.get_scrub_services().list_registered_jobs();
  for (auto& e : all_jobs) {

    auto& job = *e;
    dout(20) << __func__ << ": examine " << job.pgid << dendl;

    PGRef pg = _lookup_lock_pg(job.pgid);
    if (!pg)
      continue;

    dout(15) << __func__ << ": updating scrub schedule on " << job.pgid << dendl;
    pg->on_scrub_schedule_input_change();

    pg->unlock();
  }
  dout(10) << __func__ << ": done" << dendl;
}


// restoring local dout() settings (to be removed in a followup commit)
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  ceph_assert(f != nullptr);
  std::lock_guard lck(jobs_lock);

  f->open_array_section("scrubs");

  std::for_each(
      to_scrub.cbegin(), to_scrub.cend(),
      [&f](const Scrub::ScrubJobRef& j) { j->dump(f); });

  std::for_each(
      penalized.cbegin(), penalized.cend(),
      [&f](const Scrub::ScrubJobRef& j) { j->dump(f); });

  f->close_section();
}

/**
 *  a note regarding 'to_scrub_copy':
 *  'to_scrub_copy' is a sorted set of all the ripe jobs from to_copy.
 *  As we usually expect to refer to only the first job in this set, we could
 *  consider an alternative implementation:
 *  - have collect_ripe_jobs() return the copied set without sorting it;
 *  - loop, performing:
 *    - use std::min_element() to find a candidate;
 *    - try that one. If not suitable, discard from 'to_scrub_copy'
 */
Scrub::schedule_result_t ScrubQueue::select_pg_and_scrub(
  Scrub::OSDRestrictions& preconds)
{
  dout(10) << " reg./pen. sizes: " << to_scrub.size() << " / "
	   << penalized.size() << dendl;

  utime_t now_is = time_now();

  preconds.time_permit = scrub_time_permit(now_is);
  preconds.load_is_low = scrub_load_below_threshold();
  preconds.only_deadlined = !preconds.time_permit || !preconds.load_is_low;

  //  create a list of candidates (copying, as otherwise creating a deadlock):
  //  - possibly restore penalized
  //  - (if we didn't handle directly) remove invalid jobs
  //  - create a copy of the to_scrub (possibly up to first not-ripe)
  //  - same for the penalized (although that usually be a waste)
  //  unlock, then try the lists

  std::unique_lock lck{jobs_lock};

  // pardon all penalized jobs that have deadlined (or were updated)
  scan_penalized(restore_penalized, now_is);
  restore_penalized = false;

  // remove the 'updated' flag from all entries
  std::for_each(to_scrub.begin(),
		to_scrub.end(),
		[](const auto& jobref) -> void { jobref->updated = false; });

  // add failed scrub attempts to the penalized list
  move_failed_pgs(now_is);

  //  collect all valid & ripe jobs from the two lists. Note that we must copy,
  //  as when we use the lists we will not be holding jobs_lock (see
  //  explanation above)

  auto to_scrub_copy = collect_ripe_jobs(to_scrub, now_is);
  auto penalized_copy = collect_ripe_jobs(penalized, now_is);
  lck.unlock();

  // try the regular queue first
  auto res = select_from_group(to_scrub_copy, preconds, now_is);

  // in the sole scenario in which we've gone over all ripe jobs without success
  // - we will try the penalized
  if (res == Scrub::schedule_result_t::none_ready && !penalized_copy.empty()) {
    res = select_from_group(penalized_copy, preconds, now_is);
    dout(10) << "tried the penalized. Res: "
	     << ScrubQueue::attempt_res_text(res) << dendl;
    restore_penalized = true;
  }

  dout(15) << dendl;
  return res;
}


// not holding jobs_lock. 'group' is a copy of the actual list.
Scrub::schedule_result_t ScrubQueue::select_from_group(
    Scrub::ScrubQContainer& group,
    const Scrub::OSDRestrictions& preconds,
    utime_t now_is)
{
  dout(15) << "jobs #: " << group.size() << dendl;

  for (auto& candidate : group) {

    // we expect the first job in the list to be a good candidate (if any)

    dout(20) << "try initiating scrub for " << candidate->pgid << dendl;

    if (preconds.only_deadlined && (candidate->schedule.deadline.is_zero() ||
				    candidate->schedule.deadline >= now_is)) {
      dout(15) << " not scheduling scrub for " << candidate->pgid << " due to "
	       << (preconds.time_permit ? "high load" : "time not permitting")
	       << dendl;
      continue;
    }

    // we have a candidate to scrub. We turn to the OSD to verify that the PG
    // configuration allows the specified type of scrub, and to initiate the
    // scrub.
    switch (osd_service.initiate_a_scrub(
	candidate->pgid, preconds.allow_requested_repair_only)) {

      case Scrub::schedule_result_t::scrub_initiated:
	// the happy path. We are done
	dout(20) << " initiated for " << candidate->pgid << dendl;
	return Scrub::schedule_result_t::scrub_initiated;

      case Scrub::schedule_result_t::already_started:
      case Scrub::schedule_result_t::preconditions:
      case Scrub::schedule_result_t::bad_pg_state:
	// continue with the next job
	dout(20) << "failed (state/cond/started) " << candidate->pgid << dendl;
	break;

      case Scrub::schedule_result_t::no_such_pg:
	// The pg is no longer there
	dout(20) << "failed (no pg) " << candidate->pgid << dendl;
	break;

      case Scrub::schedule_result_t::no_local_resources:
	// failure to secure local resources. No point in trying the other
	// PGs at this time. Note that this is not the same as replica resources
	// failure!
	dout(20) << "failed (local) " << candidate->pgid << dendl;
	return Scrub::schedule_result_t::no_local_resources;

      case Scrub::schedule_result_t::none_ready:
	// can't happen. Just for the compiler.
	dout(5) << "failed !!! " << candidate->pgid << dendl;
	return Scrub::schedule_result_t::none_ready;
    }
  }

  dout(20) << " returning 'none ready'" << dendl;
  return Scrub::schedule_result_t::none_ready;
}


// ////////////////////////////////////////////////////////////////////////// //
// CPU load tracking and related


///\todo replace with Knuth's algo (to reduce the numerical error)
std::optional<double> ScrubQueue::update_load_average()
{
  int hb_interval = conf()->osd_heartbeat_interval;
  int n_samples = std::chrono::duration_cast<seconds>(24h).count();
  if (hb_interval > 1) {
    n_samples = std::max(n_samples / hb_interval, 1);
  }

  // get CPU load avg
  double loadavg;
  if (getloadavg(&loadavg, 1) == 1) {
    daily_loadavg = (daily_loadavg * (n_samples - 1) + loadavg) / n_samples;
    return 100 * loadavg;
  }

  return std::nullopt;
}

bool ScrubQueue::scrub_load_below_threshold() const
{
  double loadavgs[3];
  if (getloadavg(loadavgs, 3) != 3) {
    dout(10) << fmt::format("{}: couldn't read loadavgs", __func__) << dendl;
    return false;
  }

  // allow scrub if below configured threshold
  long cpus = sysconf(_SC_NPROCESSORS_ONLN);
  double loadavg_per_cpu = cpus > 0 ? loadavgs[0] / cpus : loadavgs[0];
  if (loadavg_per_cpu < conf()->osd_scrub_load_threshold) {
    dout(20) << fmt::format(
		    "loadavg per cpu {:.3f} < max {:.3f} = yes",
		    loadavg_per_cpu, conf()->osd_scrub_load_threshold)
	     << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (loadavgs[0] < daily_loadavg && loadavgs[0] < loadavgs[2]) {
    dout(20) << fmt::format(
		    "loadavg {:.3f} < daily_loadavg {:.3f} and < 15m avg "
		    "{:.3f} = yes",
		    loadavgs[0], daily_loadavg, loadavgs[2])
	     << dendl;
    return true;
  }

  dout(10) << fmt::format(
		  "loadavg {:.3f} >= max {:.3f} and ( >= daily_loadavg {:.3f} "
		  "or >= 15m avg {:.3f} ) = no",
		  loadavgs[0], conf()->osd_scrub_load_threshold, daily_loadavg,
		  loadavgs[2])
	   << dendl;
  return false;
}


std::optional<double> OsdScrub::update_load_average()
{
  return m_queue.update_load_average();
}



// ////////////////////////////////////////////////////////////////////////// //

// checks for half-closed ranges. Modify the (p<till)to '<=' to check for
// closed.
static inline bool isbetween_modulo(int64_t from, int64_t till, int p)
{
  // the 1st condition is because we have defined from==till as "always true"
  return (till == from) || ((till >= from) ^ (p >= from) ^ (p < till));
}

bool OsdScrub::scrub_time_permit(utime_t now) const
{
  return m_queue.scrub_time_permit(now);
}

bool ScrubQueue::scrub_time_permit(utime_t now) const
{
  tm bdt;
  time_t tt = now.sec();
  localtime_r(&tt, &bdt);

  bool day_permit = isbetween_modulo(conf()->osd_scrub_begin_week_day,
				     conf()->osd_scrub_end_week_day,
				     bdt.tm_wday);
  if (!day_permit) {
    dout(20) << "should run between week day "
	     << conf()->osd_scrub_begin_week_day << " - "
	     << conf()->osd_scrub_end_week_day << " now " << bdt.tm_wday
	     << " - no" << dendl;
    return false;
  }

  bool time_permit = isbetween_modulo(conf()->osd_scrub_begin_hour,
				      conf()->osd_scrub_end_hour,
				      bdt.tm_hour);
  dout(20) << "should run between " << conf()->osd_scrub_begin_hour << " - "
	   << conf()->osd_scrub_end_hour << " now (" << bdt.tm_hour
	   << ") = " << (time_permit ? "yes" : "no") << dendl;
  return time_permit;
}

std::chrono::milliseconds OsdScrub::scrub_sleep_time(bool must_scrub) const
{
  return m_queue.scrub_sleep_time(must_scrub);
}

std::chrono::milliseconds ScrubQueue::scrub_sleep_time(bool must_scrub) const
{
  std::chrono::milliseconds regular_sleep_period{
    uint64_t(std::max(0.0, conf()->osd_scrub_sleep) * 1000)};

  if (must_scrub || scrub_time_permit(time_now())) {
    return regular_sleep_period;
  }

  // relevant if scrubbing started during allowed time, but continued into
  // forbidden hours
  std::chrono::milliseconds extended_sleep{
    uint64_t(std::max(0.0, conf()->osd_scrub_extended_sleep) * 1000)};
  dout(20) << "w/ extended sleep (" << extended_sleep << ")" << dendl;

  return std::max(extended_sleep, regular_sleep_period);
}

// ////////////////////////////////////////////////////////////////////////// //
// forwarders to the queue

Scrub::sched_params_t OsdScrub::determine_scrub_time(
    const requested_scrub_t& request_flags,
    const pg_info_t& pg_info,
    const pool_opts_t& pool_conf) const
{
  return m_queue.determine_scrub_time(request_flags, pg_info, pool_conf);
}

void OsdScrub::update_job(
    Scrub::ScrubJobRef sjob,
    const Scrub::sched_params_t& suggested)
{
  m_queue.update_job(sjob, suggested);
}

void OsdScrub::register_with_osd(
    Scrub::ScrubJobRef sjob,
    const Scrub::sched_params_t& suggested)
{
  m_queue.register_with_osd(sjob, suggested);
}

void OsdScrub::remove_from_osd_queue(Scrub::ScrubJobRef sjob)
{
  m_queue.remove_from_osd_queue(sjob);
}

bool OsdScrub::inc_scrubs_local()
{
  return m_resource_bookkeeper.inc_scrubs_local();
}

void OsdScrub::dec_scrubs_local()
{
  m_resource_bookkeeper.dec_scrubs_local();
}

bool OsdScrub::inc_scrubs_remote()
{
  return m_resource_bookkeeper.inc_scrubs_remote();
}

void OsdScrub::dec_scrubs_remote()
{
  m_resource_bookkeeper.dec_scrubs_remote();
}

void OsdScrub::mark_pg_scrub_blocked(spg_t blocked_pg)
{
  m_queue.mark_pg_scrub_blocked(blocked_pg);
}

void OsdScrub::clear_pg_scrub_blocked(spg_t blocked_pg)
{
  m_queue.clear_pg_scrub_blocked(blocked_pg);
}

int OsdScrub::get_blocked_pgs_count() const
{
  return m_queue.get_blocked_pgs_count();
}

bool OsdScrub::set_reserving_now()
{
  return m_queue.set_reserving_now();
}

void OsdScrub::clear_reserving_now()
{
  m_queue.clear_reserving_now();
}

bool OsdScrub::is_reserving_now() const
{
  return m_queue.is_reserving_now();
}

Scrub::ScrubQContainer OsdScrub::list_registered_jobs() const
{
  return m_queue.list_registered_jobs();
}

Scrub::schedule_result_t OsdScrub::select_pg_and_scrub(
    Scrub::OSDRestrictions& preconds)
{
  return m_queue.select_pg_and_scrub(preconds);
}
