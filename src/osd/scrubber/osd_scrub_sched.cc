// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./osd_scrub_sched.h"

#include "osd/OSD.h"

#include "pg_scrubber.h"

using namespace ::std::literals;



// ////////////////////////////////////////////////////////////////////////// //
// SchedTarget

/*
  API to modify the scrub job should handle the following cases:
 
  - a shallow/deep scrub just terminated;
  - scrub attempt failed due to replicas;
  - scrub attempt failed due to environment;
  - operator requested a shallow/deep scrub;
  - the penalized are forgiven;
*/
using SchedTarget = Scrub::SchedTarget;
using urgency_t = Scrub::urgency_t;
using delay_cause_t = Scrub::delay_cause_t;

std::partial_ordering SchedTarget::operator<=>(const SchedTarget& r) const
{
  // note the reverse order for Urgency: higher is better
  if (auto cmp = r.urgency <=> urgency; cmp != 0) {
    return cmp;
  }
  if (auto cmp = r.auto_repair <=> auto_repair; cmp != 0) {
    return cmp;
  }
  // if both have deadline - use it. The earlier is better.
  if (deadline && r.deadline) {
    if (auto cmp = *deadline <=> *r.deadline; cmp != 0) {
      return cmp;
    }
  }
  return not_before <=> r.not_before;
}

SchedTarget::SchedTarget(
  ScrubJob& owning_job,
  scrub_level_t base_type,
  std::string dbg)
    : pgid{owning_job.pgid}
    , cct{owning_job.cct}
    , base_target_level{base_type}
    , dbg_val{dbg}
{
  ceph_assert(cct);
}

bool SchedTarget::check_and_redraw_upgrade()
{
  bool current_coin = upgradeable;
  // and redraw for the next time:
  upgradeable =
    (rand() % 100) < cct->_conf->osd_deep_scrub_randomize_ratio * 100;
  return current_coin;
}

void SchedTarget::set_oper_deep_target(scrub_type_t rpr)
{
  ceph_assert(base_target_level == scrub_level_t::deep);
  ceph_assert(!scrubbing);
  urgency = urgency_t::operator_requested;
  target = ceph_clock_now(); // consider merging?
  not_before = ceph_clock_now();
  auto_repair = (rpr == scrub_type_t::do_repair);
  last_issue = delay_cause_t::none;
}

void SchedTarget::replica_refusal()
{
  /*
	If it's a low priority job:
	- we will mark as penalized, and
	- (mark the time when we will forgive the job. - that's in the
          'SchedTargets')

       If high priority ('must', overdue or operator-initiated):
	- keep the existing priority, and
	- modify NB by a small amount, to make sure the job is retried soon.
  */
  switch (urgency) {
    case urgency_t::must:		 // fall-through
    case urgency_t::operator_requested:	 // fall-through
    case urgency_t::overdue:
      // high priority job. We should not delay it by much.
      push_nb_out(/* RRR conf */ 6s);
      break;

    case urgency_t::periodic_regular:
      urgency = urgency_t::penalized;
      break;

    default:
      ceph_abort();
  }
}

void SchedTarget::push_nb_out(std::chrono::seconds delay)
{
  not_before = std::max(ceph_clock_now(), not_before) + utime_t{delay};
}

void SchedTarget::pg_state_failure()
{
  // if not in a state to be scrubbed (active & clean) - we won't retry it
  // for some time
  push_nb_out(/* RRR conf */ 10s);
  last_issue = delay_cause_t::pg_state;
}

void SchedTarget::wrong_time()
{
  // wrong time / day / load
  push_nb_out(/* RRR conf */ 10s);
  last_issue = delay_cause_t::time;
}

void SchedTarget::no_local_resources()
{
  // too many scrubs on our own OSD
  push_nb_out(/* RRR conf */ 3s);
  last_issue = delay_cause_t::local_resources;
}

void SchedTarget::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << not_before;
  f->dump_stream("deadline") << deadline;
  f->dump_bool("forced",
	       target == PgScrubber::scrub_must_stamp());
  // RRR todo - add the 'urgency' field
  // RRR todo - add the 'repair' field
  f->close_section();
}



// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << "osd." << whoami << "  "

using qu_state_t = Scrub::qu_state_t;
using ScrubJob = Scrub::ScrubJob;

ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    //: RefCountedObject{cct}
    : pgid{pg}
    , whoami{node_id}
    , cct{cct}
    , shallow_target{*this, scrub_level_t::shallow, "cs"}
    , deep_target{*this, scrub_level_t::deep, "cd"}
    , next_shallow{*this, scrub_level_t::shallow, "ns"}
    , next_deep{*this, scrub_level_t::deep, "nd"}
{
}

// debug usage only
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
//   out << sjob.pgid << ",  " << sjob.schedule.scheduled_at
//       << " dead: " << sjob.schedule.deadline << " - "
//       << sjob.registration_state() << " / failure: " << sjob.resources_failure
//       << " / pen. t.o.: " << sjob.penalty_timeout
//       << " / queue state: " << qu_state_text(sjob.state);

  return out << fmt::format("{}", sjob);
}

void ScrubJob::update_schedule(
  const Scrub::scrub_schedule_t& adjusted)
{
//   schedule = adjusted;
//   penalty_timeout = utime_t(0, 0);  // helps with debugging
// 
//   // 'updated' is changed here while not holding jobs_lock. That's OK, as
//   // the (atomic) flag will only be cleared by select_pg_and_scrub() after
//   // scan_penalized() is called and the job was moved to the to_scrub queue.
  updated = true;

//   dout(10) << " pg[" << pgid << "] adjusted: " << schedule.scheduled_at << "  "
// 	   << registration_state() << dendl;
}

std::string ScrubJob::scheduling_state(utime_t now_is,
						   bool is_deep_expected) const
{
  // if not in the OSD scheduling queues, not a candidate for scrubbing
  if (state != qu_state_t::registered) {
    return "no scrub is scheduled";
  }

  // if the time has passed, we are surely in the queue
  // (note that for now we do not tell client if 'penalized')
  if (closest_target->is_ripe(now_is)) {
    // we are never sure that the next scrub will indeed be shallow:
    return fmt::format("queued for {}scrub", (is_deep_expected ? "deep " : ""));
  }

  return fmt::format("{}scrub scheduled @ {}",
		     (is_deep_expected ? "deep " : ""), // replace with upgraded_to_deep
		     closest_target->not_before);
}

bool ScrubJob::verify_targets_disabled() const
{
  return shallow_target.urgency <= urgency_t::off &&
                deep_target.urgency <= urgency_t::off &&
                next_shallow.urgency <= urgency_t::off &&
                next_deep.urgency <= urgency_t::off;
}

// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue

#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix                                                            \
  *_dout << "osd." << osd_service.get_nodeid() << " scrub-queue::" << __func__ \
	 << " "

using TargetRef = Scrub::TargetRef;

ScrubQueue::ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds)
    : cct{cct}
    , osd_service{osds}
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
  int hb_interval = conf()->osd_heartbeat_interval;
  int n_samples = 60 * 24 * 24;
  if (hb_interval > 1) {
    n_samples /= hb_interval;
    if (n_samples < 1)
      n_samples = 1;
  }

  // get CPU load avg
  double loadavg;
  if (getloadavg(&loadavg, 1) == 1) {
    daily_loadavg = (daily_loadavg * (n_samples - 1) + loadavg) / n_samples;
    dout(17) << "heartbeat: daily_loadavg " << daily_loadavg << dendl;
    return 100 * loadavg;
  }

  return std::nullopt;
}

std::string_view ScrubJob::state_desc() const
{
  return ScrubQueue::qu_state_text(state.load(std::memory_order_relaxed));
}

void ScrubJob::determine_closest()
{
  closest_target = (shallow_target <  deep_target) ? &shallow_target
                                                     : &deep_target;
}

SchedTarget& SchedTarget::operator=(const SchedTarget& r)
{
  //ceph_assert(parent_job.pgid == r.parent_job.pgid);
  ceph_assert(base_target_level == r.base_target_level);
  ceph_assert(pgid == r.pgid);
  // the above also guarantees that we have the same cct
  urgency = r.urgency;
  not_before = r.not_before;
  deadline = r.deadline;
  target = r.target;
  scrubbing = r.scrubbing;  // to reconsider
  upgraded_to_deep = r.upgraded_to_deep;
  last_issue = r.last_issue;
  auto_repair = r.auto_repair;
  marked_for_dequeue = r.marked_for_dequeue;
  dbg_val = r.dbg_val;
  return *this;
}


void ScrubJob::disable_scheduling() // RRR ever called directly?
{
  shallow_target.urgency = urgency_t::off;
  deep_target.urgency = urgency_t::off;
  next_shallow.urgency = urgency_t::off;
  next_deep.urgency = urgency_t::off;
  // RRR consider the callers - should we reset the 'req'?
}

void ScrubJob::mark_for_dequeue()
{
  disable_scheduling();
  shallow_target.marked_for_dequeue = true;
  deep_target.marked_for_dequeue = true;
  next_shallow.marked_for_dequeue = true;
  next_deep.marked_for_dequeue = true;
}

// void ScrubJob::clear_marked_for_dequeue()
// {
//   shallow_target.marked_for_dequeue = false;
//   deep_target.marked_for_dequeue = false;
//   next_shallow.marked_for_dequeue = false;
//   next_deep.marked_for_dequeue = false;
// }

// return either one of the current set of targets, or - if
// that target is the one being scrubbed - the 'next' one
TargetRef ScrubJob::get_modif_trgt(scrub_level_t lvl)
{
  auto trgt = get_current_trgt(lvl);
  if (trgt->scrubbing) {
    return (lvl == scrub_level_t::deep) ? &next_deep : &next_shallow;
  }
  return trgt;
}

TargetRef ScrubJob::get_current_trgt(scrub_level_t lvl)
{
  return (lvl == scrub_level_t::deep) ? &deep_target : &shallow_target;
}


/*
   - create a "template" target, to be compared against "shallow".
   - if later we see that a deep is required - compared against "deep", too.
   - returns either the original target, or the new one (if higher order)

*/

utime_t add_double(utime_t t, double d)
{
  return utime_t{t.sec() + static_cast<int>(d), t.nsec()};
}

// RRR must review the logic here, now that some flags were removed
// Also - if only used after register_with_osd() - what flags can be set?
void ScrubJob::initial_shallow_target(
  const requested_scrub_t& request_flags,
  const pg_info_t& pg_info,
  const sched_conf_t& config,
  utime_t time_now)
{
  // fix scrub-job dout!
#ifdef NOTYET
  dout(20) << fmt::format("{}: pg: [{}] flags: {} conf: {}",
                          __func__, pgid, request_flags, config) << dendl;
#endif
  auto targ = get_modif_trgt(scrub_level_t::shallow);
  if (request_flags.must_scrub || request_flags.need_auto) {
    targ->urgency = urgency_t::must;
    auto base =
      pg_info.stats.stats_invalid ? time_now : pg_info.history.last_scrub_stamp;
    targ->target = base;
    targ->not_before = time_now;
    if (config.max_shallow && *config.max_shallow > 0.1) {
      targ->deadline = add_double(base, *config.max_shallow);
    } 
  } else if (pg_info.stats.stats_invalid && cct->_conf->osd_scrub_invalid_stats) {
    targ->urgency = urgency_t::must;
    targ->target = time_now;
    targ->not_before = time_now;
    if (config.max_shallow && *config.max_shallow > 0.1) {
      targ->deadline = add_double(time_now, *config.max_shallow);
    } 
  } else {
    targ->urgency = urgency_t::periodic_regular;
    auto base = pg_info.history.last_scrub_stamp;
    targ->target = add_double(base, config.shallow_interval);
    targ->not_before = shallow_target.target;
    // if in the past - do not delay. Otherwise - add a random delay
    if (time_now > shallow_target.target) {
      double r = rand() / (double)RAND_MAX;
      targ->not_before +=
	config.shallow_interval * config.interval_randomize_ratio * r;
    }
  }
  targ->last_issue = delay_cause_t::none;

  // prepare the 'upgrade lottery' for some possible future use.
//   targ->upgraded_to_deep =
//       (rand() % 100) < cct->_conf->osd_deep_scrub_randomize_ratio * 100;
}

/*
 * A note re the randomization:
 * for deep scrubs, we will only "randomize backwards", i.e we will not
 * delay till after the deep-interval.
 */
void ScrubJob::initial_deep_target(
  const requested_scrub_t& request_flags,
  const pg_info_t& pg_info,
  const sched_conf_t& config,
  utime_t time_now)
{
  auto targ = get_modif_trgt(scrub_level_t::deep);
  auto base = pg_info.stats.stats_invalid
		? time_now
		: pg_info.history.last_deep_scrub_stamp;

  if (request_flags.must_deep_scrub || request_flags.need_auto) { // RRR need_auto will not be needed
    targ->urgency = urgency_t::must;
    targ->target = base;
    targ->not_before = base;

  } else {
    targ->urgency = urgency_t::periodic_regular;
    double r = rand() / (double)RAND_MAX;
    targ->target = add_double(
      base, (1.0 - r * config.interval_randomize_ratio) * config.deep_interval);
    targ->not_before = targ->target;
  }

  targ->deadline = add_double(base, config.max_deep);
  targ->auto_repair = false;
  targ->last_issue = delay_cause_t::none;
  // 'upgraded_to_deep' is always asserted for deep targets, enabling
  // us to query for the level of the 'next' target regardless of its
  // base level.
  targ->upgraded_to_deep = true;
}

/**
 * mark for a deep-scrub after the current scrub ended with errors.
 */
void ScrubJob::mark_for_rescrubbing(requested_scrub_t& request_flags)
{
  auto targ = get_modif_trgt(scrub_level_t::deep);
  targ->auto_repair = true;
  targ->urgency = urgency_t::must; // no need, I think, to use max(...)
  targ->target = ceph_clock_now(); // replace with time_now()
  targ->not_before = targ->target;
  determine_closest();

  // fix scrub-job dout!
#ifdef NOTYET
  dout(10) << fmt::format("{}: need deep+a.r. after scrub errors. Target set to {}",
                          __func__, deep_target->target)
           << dendl;
#endif
}


// Scrub::TargetRef ScrubJob::create_oper_deep_target(scrub_type_t rpr) const
// {
//   TargetRef suggested =
//     ceph::make_ref<Scrub::SchedTarget>(this->get(), scrub_level_t::deep);
//   suggested->urgency = urgency_t::operator_requested;
//   suggested->target = ceph_clock_now();
//   suggested->not_before = ceph_clock_now();
//   suggested->auto_repair = (rpr == scrub_type_t::do_repair);
//   return suggested;
// }

void ScrubJob::merge_deep_target(TargetRef&& candidate)
{
#if 0
  if (candidate->urgency > deep_target->urgency) {
    deep_target->urgency = candidate->urgency;
  }
  deep_target->target = std::min(deep_target->target, candidate->target);
  deep_target->not_before =
    std::min(deep_target->not_before, candidate->not_before);
  // RRR +deep_target->deadline = std::min(deep_target->deadline,
  // candidate->deadline);
  deep_target->auto_repair = deep_target->auto_repair || candidate->auto_repair;
#endif
}


#if 0
void ScrubJob::at_scrub_completion(
  const pg_info_t& pg_info,
  const sched_conf_t& aconf,
  const requested_scrub_t& request_flags)
// RRR must remove the request-flags
{
  // we do not know whether we are scrubbing this PG now. After all,
  // queued_or_active was just cleared.
  // but we do not care what was the specific scrub type that just
  // terminated.

  // shallow targets

  // If we have a 'next' shallow target - it should have higher priority than
  // 'periodic''. We can just use it. Otherwise - we'll create a
  // regular one.

  auto l = scrub_level_t::shallow;
  bool just_done = get_current_trgt(l)->scrubbing;

  auto ns = get_modif_trgt(l);

  if (!just_done) {
    // we do not expect to have a 'next' target (but if we do - is it an error?)
    ceph_assert(next_shallow.urgency == urgency_t::off);
    ns->update_target(pg_info, aconf, request_flags);
  } else {
    if (next_shallow.urgency != urgency_t::off) {
      // we have a 'next' target. Merge it with the current one
      // merge_shallow_target(std::move(next_shallow));
      ceph_assert(next_shallow.urgency >= urgency_t::operator_requested);
      // nothing to do
    } else {
      // no 'next' target. Update the current one
      ns->update_target(pg_info, aconf, request_flags);
    }
  }

  // RRR what can we combine?
  // deep targets
  l = scrub_level_t::deep;
  just_done = get_current_trgt(l)->scrubbing;

  auto nd = get_modif_trgt(l);

  if (!just_done) {
    // we do not expect to have a 'next' target (but if we do - is it an error?)
    ceph_assert(next_deep.urgency == urgency_t::off);
    nd->update_target(pg_info, aconf, request_flags);
  } else {
    if (next_deep.urgency != urgency_t::off) {
      // we have a 'next' target. Merge it with the current one
      // merge_shallow_target(std::move(next_shallow));
      ceph_assert(next_deep.urgency >= urgency_t::operator_requested);
      // nothing to do
    } else {
      // no 'next' target. Update the current one
      nd->update_target(pg_info, aconf, request_flags);
    }
  }
}

#endif



/*
  - called after the last-stamps were updated;
  - not 'active' or 'q/a' anymore!
  - note that we may already have 'next' targets, which should be
    merged (they would probably (check) have higher urgency)

 later on:
  - we may be called from on_operator_scrub() - with a newly faked
    timestamps. Thus - we might be scrubbing.
  - we will not try to modify high priority targets, unless to update
    to a nearer target time if the updated parameters reach that
    result.
  - but should we, in that case, update 'nb', or just 'target'?
    only update NB if in the future and there was no failure recorded
    in 'last_issue' (which also means that we have to make sure last_issue
    is always cleared when needed).
*/
void ScrubJob::at_scrub_completion(
  const pg_info_t& pg_info,
  const sched_conf_t& aconf,
  const requested_scrub_t& request_flags)
// RRR must remove the request-flags
{
  auto now_is = ceph_clock_now();

  // if, by chance, the 'd' target is determined to be very shortly
  // after the 's' target - we'll "merge" them
  //utime_t deep_time{eternity};

  // the deep target
  auto d_targ = get_modif_trgt(scrub_level_t::deep);
  if (d_targ->urgency > urgency_t::overdue) {
    // We are not allowed to fully reschedule this target.
    // But - should we boost it?
    // \todo not implemented for now
  } else {
    // periodic scrub
    d_targ->update_as_deep(now_is, pg_info, aconf, request_flags);
  }

  // the shallow target
  auto s_targ = get_modif_trgt(scrub_level_t::shallow);
  if (s_targ->urgency > urgency_t::overdue) {
    // We are not allowed to fully reschedule this target.
    // But - should we boost it?
    // \todo not implemented for now
  } else {
    // periodic scrub
    s_targ->update_as_shallow(now_is, pg_info, aconf, request_flags);
  }

  if (d_targ->target > s_targ->target && d_targ->target < (s_targ->target + utime_t(10s))) {
    // better have a deep scrub
    s_targ->upgraded_to_deep = true;
  }

  determine_closest();
}


// the version that was supposed to run before clearing the 'active' flag
// void ScrubJob::at_scrub_completion(
//   const pg_info_t& pg_info,
//   const sched_conf_t& aconf,
//   const requested_scrub_t& request_flags)
// // RRR must remove the request-flags
// {
//   // shallow targets
// 
//   // we have just completed a successful shallow-scrub.
//   // If we have a 'next' shallow target - it should have higher priority than
//   // the one that just completed. We can just use it. Otherwise - we'll create a
//   // regular one in the 'next' slot. It will be moved over the 'current' slot
//   // when 'active' is turned off.
// 
//   auto l = scrub_level_t::shallow;
//   bool just_done = get_current_trgt(l)->scrubbing;
// 
//   auto mod_s = get_modif_trgt(l);
// 
//   if (!just_done) {
//     // we do not expect to have a 'next' target (but if we do - is it an error?)
//     ceph_assert(next_shallow.urgency == urgency_t::off);
//     mod_s->update_target(pg_info, aconf, request_flags);
//   } else {
//     if (next_shallow.urgency != urgency_t::off) {
//       // we have a 'next' target. Merge it with the current one
//       // merge_shallow_target(std::move(next_shallow));
//       ceph_assert(next_shallow.urgency >= urgency_t::operator_requested);
//       // nothing to do
//     } else {
//       // no 'next' target. Update the current one
//       mod_s->update_target(pg_info, aconf, request_flags);
//     }
//   }
// 
//   // RRR what code can we combine?
//   // deep targets
//   l = scrub_level_t::deep;
//   just_done = get_current_trgt(l)->scrubbing;
// 
//   auto mod_d = get_modif_trgt(l);
// 
//   if (!just_done) {
//     // we do not expect to have a 'next' target (but if we do - is it an error?)
//     ceph_assert(next_deep.urgency == urgency_t::off);
//     mod_d->update_target(pg_info, aconf, request_flags);
//   } else {
//     if (next_deep.urgency != urgency_t::off) {
//       // we have a 'next' target. Use it.
//       ceph_assert(next_deep.urgency >= urgency_t::operator_requested);
//       // nothing to do
//     } else {
//       // no 'next' target. Update the current one
//       mod_d->update_target(pg_info, aconf, request_flags);
//     }
//   }
// }

// an aux used by SchedTarget::update_target()
// RRR make sure we are not un-penalizing failed PGs
void SchedTarget::update_as_shallow(
  utime_t now_is,
  const pg_info_t& pg_info,
  const Scrub::sched_conf_t& config,
  const requested_scrub_t& request_flags)
{
  ceph_assert(base_target_level == scrub_level_t::shallow);

  // won't be called for high-urgency scrubs
  ceph_assert(urgency <= urgency_t::overdue);

  if (request_flags.must_scrub || request_flags.need_auto) {
    // should be handled before this call!
    urgency = urgency_t::must;
    auto base =
      pg_info.stats.stats_invalid ? now_is : pg_info.history.last_scrub_stamp;
    target = base;
    not_before = now_is;
    if (config.max_shallow && *config.max_shallow > 0.1) {
      deadline = add_double(base, *config.max_shallow);
    }
  } else if (pg_info.stats.stats_invalid && config.mandatory_on_invalid) {
    urgency = urgency_t::must;
    target = now_is;
    not_before = now_is;
    if (config.max_shallow && *config.max_shallow > 0.1) {
      deadline = add_double(now_is, *config.max_shallow);
    }
  } else {
    urgency = urgency_t::periodic_regular;
    auto base = std::min(pg_info.history.last_scrub_stamp, now_is);
    target = add_double(base, config.shallow_interval);
    not_before = target;
    // if in the past - do not delay. Otherwise - add a random delay
    if (now_is < target) {
      double r = rand() / (double)RAND_MAX;
      not_before +=
	config.shallow_interval * config.interval_randomize_ratio * r;
    }
  }
  // prepare the 'upgrade lottery' for some possible future use.
  // RRR 
  std::ignore = check_and_redraw_upgrade();
}

// an aux used by SchedTarget::update_target()
void SchedTarget::update_as_deep(
  utime_t now_is,
  const pg_info_t& pg_info,
  const Scrub::sched_conf_t& config,
  const requested_scrub_t& request_flags)
{
  ceph_assert(base_target_level == scrub_level_t::deep);
  // won't be called for high-urgency scrubs
  ceph_assert(urgency <= urgency_t::overdue);
  auto base = pg_info.stats.stats_invalid
		? now_is
		: pg_info.history.last_deep_scrub_stamp;

  // RRR need_auto will not be needed
  if (request_flags.must_deep_scrub || request_flags.need_auto) {
    urgency = urgency_t::must;
    target = base;
    not_before = base;

  } else {
    urgency = urgency_t::periodic_regular;
    double r = rand() / (double)RAND_MAX;
    target = add_double(
      base, (1.0 - r * config.interval_randomize_ratio) * config.deep_interval);
    not_before = target;
  }

  deadline = add_double(base, config.max_deep);
  auto_repair = false;
  // so that we can refer to 'upgraded..' for both s & d:
  upgraded_to_deep = true;
  //return target;
}

void SchedTarget::update_target(
  const pg_info_t& pg_info,
  const Scrub::sched_conf_t& config,
  const requested_scrub_t& request_flags)
{
  auto now_is = ceph_clock_now();

  if (base_target_level == scrub_level_t::shallow) {
    // the equivalent of initial_shallow_target()
    update_as_shallow(now_is, pg_info, config, request_flags);

  } else {
    // updating the deep-scrub target
    update_as_deep(now_is, pg_info, config, request_flags);
  }
}

// RRR consider using '<overdue' instead of '<='
static bool to_change_on_conf(urgency_t u)
{
  return (u > urgency_t::off) && (u <= urgency_t::overdue);
}

/*
Following a change in the 'scrub period' parameters -
recomputing the targets:
- won't affect 'must' targets;
- maybe: won't *delay* targets that were already tried and failed (have a failure reason)
- should it affect ripe jobs?


*/
// holding PG lock. Either we guarantee that we never touch the scrub-job
// without it, or we implement a local lock for the job/queue
bool ScrubJob::on_periods_change(const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    const requested_scrub_t& request_flags)
{
  // is_primary() was verified by the caller

  // we are not interested in currently running jobs. Those will either
  // have their targets updated based on up-to-date stamps and conf when done,
  // or already have a 'next' target with a higher urgency

  bool something_changed{false};

  // the job's shallow target

  auto trgt = get_current_trgt(scrub_level_t::shallow);
  if (!trgt->scrubbing && to_change_on_conf(trgt->urgency)) {
    if (trgt->urgency == urgency_t:: penalized) {
      trgt->urgency = urgency_t::periodic_regular;
    }
    trgt->update_target(info, aconf, request_flags);
    something_changed = true;
  }

  // the job's shallow target // RRR combine the code with the above

  trgt = get_current_trgt(scrub_level_t::deep);
  if (!trgt->scrubbing && to_change_on_conf(trgt->urgency)) {
    if (trgt->urgency == urgency_t:: penalized) {
      trgt->urgency = urgency_t::periodic_regular;
    }
    trgt->update_target(info, aconf, request_flags);
    something_changed = true;
  }

  return something_changed;
}

void ScrubJob::un_penalize(utime_t now_is)
{
  auto trgt = get_modif_trgt(scrub_level_t::shallow);
  if (trgt->urgency == urgency_t::penalized) {
    // restored to either 'overdue' or 'periodic_regular'
    if (trgt->over_deadline(now_is)) {
      trgt->urgency = urgency_t::overdue;
    } else {
      trgt->urgency = urgency_t::periodic_regular;
    }
  }
  trgt = get_modif_trgt(scrub_level_t::deep);
  if (trgt->urgency == urgency_t::penalized) {
    if (trgt->over_deadline(now_is)) {
      trgt->urgency = urgency_t::overdue;
    } else {
      trgt->urgency = urgency_t::periodic_regular;
    }
  }
}

void ScrubJob::activate_next_targets()
{
  // We are at the last step of completing a scrub, and
  // queued_or_active is still set.
  // But there is no actual scrubbing going on, so we can manipulate
  // the targets with no race risk.

  shallow_target.urgency = urgency_t::off;
  if (next_shallow.urgency != urgency_t::off) {
    // we have a 'next' target. Use it.
    shallow_target = next_shallow;
    next_shallow.urgency = urgency_t::off;
  }
  deep_target.urgency = urgency_t::off;
  if (next_deep.urgency != urgency_t::off) {
    // we have a 'next' target. Use it.
    deep_target = next_deep;
    next_deep.urgency = urgency_t::off;
  }
}

// /*
//  * A note re the randomization:
//  * for deep scrubs, we will only "randomize backwards", i.e we will not
//  * delay till after the deep-interval.
//  */
// SchedTarget ScrubQueue::initial_deep_target(
//   const requested_scrub_t& request_flags,
//   const pg_info_t& pg_info,
//   const sched_conf_t& config,
//   utime_t time_now) const
// {
//   auto base = pg_info.stats.stats_invalid
// 		? time_now
// 		: pg_info.history.last_deep_scrub_stamp;
// 
//   SchedTarget t{};
//   if (request_flags.must_deep_scrub || request_flags.need_auto) {
//     t.urgency = urgency_t::must;
//     t.target = base;
//     t.not_before = base;
//   } else {
//     t.urgency = urgency_t::periodic_regular;
//     double r = rand() / (double)RAND_MAX;
//     t.target = add_double(base, (1.0 - r * config.interval_randomize_ratio) * config.deep_interval);
//     t.not_before = t.target;
//   }
//   t.deadline = add_double(base, config.max_deep);
//   return t;
// }

Scrub::sched_conf_t ScrubQueue::populate_config_params(
  const pool_opts_t& pool_conf)
{
  Scrub::sched_conf_t configs;

  // deep-scrub optimal interval
  configs.deep_interval =
    pool_conf.value_or(pool_opts_t::DEEP_SCRUB_INTERVAL, 0.0);
  if (configs.deep_interval <= 0.0) {
    configs.deep_interval = conf()->osd_deep_scrub_interval;
  }

  // shallow-scrub interval
  configs.shallow_interval =
    pool_conf.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
  if (configs.shallow_interval <= 0.0) {
    configs.shallow_interval = conf()->osd_scrub_min_interval;
  }

  // the max allowed delay between scrubs
  // For deep scrubs - there is no equivalent of scrub_max_interval. Per the
  // documentation, once deep_scrub_interval has passed, we are already
  // "overdue", at least as far as the "ignore allowed load" window is
  // concerned. RRR maybe: Thus - I'll use 'mon_warn_not_deep_scrubbed' as the
  // max delay.

  configs.max_deep =
    configs.deep_interval;  // conf()->mon_warn_not_deep_scrubbed;

  auto max_shallow = pool_conf.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  if (max_shallow <= 0.0) {
    max_shallow = conf()->osd_scrub_max_interval;
  }
  if (max_shallow > 0.0) {
    configs.max_shallow = max_shallow;
    // otherwise - we're left with the default nullopt
  }

  configs.interval_randomize_ratio = conf()->osd_scrub_interval_randomize_ratio;
  //configs.deep_randomize_ratio = conf()->osd_deep_scrub_randomize_ratio;
  configs.mandatory_on_invalid = conf()->osd_scrub_invalid_stats;
  return configs;
}

void ScrubQueue::set_initial_targets(
  Scrub::ScrubJobRef sjob,
  const requested_scrub_t& request_flags,
  const pg_info_t& pg_info,
  const Scrub::sched_conf_t& sched_configs)
{
  // assuming only called on 'on_primary_change': no need: std::unique_lock
  // lck{jobs_lock};
  auto now = time_now();
  dout(15) << fmt::format(
		"pg:[{} ({:s}/{:s})] (now:{:s}) conf:({})", pg_info.pgid,
		pg_info.history.last_scrub_stamp,
		pg_info.history.last_deep_scrub_stamp, now,
                sched_configs)
	   << dendl;
  sjob->initial_shallow_target(request_flags, pg_info, sched_configs, now);
  dout(15) << fmt::format(
		"after shallow pg:[{}] <{}>", pg_info.pgid,
		*(sjob->get_modif_trgt(scrub_level_t::shallow)))
	   << dendl;
  sjob->initial_deep_target(request_flags, pg_info, sched_configs, now);
  dout(15) << fmt::format(
		"after deep pg:[{}] <{}>", pg_info.pgid,
		*(sjob->get_modif_trgt(scrub_level_t::deep)))
	   << dendl;
  sjob->determine_closest();
  dout(15) << fmt::format(
		"best pg:[{}] <{}>", pg_info.pgid, *(sjob->closest_target))
	   << dendl;
}



#ifdef NOT_YET
Scrub::sched_params_t ScrubQueue::on_request_flags_change(
  const requested_scrub_t& request_flags,
  const pg_info_t& pg_info,
  const pool_opts_t pool_conf) const
{
  Scrub::sched_params_t res;
  dout(15) << ": requested_scrub_t: {}" <<  request_flags << dendl; 

  if (request_flags.must_scrub || request_flags.need_auto) {

    // Set the smallest time that isn't utime_t()
    res.proposed_time = PgScrubber::scrub_must_stamp();
    res.is_must = Scrub::must_scrub_t::mandatory;
    // we do not need the interval data in this case

  } else if (pg_info.stats.stats_invalid &&
	     conf()->osd_scrub_invalid_stats) {
    res.proposed_time = time_now();
    res.is_must = Scrub::must_scrub_t::mandatory;

  } else {
    res.proposed_time = pg_info.history.last_scrub_stamp;
    res.min_interval = pool_conf.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
    res.max_interval = pool_conf.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  }

  dout(15) << fmt::format(
		": suggested: {} hist: {} v: {}/{} must: {} pool-min: {}",
		res.proposed_time,
		pg_info.history.last_scrub_stamp,
		(bool)pg_info.stats.stats_invalid,
		conf()->osd_scrub_invalid_stats,
		(res.is_must == must_scrub_t::mandatory ? "y" : "n"),
		res.min_interval)
	   << dendl;
  return res;
}
#endif



/*
 * Modify the scrub job state:
 * - if 'registered' (as expected): mark as 'unregistering'. The job will be
 *   dequeued the next time sched_scrub() is called.
 * - if already 'not_registered': shouldn't really happen, but not a problem.
 *   The state will not be modified.
 * - same for 'unregistering'.
 *
 * Note: not holding the jobs lock
 */
void ScrubQueue::remove_from_osd_queue(Scrub::ScrubJobRef scrub_job)
{
  dout(15) << "removing pg[" << scrub_job->pgid << "] from OSD scrub queue"
	   << dendl;

  qu_state_t expected_state{qu_state_t::registered};
  auto ret =
    scrub_job->state.compare_exchange_strong(expected_state,
					     Scrub::qu_state_t::unregistering);

  if (ret) {

    //scrub_job->mark_for_dequeue();
    dout(10) << "pg[" << scrub_job->pgid << "] sched-state changed from "
	     << qu_state_text(expected_state) << " to "
	     << qu_state_text(scrub_job->state) << dendl;

  } else {

    // job wasn't in state 'registered' coming in
    dout(5) << "removing pg[" << scrub_job->pgid
	    << "] failed. State was: " << qu_state_text(expected_state)
	    << dendl;
  }
}

void ScrubQueue::register_with_osd(Scrub::ScrubJobRef scrub_job)
{
  // note: set_initial_targets() was just called by the caller, so we have
  // up-to-date information on the scrub targets
  qu_state_t state_at_entry = scrub_job->state.load();

  dout(15) << "pg[" << scrub_job->pgid << "] was "
	   << qu_state_text(state_at_entry) << dendl;

  //scrub_job->clear_marked_for_dequeue();

  switch (state_at_entry) {
    case qu_state_t::registered:
      // just updating the schedule? not thru here!
      //update_job(scrub_job, suggested);
      break;

    case qu_state_t::not_registered:
      // insertion under lock
      {
	std::unique_lock lck{jobs_lock};

	if (state_at_entry != scrub_job->state) {
	  lck.unlock();
	  dout(5) << " scrub job state changed" << dendl;
	  // retry
	  register_with_osd(scrub_job);
	  break;
	}


	//update_job(scrub_job, suggested);
        // done by caller: scrub_job->nschedule.calculate_effective(ceph_clock_now());
	//all_pgs.push_back(scrub_job);
	scrub_job->in_queues = true;
	scrub_job->state = qu_state_t::registered;

        // RRR where do we initialize the scrub-job's 'sched-targets'?
        //to_scrub.emplace_back(Scrub::SchedEntry{&(*scrub_job), scrub_level_t::shallow});
        to_scrub.emplace_back(Scrub::SchedEntry{scrub_job, scrub_level_t::shallow});
        to_scrub.emplace_back(Scrub::SchedEntry{scrub_job, scrub_level_t::deep});
      }

      break;

    case qu_state_t::unregistering:
      // restore to the queue
      {
	// must be under lock, as the job might be removed from the queue
	// at any minute
	std::lock_guard lck{jobs_lock};

	//update_job(scrub_job, suggested);
	if (scrub_job->state == qu_state_t::not_registered) {
	  dout(5) << " scrub job state changed to 'not registered'" << dendl;
	  // all_pgs.push_back(scrub_job);
          // RRR undo the 'mark_for_dequeue()'?
          // assuming the actual scrub targets are already in the queue
	}
	scrub_job->in_queues = true;
	scrub_job->state = qu_state_t::registered;
      }
      break;
  }

  dout(10) << "pg(" << scrub_job->pgid << ") sched-state changed from "
	   << qu_state_text(state_at_entry) << " to "
	   << qu_state_text(scrub_job->state)
	   << " at: " << scrub_job->closest_target->not_before << dendl;
}

#if 0
void ScrubQueue::register_with_osd(Scrub::ScrubJobRef scrub_job,
				   const Scrub::sched_params_t& suggested)
{
  // note: set_initial_targets() was just called by the caller, so we have
  // up-to-date information on the scrub targets

//   qu_state_t state_at_entry = scrub_job->state.load();
//   dout(15) << "pg[" << scrub_job->pgid << "] was "
// 	   << qu_state_text(state_at_entry) << dendl; // RRR fmt for this enum?
// 
//   switch (state_at_entry) {
//     case qu_state_t::registered:
//       // just updating the schedule?
//       update_job(scrub_job, suggested);
//       break;
// 
//     case qu_state_t::not_registered:
//       // insertion under lock
//       {
// 	std::unique_lock lck{jobs_lock};
// 
// 	if (state_at_entry != scrub_job->state) {
// 	  lck.unlock();
// 	  dout(5) << " scrub job state changed" << dendl;
// 	  // retry
// 	  register_with_osd(scrub_job, suggested);
// 	  break;
// 	}
// 
// 	update_job(scrub_job, suggested);
// 	to_scrub.push_back(scrub_job);
// 	scrub_job->in_queues = true;
// 	scrub_job->state = qu_state_t::registered;
//       }
// 
//       break;
// 
//     case qu_state_t::unregistering:
//       // restore to the to_sched queue
//       {
// 	// must be under lock, as the job might be removed from the queue
// 	// at any minute
// 	std::lock_guard lck{jobs_lock};
// 
// 	update_job(scrub_job, suggested);
// 	if (scrub_job->state == qu_state_t::not_registered) {
// 	  dout(5) << " scrub job state changed to 'not registered'" << dendl;
// 	  to_scrub.push_back(scrub_job);
// 	}
// 	scrub_job->in_queues = true;
// 	scrub_job->state = qu_state_t::registered;
//       }
//       break;
//   }
// 
//   dout(10) << "pg(" << scrub_job->pgid << ") sched-state changed from "
// 	   << qu_state_text(state_at_entry) << " to "
// 	   << qu_state_text(scrub_job->state)
// 	   << " at: " << scrub_job->nschedule.effective.not_before << dendl;
}
#endif



void ScrubQueue::update_job(Scrub::ScrubJobRef scrub_job,
			    const Scrub::sched_params_t& suggested)
{
  // adjust the suggested scrub time according to OSD-wide status
  auto adjusted = adjust_target_time(suggested);
  scrub_job->update_schedule(adjusted);
}

Scrub::sched_params_t ScrubQueue::determine_scrub_time(
  const requested_scrub_t& request_flags,
  const pg_info_t& pg_info,
  const pool_opts_t pool_conf) const
{
  Scrub::sched_params_t res;
  dout(15) << ": requested_scrub_t: {}" <<  request_flags << dendl; 

  if (request_flags.must_scrub || request_flags.need_auto) {

    // Set the smallest time that isn't utime_t()
    res.proposed_time = PgScrubber::scrub_must_stamp();
    res.is_must = Scrub::must_scrub_t::mandatory;
    // we do not need the interval data in this case

  } else if (pg_info.stats.stats_invalid &&
	     conf()->osd_scrub_invalid_stats) {
    res.proposed_time = time_now();
    res.is_must = Scrub::must_scrub_t::mandatory;

   // RRR and if we do not have osd_scrub_invalid_stats set??

  } else {
    res.proposed_time = pg_info.history.last_scrub_stamp;
    res.min_interval = pool_conf.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
    res.max_interval = pool_conf.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  }

  dout(15) << fmt::format(
		": suggested: {} hist: {} v: {}/{} must: {} pool-min: {}",
		res.proposed_time,
		pg_info.history.last_scrub_stamp,
		(bool)pg_info.stats.stats_invalid,
		conf()->osd_scrub_invalid_stats,
		(res.is_must == Scrub::must_scrub_t::mandatory ? "y" : "n"),
		res.min_interval)
	   << dendl;
  return res;
}

// the refactored "OSD::sched_all_scrubs()"
void ScrubQueue::on_config_times_change()
{
  dout(10) << fmt::format("{} - starting", __func__) << dendl;
  auto all_jobs = list_registered_jobs();  // RRR

  int modified_cnt{0};
  for (const auto& [job, lvl] : all_jobs) {
    auto trgt = job->get_current_trgt(lvl);
    dout(20) << fmt::format("{}: examine {} ({})", __func__, job->pgid, *trgt)
	     << dendl;

    PgLockWrapper locked_g = osd_service.get_locked_pg(job->pgid);
    PGRef pg = locked_g.m_pg;
    if (!pg)
      continue;

    if (!pg->is_primary()) {
      dout(20) << fmt::format("{}: {} is not primary", __func__, job->pgid)
               << dendl;
      continue;
    }

    // we have the job... auto& sjob = pg->m_scrubber->m_scrub_job;
    auto applicable_conf = populate_config_params(pg->get_pgpool().info.opts);

    if (job->on_periods_change(
          pg->info,
	  /*pg->get_pg_info(ScrubberPasskey),*/ applicable_conf,
	  pg->m_planned_scrub)) {
      dout(10) << fmt::format(
		    "{}: {} ({}) - rescheduled", __func__, job->pgid, *trgt)
	       << dendl;
      ++modified_cnt;
    }
    // auto-unlocked as 'locked_g' gets out of scope
  }

  dout(10) << fmt::format(
		"{}: {} planned scrubs rescheduled", __func__, modified_cnt)
	   << dendl;
}


// used under jobs_lock
void ScrubQueue::move_failed_pgs(utime_t now_is)
{
  // determine the penalty time, after which the job should be reinstated
  utime_t after = now_is;
  after += conf()->osd_scrub_sleep * 2 + utime_t{300'000ms};
  int punished_cnt{0};	// for log/debug only

  auto per_trgt = [](
		    Scrub::ScrubJobRef job, scrub_level_t lvl) mutable -> void {
    auto trgt = job->get_modif_trgt(lvl);
    if (
      trgt->urgency == urgency_t::periodic_regular ||
      trgt->urgency == urgency_t::overdue) {
      // RRR fix indentation (clang-format settings)
      trgt->urgency = urgency_t::penalized;
      trgt->push_nb_out(5s);
    }
  };

  for (auto& [job, lvl] : to_scrub) {
    if (!job->resources_failure || job->penalized)
      continue;

    per_trgt(job, scrub_level_t::deep);
    per_trgt(job, scrub_level_t::shallow);
    job->penalty_timeout = after;
    job->penalized = true;
    job->resources_failure = false;

    punished_cnt++;
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
std::string_view ScrubQueue::attempt_res_text(Scrub::schedule_result_t v)
{
  switch (v) {
    case Scrub::schedule_result_t::scrub_initiated: return "scrubbing"sv;
    case Scrub::schedule_result_t::none_ready: return "no ready job"sv;
    case Scrub::schedule_result_t::no_local_resources: return "local resources shortage"sv;
    case Scrub::schedule_result_t::already_started: return "denied as already started"sv;
    case Scrub::schedule_result_t::no_such_pg: return "pg not found"sv;
    case Scrub::schedule_result_t::bad_pg_state: return "prevented by pg state"sv;
    case Scrub::schedule_result_t::preconditions: return "preconditions not met"sv;
  }
  // g++ (unlike CLANG), requires an extra 'return' here
  return "(unknown)"sv;
}

std::string_view ScrubQueue::qu_state_text(Scrub::qu_state_t st)
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


void ScrubQueue::sched_scrub(
  const ceph::common::ConfigProxy& config,
  bool is_recovery_active)
{
  if (auto blocked_pgs = get_blocked_pgs_count(); blocked_pgs > 0) {
    // some PGs managed by this OSD were blocked by a locked object during
    // scrub. This means we might not have the resources needed to scrub now.
    dout(10)
      << fmt::format(
	   "{}: PGs are blocked while scrubbing due to locked objects ({} PGs)",
	   __func__, blocked_pgs)
      << dendl;
  }

  // sometimes we just skip the scrubbing
  if ((rand()/(double)RAND_MAX) >= cct->_conf->osd_scrub_backoff_ratio) {
    dout(20) << fmt::format(
		  "{}: lost coin flip, randomly backing off (ratio: {})",
		  __func__, cct->_conf->osd_scrub_backoff_ratio)
	     << dendl;
    return;
  }

  // fail fast if no resources are available
  if (!can_inc_scrubs()) {
    dout(10) << __func__ << ": OSD cannot inc scrubs" << dendl;
    return;
  }

  // if there is a PG that is just now trying to reserve scrub replica resources
  // - we should wait and not initiate a new scrub
  if (is_reserving_now()) {
    dout(20) << __func__ << ": scrub resources reservation in progress"
	     << dendl;
    return;
  }

  Scrub::ScrubPreconds env_conditions;

  if (is_recovery_active && !config->osd_scrub_during_recovery) {
    if (!config->osd_repair_during_recovery) {
      dout(15) << __func__ << ": not scheduling scrubs due to active recovery"
	       << dendl;
      return;
    }
    dout(10) << __func__
	     << " will only schedule explicitly requested repair due to active "
		"recovery"
	     << dendl;
    env_conditions.allow_requested_repair_only = true;
  }

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    dout(20) << __func__ << " sched_scrub starts" << dendl;
    auto all_jobs = list_registered_jobs();
    std::sort(all_jobs.begin(), all_jobs.end());
    for (const auto& [j, lvl] : all_jobs) {
      dout(20) << fmt::format(
		    "scrub_queue jobs: [{:s}] <<target: {}>>", *j,
		    *j->get_current_trgt(lvl))
	       << dendl;
    }
  }

  auto was_started = select_pg_and_scrub(env_conditions);
  dout(20) << "sched_scrub done (" << ScrubQueue::attempt_res_text(was_started)
	   << ")" << dendl;
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
  Scrub::ScrubPreconds& preconds)
{
  dout(10) << fmt::format(
		"jobs#:{} preconds: {}", to_scrub.size(),
		preconds)
	   << dendl;

  utime_t now_is = time_now();
  preconds.time_permit = scrub_time_permit(now_is);
  preconds.load_is_low = scrub_load_below_threshold();
  preconds.only_deadlined = !preconds.time_permit || !preconds.load_is_low;

  //  create a list of candidates (copying, as otherwise creating a deadlock):
  //  - possibly restore penalized
  //  - (if we didn't handle directly) remove invalid jobs
  //  - create a copy of the to_scrub (possibly up to first not-ripe)
  //  unlock, then try the lists

  std::unique_lock lck{jobs_lock};

  rm_unregistered_jobs();

  // pardon all penalized jobs that have deadlined
  scan_penalized(restore_penalized, now_is);
  restore_penalized = false;

  // change the urgency of all jobs that failed to reserve resources
  move_failed_pgs(now_is);

  //  collect all valid & ripe jobs from the two lists. Note that we must copy,
  //  as when we use the lists we will not be holding jobs_lock (see
  //  explanation above)

  auto to_scrub_copy = collect_ripe_jobs(to_scrub, now_is);
  lck.unlock();

  // try the regular queue first
  auto res = select_n_scrub(to_scrub_copy, preconds, now_is);

  dout(15) << dendl;  // RRR rm/modify this line
  return res;
}

// must be called under lock
void ScrubQueue::rm_unregistered_jobs()
{
  std::for_each(to_scrub.begin(), to_scrub.end(), [this](auto& trgt) {
    // 'trgt' is one of the two entries belonging to a single job (single PG)
    if (trgt.job->state == qu_state_t::unregistering) {
      trgt.job->in_queues = false;
      trgt.job->state = qu_state_t::not_registered;
    } else if (trgt.job->state == qu_state_t::not_registered) {
      trgt.job->in_queues = false;
    }
    if (!trgt.job->in_queues) {
      // RRR make sure we could not be scrubbing this target now
      dout(20) << fmt::format("{}: removing job: {}", __func__, *trgt.job)
	       << dendl;
      //trgt.job->disable_scheduling();
      trgt.job->mark_for_dequeue();
    }
  });

  to_scrub.erase(
    std::remove_if(
      to_scrub.begin(), to_scrub.end(),
      [](const auto& sched_entry) {
	return sched_entry.target()->marked_for_dequeue;
      }),
    to_scrub.end());
}


// called under lock
ScrubQueue::SchedulingQueue ScrubQueue::collect_ripe_jobs(
  SchedulingQueue& group,
  utime_t time_now)
{
/*
  RRR consider rotating the list based on 'ripeness', then sorting only
      the ripe entries, and returning the first N entries.
*/
  // copy ripe jobs
  ScrubQueue::SchedulingQueue ripes;
  ripes.reserve(group.size());

  std::copy_if(
    group.begin(), group.end(), std::back_inserter(ripes),
    [time_now](const auto& trgt) -> bool {
      return trgt.target()->is_ripe(time_now) && !trgt.is_scrubbing();
    });
  std::sort(ripes.begin(), ripes.end());

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    for (const auto& trgt : group) {
      if (true || !trgt.target()->is_ripe(time_now)) {
	dout(20) << fmt::format(
		      " not ripe: {} @ {} ({})", trgt.job->pgid,
		      trgt.target()->not_before, *trgt.target())
		 << dendl;
      }
    }
  }

  // only if there are no ripe jobs, and as a help to the
  // readers of the log - ort the entire list
  if (ripes.empty()) {
    std::sort(group.begin(), group.end());
  }

  return ripes;
}

// not holding jobs_lock. 'group' is a copy of the actual list.
// And the scheduling targets are holding a ref to their parent jobs.
Scrub::schedule_result_t ScrubQueue::select_n_scrub(
  SchedulingQueue& group,
  const Scrub::ScrubPreconds& preconds,
  utime_t now_is)
{
  dout(15) << fmt::format("{}: ripe jobs #:{}. Preconds: {}",
                          __func__, group.size(), preconds)
           << dendl;

  for (auto& candidate : group) {
    // we expect the first job in the list to be a good candidate (if any)

    auto pgid = candidate.job->pgid;
    dout(10) << fmt::format("initiating a scrub for pg[{}] ({}) [preconds:{}]",
                            pgid, *candidate.target(), preconds)
             << dendl;

    // RRR should we take 'urgency' into account here?
    if (preconds.only_deadlined && !candidate.target()->over_deadline(now_is)) {
      dout(15) << " not scheduling scrub for " << pgid << " due to "
	       << (preconds.time_permit ? "high load" : "time not permitting")
	       << dendl;
      candidate.target()->wrong_time();
      continue;
    }

    // are we left with only penalized jobs?
    if (candidate.target()->urgency == urgency_t::penalized) {
      dout(15) << " only penalized jobs left. Pardoning them all" << dendl;
      restore_penalized = true;
    }

    // we have a candidate to scrub. We turn to the OSD to verify that the PG
    // configuration allows the specified type of scrub, and to initiate the
    // scrub.
    switch (
      osd_service.initiate_a_scrub(pgid, candidate,
				   preconds.allow_requested_repair_only)) {

      case Scrub::schedule_result_t::scrub_initiated:
	// the happy path. We are done
	dout(20) << " initiated for " << pgid << dendl;
        candidate.target()->last_issue = delay_cause_t::none;
        // moved into set_op_p() candidate.target()->set_scrubbing();
	return Scrub::schedule_result_t::scrub_initiated;

      case Scrub::schedule_result_t::already_started:
	// continue with the next job
	dout(20) << "already started " << pgid << dendl;
	break;

      case Scrub::schedule_result_t::preconditions:
      case Scrub::schedule_result_t::bad_pg_state:
	// continue with the next job
	dout(20) << "failed (state/cond) " << pgid << dendl;
        candidate.target()->pg_state_failure();
	break;

      case Scrub::schedule_result_t::no_such_pg:
	// The pg is no longer there
	dout(20) << "failed (no pg) " << pgid << dendl;
	break;

      case Scrub::schedule_result_t::no_local_resources:
	// failure to secure local resources. No point in trying the other
	// PGs at this time. Note that this is not the same as replica resources
	// failure!
	dout(20) << "failed (local) " << pgid << dendl;
        candidate.target()->no_local_resources();
	return Scrub::schedule_result_t::no_local_resources;

      case Scrub::schedule_result_t::none_ready:
	// can't happen. Just for the compiler.
	dout(5) << "failed !!! " << pgid << dendl;
	return Scrub::schedule_result_t::none_ready;
    }
  }

  dout(20) << " returning 'none ready' " << dendl;
  return Scrub::schedule_result_t::none_ready;
}


Scrub::scrub_schedule_t ScrubQueue::adjust_target_time(
  const Scrub::sched_params_t& times) const
{
  Scrub::scrub_schedule_t sched_n_dead{times.proposed_time,
					    times.proposed_time};

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    dout(20) << "min t: " << times.min_interval
	     << " osd: " << conf()->osd_scrub_min_interval
	     << " max t: " << times.max_interval
	     << " osd: " << conf()->osd_scrub_max_interval << dendl;

    dout(20) << "at " << sched_n_dead.scheduled_at << " ratio "
	     << conf()->osd_scrub_interval_randomize_ratio << dendl;
  }

  if (times.is_must == Scrub::must_scrub_t::not_mandatory) {

    // unless explicitly requested, postpone the scrub with a random delay
    double scrub_min_interval = times.min_interval > 0
				  ? times.min_interval
				  : conf()->osd_scrub_min_interval;
    double scrub_max_interval = times.max_interval > 0
				  ? times.max_interval
				  : conf()->osd_scrub_max_interval;

    sched_n_dead.scheduled_at += scrub_min_interval;
    double r = rand() / (double)RAND_MAX;
    sched_n_dead.scheduled_at +=
      scrub_min_interval * conf()->osd_scrub_interval_randomize_ratio * r;

    if (scrub_max_interval <= 0) {
      sched_n_dead.deadline = utime_t{};
    } else {
      sched_n_dead.deadline += scrub_max_interval;
    }
  }

  dout(17) << "at (final) " << sched_n_dead.scheduled_at << " - "
	   << sched_n_dead.deadline << dendl;
  return sched_n_dead;
}

double ScrubQueue::scrub_sleep_time(bool must_scrub) const
{
  double regular_sleep_period = conf()->osd_scrub_sleep;

  if (must_scrub || scrub_time_permit(time_now())) {
    return regular_sleep_period;
  }

  // relevant if scrubbing started during allowed time, but continued into
  // forbidden hours
  double extended_sleep = conf()->osd_scrub_extended_sleep;
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
  if (loadavg_per_cpu < conf()->osd_scrub_load_threshold) {
    dout(20) << "loadavg per cpu " << loadavg_per_cpu << " < max "
	     << conf()->osd_scrub_load_threshold << " = yes" << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (loadavgs[0] < daily_loadavg && loadavgs[0] < loadavgs[2]) {
    dout(20) << "loadavg " << loadavgs[0] << " < daily_loadavg "
	     << daily_loadavg << " and < 15m avg " << loadavgs[2] << " = yes"
	     << dendl;
    return true;
  }

  dout(20) << "loadavg " << loadavgs[0] << " >= max "
	   << conf()->osd_scrub_load_threshold << " and ( >= daily_loadavg "
	   << daily_loadavg << " or >= 15m avg " << loadavgs[2] << ") = no"
	   << dendl;
  return false;
}


// note: called with jobs_lock held
void ScrubQueue::scan_penalized(bool forgive_all, utime_t time_now)
{
  dout(20) << fmt::format("{}: forgive_all: {}", __func__, forgive_all) << dendl;
  ceph_assert(ceph_mutex_is_locked(jobs_lock));

  for (auto& candidate : to_scrub) {
    if (candidate.target()->urgency == urgency_t::penalized) {
      if (forgive_all || candidate.target()->deadline < time_now) {
        candidate.job->un_penalize(time_now);
      }
    }
  }
}

// checks for half-closed ranges. Modify the (p<till)to '<=' to check for
// closed.
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

void ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << get_sched_time();
  f->dump_stream("deadline") << closest_target->deadline;
  f->dump_bool("forced",
	       get_sched_time() == PgScrubber::scrub_must_stamp());
  f->close_section();
}

void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  std::lock_guard lck(jobs_lock);
  f->open_array_section("scrubs");
  std::for_each(to_scrub.cbegin(), to_scrub.cend(), [&f](const auto& j) {
    j.target()->dump(f);
  });
  f->close_section();
}


// void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
// {
//   ceph_assert(f != nullptr);
//   std::lock_guard lck(jobs_lock);
// 
//   f->open_array_section("scrubs");
// 
//   std::for_each(to_scrub.cbegin(), to_scrub.cend(), [&f](const ScrubJobRef& j) {
//     j->dump(f);
//   });
// 
//   std::for_each(penalized.cbegin(),
// 		penalized.cend(),
// 		[&f](const ScrubJobRef& j) { j->dump(f); });
// 
//   f->close_section();
// }

ScrubQueue::SchedulingQueue ScrubQueue::list_registered_jobs() const
{
  ScrubQueue::SchedulingQueue all_targets;
  all_targets.reserve(to_scrub.size());
  dout(20) << " size: " << all_targets.capacity() << dendl;

  std::lock_guard lck{jobs_lock};

  std::copy(to_scrub.begin(),
	       to_scrub.end(),
	       std::back_inserter(all_targets));

  // RRR consider filtering-out the urgency==off
//   std::copy(penalized.begin(),
// 	       penalized.end(),
// 	       std::back_inserter(all_targets),
// 	       registered_job);

  return all_targets;
}



// ScrubQueue::ScrubQContainer ScrubQueue::list_registered_jobs() const
// {
//   ScrubQueue::ScrubQContainer all_jobs;
//   all_jobs.reserve(to_scrub.size() + penalized.size());
//   dout(20) << " size: " << all_jobs.capacity() << dendl;
// 
//   std::lock_guard lck{jobs_lock};
// 
//   std::copy_if(to_scrub.begin(),
// 	       to_scrub.end(),
// 	       std::back_inserter(all_jobs),
// 	       registered_job);
//   std::copy_if(penalized.begin(),
// 	       penalized.end(),
// 	       std::back_inserter(all_jobs),
// 	       registered_job);
// 
//   return all_jobs;
// }

// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue - scrub resource management

bool ScrubQueue::can_inc_scrubs() const
{
  // consider removing the lock here. Caller already handles delayed
  // inc_scrubs_local() failures
  std::lock_guard lck{resource_lock};

  if (scrubs_local + scrubs_remote < conf()->osd_max_scrubs) {
    return true;
  }

  dout(20) << " == false. " << scrubs_local << " local + " << scrubs_remote
	   << " remote >= max " << conf()->osd_max_scrubs << dendl;
  return false;
}

bool ScrubQueue::inc_scrubs_local()
{
  std::lock_guard lck{resource_lock};

  if (scrubs_local + scrubs_remote < conf()->osd_max_scrubs) {
    ++scrubs_local;
    return true;
  }

  dout(20) << ": " << scrubs_local << " local + " << scrubs_remote
	   << " remote >= max " << conf()->osd_max_scrubs << dendl;
  return false;
}

void ScrubQueue::dec_scrubs_local()
{
  std::lock_guard lck{resource_lock};
  dout(20) << ": " << scrubs_local << " -> " << (scrubs_local - 1) << " (max "
	   << conf()->osd_max_scrubs << ", remote " << scrubs_remote << ")"
	   << dendl;

  --scrubs_local;
  ceph_assert(scrubs_local >= 0);
}

bool ScrubQueue::inc_scrubs_remote()
{
  std::lock_guard lck{resource_lock};

  if (scrubs_local + scrubs_remote < conf()->osd_max_scrubs) {
    dout(20) << ": " << scrubs_remote << " -> " << (scrubs_remote + 1)
	     << " (max " << conf()->osd_max_scrubs << ", local "
	     << scrubs_local << ")" << dendl;
    ++scrubs_remote;
    return true;
  }

  dout(20) << ": " << scrubs_local << " local + " << scrubs_remote
	   << " remote >= max " << conf()->osd_max_scrubs << dendl;
  return false;
}

void ScrubQueue::dec_scrubs_remote()
{
  std::lock_guard lck{resource_lock};
  dout(20) << ": " << scrubs_remote << " -> " << (scrubs_remote - 1) << " (max "
	   << conf()->osd_max_scrubs << ", local " << scrubs_local << ")"
	   << dendl;
  --scrubs_remote;
  ceph_assert(scrubs_remote >= 0);
}

void ScrubQueue::dump_scrub_reservations(ceph::Formatter* f) const
{
  std::lock_guard lck{resource_lock};
  f->dump_int("scrubs_local", scrubs_local);
  f->dump_int("scrubs_remote", scrubs_remote);
  f->dump_int("osd_max_scrubs", conf()->osd_max_scrubs);
}

void ScrubQueue::clear_pg_scrub_blocked(spg_t blocked_pg)
{
  dout(5) << fmt::format(": pg {} is unblocked", blocked_pg) << dendl;
  --blocked_scrubs_cnt;
  ceph_assert(blocked_scrubs_cnt >= 0);
}

void ScrubQueue::mark_pg_scrub_blocked(spg_t blocked_pg)
{
  dout(5) << fmt::format(": pg {} is blocked on an object", blocked_pg)
	  << dendl;
  ++blocked_scrubs_cnt;
}

int ScrubQueue::get_blocked_pgs_count() const
{
  return blocked_scrubs_cnt;
}
