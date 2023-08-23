// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./osd_scrub.h"

#include <iostream>

#include "osd/scrubber/scrub_resources.h"
#include "osd/scrubber/osd_scrub_sched.h"

#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix_target(_dout, this)

template <class T>
static std::ostream& _prefix_target(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}


OsdScrub::OsdScrub(CephContext* cct, const ceph::common::ConfigProxy& config)
    : cct(cct)
    , conf(config)
    , m_resource_bookkeeper{[this](std::string msg) { log_fwd(msg); }, config}
    , m_queue{} // RRR: ScrubQueue
{
  dout(20) << fmt::format("{}: created", __func__) << dendl;
}


void OsdScrub::initiate_a_scrub(bool is_recovery_active)
{
  #ifdef NOT_YET
  if (auto blocked_pgs = scrub_scheduler.get_blocked_pgs_count();
      blocked_pgs > 0) {
    // some PGs managed by this OSD were blocked by a locked object during
    // scrub. This means we might not have the resources needed to scrub now.
    dout(10)
      << fmt::format(
	   "{}: PGs are blocked while scrubbing due to locked objects ({} PGs)",
	   __func__,
	   blocked_pgs)
      << dendl;
  }
  #endif

  // fail fast if no resources are available
  if (!m_resource_bookkeeper.can_inc_scrubs()) {
    dout(20) << fmt::format("{}: too many scrubs already running on this OSD", __func__) << dendl;
    return;
  }

  // if there is a PG that is just now trying to reserve scrub replica resources -
  // we should wait and not initiate a new scrub
  if (is_reserving_now()) {
    dout(20) << fmt::format("{}: scrub resources reservation in progress", __func__) << dendl;
    return;
  }

  m_scrub_tick_time = ceph_clock_now();
  dout(10) << fmt::format(
		  "{}: time now:{}, is_recovery_active:{}", __func__, m_scrub_tick_time,
		  is_recovery_active)
	   << dendl;


  // check the OSD-wide environment conditions (scrub resources, time, etc.).
  // These may restrict the type of scrubs we are allowed to start, or just
  // prevent us from starting any scrub at all.
  auto env_restrictions =
      restrictions_on_scrubbing(is_recovery_active, m_scrub_tick_time);
  if (!env_restrictions) {
    return;
  }

  #ifdef NOT_YET
  list the queue - will be done in select_pg_to_scrub()
  #endif

  /*
   at this phase of the refactoring: no change to the actual interface used
   to initiate a scrub (via the OSD).
   We loop over the queue, until the first PG that is not immediately not available
   for scrubbing.
  */
  while (true) { // RRR refine to include a max number of iterations
    // let the queue handle the load/time issues?
    auto candidate = m_queue.select_pg_to_scrub(*env_restrictions, m_scrub_tick_time);
    if (!candidate) {
      dout(20) << fmt::format("{}: no more PGs to try", __func__) << dendl;
      break;
    }

    // State @ entering:
    // - the target was already dequeued from the queue
    //
    // process now:
    // - mark the OSD as 'reserving now'
    // - queue the initiation message on the PG
    // - (later) set a timer for initiation confirmation/failure
    set_reserving_now();
    dout(20) << fmt::format("{}: initiating scrub on {}", __func__, *candidate) << dendl;

    // we have a candidate to scrub. We turn to the OSD to verify that the PG
    // configuration allows the specified type of scrub, and to initiate the
    // scrub.
    switch (
      osd_service.initiate_a_scrub(candidate->pgid,
				   env_restrictions->allow_requested_repair_only)) {

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




auto candidate = m_queue.select_pg_to_scrub(*env_restrictions, m_scrub_tick_time); candidate) {




  }


  auto candidate = m_queue.select_pg_to_scrub(*env_restrictions, m_scrub_tick_time);
  if (candidate) {
    // State @ entering:
    // - the target was already dequeued from the queue
    //
    // process now:
    // - mark the OSD as 'reserving now'
    // - queue the initiation message on the PG
    // - (later) set a timer for initiation confirmation/failure

  }

// 
// 
//   auto& scrub_scheduler = service.get_scrub_services();
// 
//   if (auto blocked_pgs = scrub_scheduler.get_blocked_pgs_count();
//       blocked_pgs > 0) {
//     // some PGs managed by this OSD were blocked by a locked object during
//     // scrub. This means we might not have the resources needed to scrub now.
//     dout(10)
//       << fmt::format(
// 	   "{}: PGs are blocked while scrubbing due to locked objects ({} PGs)",
// 	   __func__,
// 	   blocked_pgs)
//       << dendl;
//   }
// 
//   // fail fast if no resources are available
//   if (!scrub_scheduler.can_inc_scrubs()) {
//     dout(20) << __func__ << ": OSD cannot inc scrubs" << dendl;
//     return;
//   }
// 
//   // if there is a PG that is just now trying to reserve scrub replica resources -
//   // we should wait and not initiate a new scrub
//   if (scrub_scheduler.is_reserving_now()) {
//     dout(20) << __func__ << ": scrub resources reservation in progress" << dendl;
//     return;
//   }

//   Scrub::OSDRestrictions env_conditions;
// 
//   if (service.is_recovery_active() && !cct->_conf->osd_scrub_during_recovery) {
//     if (!cct->_conf->osd_repair_during_recovery) {
//       dout(15) << __func__ << ": not scheduling scrubs due to active recovery"
// 	       << dendl;
//       return;
//     }
//     dout(10) << __func__
//       << " will only schedule explicitly requested repair due to active recovery"
//       << dendl;
//     env_conditions.allow_requested_repair_only = true;
//   }

//   if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
//     dout(20) << __func__ << " sched_scrub starts" << dendl;
//     auto all_jobs = scrub_scheduler.list_registered_jobs();
//     for (const auto& sj : all_jobs) {
//       dout(20) << "sched_scrub scrub-queue jobs: " << *sj << dendl;
//     }
//   }

//  auto was_started = scrub_scheduler.select_pg_and_scrub(env_conditions, m_scrub_tick_time);
  dout(20) << "sched_scrub done (" << ScrubQueue::attempt_res_text(was_started)
	   << ")" << dendl;
}

void OsdScrub::log_fwd(std::string_view text)
{
  dout(20) << text << dendl;
}

std::optional<Scrub::OSDRestrictions> OsdScrub::restrictions_on_scrubbing(
    bool is_recovery_active,
    utime_t scrub_clock_now) const
{
  if (auto blocked_pgs = get_blocked_pgs_count(); blocked_pgs > 0) {
    // some PGs managed by this OSD were blocked by a locked object during
    // scrub. This means we might not have the resources needed to scrub now.
    dout(10) << fmt::format(
		    "{}: PGs are blocked while scrubbing due to locked objects "
		    "({} PGs)",
		    __func__, blocked_pgs)
	     << dendl;
  }

  // sometimes we just skip the scrubbing
  if (Scrub::was_randomized_below(config->osd_scrub_backoff_ratio)) {
    dout(20) << fmt::format(
		    "{}: lost coin flip, randomly backing off (ratio: {:f})",
		    __func__, config->osd_scrub_backoff_ratio)
	     << dendl;
    return std::nullopt;
  }

  // our local OSD may already be running too many scrubs
  if (!resource_bookkeeper().can_inc_scrubs()) {
    dout(10) << fmt::format("{}: OSD cannot inc scrubs", __func__) << dendl;
    return std::nullopt;
  }

  // if there is a PG that is just now trying to reserve scrub replica resources
  // - we should wait and not initiate a new scrub
  if (is_reserving_now()) {
    dout(10) << fmt::format(
		    "{}: scrub resources reservation in progress", __func__)
	     << dendl;
    return std::nullopt;
  }

  Scrub::OSDRestrictions env_conditions;
  env_conditions.time_permit = scrub_time_permit();
  env_conditions.load_is_low = m_load_tracker.scrub_load_below_threshold();
  env_conditions.only_deadlined =
      !env_conditions.time_permit || !env_conditions.load_is_low;

  if (is_recovery_active && !config->osd_scrub_during_recovery) {
    if (!config->osd_repair_during_recovery) {
      dout(15) << fmt::format(
		      "{}: not scheduling scrubs due to active recovery",
		      __func__)
	       << dendl;
      return std::nullopt;
    }

    dout(10) << fmt::format(
		    "{}: will only schedule explicitly requested repair due to "
		    "active recovery",
		    __func__)
	     << dendl;
    env_conditions.allow_requested_repair_only = true;
  }

  return env_conditions;
}
