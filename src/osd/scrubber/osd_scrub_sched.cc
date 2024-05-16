// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./osd_scrub_sched.h"

#include <string_view>
#include "osd/OSD.h"

#include "pg_scrubber.h"

using namespace ::std::chrono;
using namespace ::std::chrono_literals;
using namespace ::std::literals;
namespace rng = std::ranges;



//using qu_state_t = Scrub::qu_state_t;
using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using ScrubJob = Scrub::ScrubJob;



// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue

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

ScrubQueue::ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds)
    : cct{cct}
    , osd_service{osds}
{}

std::ostream& ScrubQueue::gen_prefix(std::ostream& out, std::string_view fn)
    const
{
  return out << fmt::format(
	     "osd.{} scrub-queue:{}: ", osd_service.get_nodeid(), fn);
}

/*
 * Remove the scrub job from the OSD scrub queue.
 * Mark the Scrubber-owned job as 'not_registered'.
 */
void ScrubQueue::remove_from_osd_queue(Scrub::ScrubJob& scrub_job)
{
  dout(10) << fmt::format(
                  "removing pg[{}] from OSD scrub queue", scrub_job.pgid)
          << dendl;

  std::unique_lock lck{jobs_lock};

  to_scrub.erase(std::remove_if(to_scrub.begin(), to_scrub.end(),
                                [&scrub_job](const auto& job) {
                                  return job.pgid == scrub_job.pgid;
                                }),
                 to_scrub.end());
  scrub_job.in_queues = false;
  // RRR a good time to reset / recreate the original sjob?
}

void ScrubQueue::enqueue_target(Scrub::ScrubJob& sjob)
{
  std::unique_lock lck{jobs_lock};
  to_scrub.push_back(sjob);
}

std::optional<Scrub::ScrubJob> ScrubQueue::dequeue_target(spg_t pgid)
{
  std::unique_lock lck{jobs_lock};

  auto it = std::find_if(
      to_scrub.begin(), to_scrub.end(),
      [pgid](const auto& job) { return job.pgid == pgid; });

  if (it == to_scrub.end()) {
    return std::nullopt;
  }

  auto sjob = *it;
  to_scrub.erase(it);
  return sjob;
}


void ScrubQueue::register_with_osd(
    Scrub::ScrubJob& scrub_job,
    const sched_params_t& suggested)
{
  bool was_registered = scrub_job.in_queues;

  if (was_registered) {
    // just updating the schedule?
    update_job(scrub_job, suggested, false /* keep n.b. delay */);
  } else {
    // insertion under lock
    {
      std::unique_lock lck{jobs_lock};

      update_job(scrub_job, suggested, true /* reset not_before */);
      to_scrub.push_back(scrub_job);
      scrub_job.in_queues = true;
    }
  }

  dout(10)
      << fmt::format(
	     "pg[{}] sched-state changed from <{:.14}> to <{:.14}> (@{:s})",
	     scrub_job.pgid, ScrubJob::qu_state_text(was_registered),
	     ScrubJob::qu_state_text(true), scrub_job.schedule.not_before)
      << dendl;
}


// void ScrubQueue::update_job(Scrub::ScrubJob& scrub_job,
// 			    const sched_params_t& suggested,
//                             bool reset_nb)
// {
//   // adjust the suggested scrub time according to OSD-wide status
//   auto adjusted = adjust_target_time(suggested);
//   scrub_job.high_priority = suggested.is_must == must_scrub_t::mandatory;
//   scrub_job.update_schedule(adjusted, reset_nb);
// }


void ScrubQueue::delay_on_failure(
    Scrub::ScrubJob& sjob,
    std::chrono::seconds delay,
    Scrub::delay_cause_t delay_cause,
    utime_t now_is)
{
  dout(10) << fmt::format(
		  "pg[{}] delay_on_failure: delay:{} now:{:s}",
		  sjob.pgid, delay, now_is)
	   << dendl;
  sjob.delay_on_failure(delay, delay_cause, now_is);
}


// std::vector<ScrubTargetId> ScrubQueue::ready_to_scrub(
//     OSDRestrictions restrictions,  // note: 4B in size! (copy)
//     utime_t scrub_tick)
// {
//   dout(10) << fmt::format(
// 		  " @{:s}: registered: {} ({})", scrub_tick,
// 		  to_scrub.size(), restrictions)
// 	   << dendl;
// 
//   //  create a list of candidates (copying, as otherwise creating a deadlock):
//   //  - (if we didn't handle directly) remove invalid jobs
//   //  - create a copy of the to_scrub (possibly up to first not-ripe)
//   //  unlock, then try the lists
//   std::unique_lock lck{jobs_lock};
// 
//   // remove the 'updated' flag from all entries
//   std::for_each(
//       to_scrub.begin(), to_scrub.end(),
//       [](const auto& jobref) -> void { jobref->updated = false; });
// 
//   // collect all valid & ripe jobs. Note that we must copy,
//   // as when we use the lists we will not be holding jobs_lock (see
//   // explanation above)
// 
//   // and in this step 1 of the refactoring (Aug 2023): the set returned must be
//   // transformed into a vector of targets (which, in this phase, are
//   // the PG id-s).
//   auto to_scrub_copy = collect_ripe_jobs(to_scrub, restrictions, scrub_tick);
//   lck.unlock();
// 
//   std::vector<ScrubTargetId> all_ready;
//   std::transform(
//       to_scrub_copy.cbegin(), to_scrub_copy.cend(),
//       std::back_inserter(all_ready),
//       [](const auto& jobref) -> ScrubTargetId { return jobref->pgid; });
//   return all_ready;
// }
//
std::optional<ScrubJob> ScrubQueue::pop_ready_pg(
    OSDRestrictions restrictions,  // note: 4B in size! (copy)
    utime_t time_now)
{
  std::unique_lock lck{jobs_lock};

  auto filtr = [time_now, rst = restrictions](const ScrubJob& jb) -> bool {
    return jb.get_sched_time() <= time_now &&
	   (!rst.high_priority_only || jb.high_priority) &&
	   (!rst.only_deadlined || (!jb.schedule.deadline.is_zero() &&
				    jb.schedule.deadline <= time_now));
  };

  auto not_ripes = rng::partition(to_scrub, filtr);
  if (not_ripes.begin() == to_scrub.begin()) {
    return std::nullopt;
  }
  auto top = rng::min_element(
      to_scrub.begin(), not_ripes.begin(), rng::less(),
      [](const ScrubJob& jb) -> utime_t { return jb.get_sched_time(); });

  if (top == not_ripes.begin()) {
    return std::nullopt;
  }

  auto top_job = *top;
  to_scrub.erase(top);
  return top_job;
}

void ScrubQueue::restore_job(Scrub::ScrubJob&& sjob)
{
  std::lock_guard lck{jobs_lock};
  to_scrub.push_back(std::move(sjob));
  //sjob.in_queues = true;
  //sjob.state = qu_state_t::registered;
}


namespace {
struct cmp_time_n_priority_t {
  bool operator()(const Scrub::ScrubJob& lhs, const Scrub::ScrubJob& rhs)
      const
  {
    return lhs.is_high_priority() > rhs.is_high_priority() ||
	   (lhs.is_high_priority() == rhs.is_high_priority() &&
	    lhs.schedule.scheduled_at < rhs.schedule.scheduled_at);
  }
};
}  // namespace


// Scrub::scrub_schedule_t ScrubQueue::adjust_target_time(
//   const sched_params_t& times) const
// {
//   Scrub::scrub_schedule_t sched_n_dead{
//     times.proposed_time, times.proposed_time, times.proposed_time};
// 
//   if (times.is_must == Scrub::must_scrub_t::not_mandatory) {
//     // unless explicitly requested, postpone the scrub with a random delay
//     double scrub_min_interval = times.min_interval > 0
// 				  ? times.min_interval
// 				  : conf()->osd_scrub_min_interval;
//     double scrub_max_interval = times.max_interval > 0
// 				  ? times.max_interval
// 				  : conf()->osd_scrub_max_interval;
// 
//     sched_n_dead.scheduled_at += scrub_min_interval;
//     double r = rand() / (double)RAND_MAX;
//     sched_n_dead.scheduled_at +=
//       scrub_min_interval * conf()->osd_scrub_interval_randomize_ratio * r;
// 
//     if (scrub_max_interval <= 0) {
//       sched_n_dead.deadline = utime_t{};
//     } else {
//       sched_n_dead.deadline += scrub_max_interval;
//     }
//     // note: no specific job can be named in the log message
//     dout(20) << fmt::format(
// 		  "not-must. Was:{:s} {{min:{}/{} max:{}/{} ratio:{}}} "
// 		  "Adjusted:{:s} ({:s})",
// 		  times.proposed_time, fmt::group_digits(times.min_interval),
// 		  fmt::group_digits(conf()->osd_scrub_min_interval),
// 		  fmt::group_digits(times.max_interval),
// 		  fmt::group_digits(conf()->osd_scrub_max_interval),
// 		  conf()->osd_scrub_interval_randomize_ratio,
// 		  sched_n_dead.scheduled_at, sched_n_dead.deadline)
// 	     << dendl;
//   }
//   // else - no log needed. All relevant data will be logged by the caller
//   return sched_n_dead;
// }


void ScrubQueue::dump_scrubs(ceph::Formatter* f) const
{
  ceph_assert(f != nullptr);
  std::lock_guard lck(jobs_lock);

  f->open_array_section("scrubs");
  std::for_each(
      to_scrub.cbegin(), to_scrub.cend(),
      [&f](const Scrub::ScrubJob& j) { j.dump(f); });
  f->close_section();
}

/**
 * the set of all PGs named by the entries in the queue (but only those
 * entries that satisfy the predicate)
 */
std::set<spg_t> ScrubQueue::get_pgs(const ScrubQueue::EntryPred& cond) const
{
  std::lock_guard lck{jobs_lock};
  std::set<spg_t> pgs_w_matching_entries;
  rng::transform(
      to_scrub | std::views::filter(
		     [&cond](const auto& job) -> bool { return (cond)(job); }),
      std::inserter(pgs_w_matching_entries, pgs_w_matching_entries.end()),
      [](const auto& job) { return job.pgid; });
  return pgs_w_matching_entries;
}

ScrubQContainer ScrubQueue::list_registered_jobs() const
{
  ScrubQContainer all_jobs;
  all_jobs.reserve(to_scrub.size());
  dout(20) << " size: " << all_jobs.capacity() << dendl;

  std::lock_guard lck{jobs_lock};
  std::copy(to_scrub.begin(), to_scrub.end(), std::back_inserter(all_jobs)); // is this faster than copy? RRR
  return all_jobs;
}

// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue - maintaining the 'blocked on a locked object' count

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

// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue - maintaining the 'some PG is reserving' flag

bool ScrubQueue::set_reserving_now(spg_t reserving_id, utime_t now_is)
{
  std::unique_lock l{reserving_lock};

  if (!reserving_pg.has_value()) {
    reserving_pg = reserving_id;
    reserving_since = now_is;
    return true;
  }
  ceph_assert(reserving_id != *reserving_pg);
  return false;
}

void ScrubQueue::clear_reserving_now(spg_t was_reserving_id)
{
  std::unique_lock l{reserving_lock};
  if (reserving_pg && (*reserving_pg == was_reserving_id)) {
    reserving_pg.reset();
  }
  // otherwise - ignore silently
}

bool ScrubQueue::is_reserving_now() const
{
  // no lock needed, as set_reserving_now() will recheck
  return reserving_pg.has_value();
}
