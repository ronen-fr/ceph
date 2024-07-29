// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_job.h"
#include "pg_scrubber.h"

using qu_state_t = Scrub::qu_state_t;
using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using ScrubJob = Scrub::ScrubJob;
using SchedTarget = Scrub::SchedTarget;


#define dout_subsys ceph_subsys_osd
#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)


// ////////////////////////////////////////////////////////////////////////// //
// SchedTarget

  void SchedTarget::delay_on_failure(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now)
{
  sched_info.not_before =
      std::max(scrub_clock_now, sched_info.not_before) + utime_t{delay};
  last_issue = delay_cause;
}

void SchedTarget::dump(std::string_view sect_name, ceph::Formatter* f) const
{
  f->open_object_section(sect_name);
  /// \todo improve the performance of u_time dumps here
  f->dump_stream("pg") << sched_info.pgid;
  f->dump_stream("level")
      << (sched_info.level == scrub_level_t::deep ? "deep" : "shallow");
  //f->dump_stream("urgency") << fmt::format("{}", sched_info.urgency);
  f->dump_stream("target") << sched_info.target;
  f->dump_stream("not_before") << sched_info.not_before;
  f->dump_stream("deadline") << sched_info.deadline;
  f->dump_bool("auto_rpr", auto_repairing);
  f->dump_bool("forced", is_high_priority());
  f->dump_stream("last_delay") << fmt::format("{}", last_issue);
  f->close_section();
}


// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}

ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    : RefCountedObject{cct}
    , m_pgid{pg}
    , whoami{node_id}
    , m_shallow_target{cct, pg, scrub_level_t::shallow}
    , m_deep_target{cct, pg, scrub_level_t::deep}
    , cct{cct}
    , osd_scrub_queue{m_scrubber.m_osds->get_scrub_services().get_scrub_queue()}
    , log_msg_prefix{fmt::format("osd.{}: scrub-job:pg[{}]:", node_id, m_pgid)}
{}

// debug usage only
namespace std {
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}
}  // namespace std


void ScrubJob::init_and_register(
    const Scrub::sched_conf_t& aconf,
    utime_t scrub_time_now)
{
  std::lock_guard l{m_osd_scrub.get_scrub_queue_lock()};
  const int before_cnt = recalc_periodic_targets(aconf, true /*?*/, scrub_time_now);
  dout(10) << fmt::format(
		  "{} targets removed from queue; added {} & {}", before_cnt,
		  m_shallow_target, m_deep_target)
	   << dendl;
}

int ScrubJob::recalc_periodic_targets(
    const Scrub::sched_conf_t& aconf,
    bool modify_ready_tarets,
    utime_t scrub_time_now)
{
  // called with the queue lock held

  dout(/*25*/ 1) << fmt::format(
			"shallow_target: {} (rand: {})", m_shallow_target,
			aconf.interval_randomize_ratio)
		 << dendl;
  dout(/*25*/ 1) << fmt::format(
			"deep_target: {} (rand: {})", m_deep_target,
			aconf.deep_randomize_ratio)
		 << dendl;

  const int in_q_count = m_queue.dequeue_pg(m_pgid);
  m_shallow_target.clear_queued();
  m_deep_target.clear_queued();

  // re-calculate both targets as periodic scrubs (unless already
  // set for a higher priority scrub)

  const auto& info = m_scrubber.m_pg->get_pg_info(ScrubberPasskey{});

  if (modify_ready_tarets || !m_shallow_target.is_ripe(scrub_time_now)) {
    m_shallow_target.update_periodic_shallow(info, aconf, scrub_time_now);
    dout(/*25*/ 20) << fmt::format(
			   "m_shallow_target: {} (rand: {})", m_shallow_target,
			   aconf.interval_randomize_ratio)
		    << dendl;
  }

  if (modify_ready_tarets || !m_deep_target.is_ripe(scrub_time_now)) {
    m_deep_target.update_periodic_deep(info, aconf, scrub_time_now);
    dout(/*25*/ 20) << fmt::format("deep_target: {}", m_deep_target) << dendl;
  }

  m_queue.enqueue_targets(
      m_pgid, m_shallow_target.queued_element(),
      m_deep_target.queued_element());
  m_shallow_target.set_queued();
  m_deep_target.set_queued();
  dout(25) << fmt::format(
		  "{} targets removed from queue; added {} & {}", in_q_count,
		  m_shallow_target, m_deep_target)
	   << dendl;

  return in_q_count;
}


void ScrubJob::update_schedule(
    const Scrub::scrub_schedule_t& adjusted,
    bool reset_failure_penalty)
{
  dout(15) << fmt::format(
		  "was: nb:{:s}({:s}). Called with: rest?{} {:s} ({})",
		  schedule.not_before, schedule.scheduled_at,
		  reset_failure_penalty, adjusted.scheduled_at,
		  registration_state())
	   << dendl;
  schedule.scheduled_at = adjusted.scheduled_at;
  schedule.deadline = adjusted.deadline;

  if (reset_failure_penalty || (schedule.not_before < schedule.scheduled_at)) {
    schedule.not_before = schedule.scheduled_at;
  }

  updated = true;
  dout(10) << fmt::format(
		  "adjusted: nb:{:s} ({:s}) ({})", schedule.not_before,
		  schedule.scheduled_at, registration_state())
	   << dendl;
}

void ScrubJob::delay_on_failure(
    scrub_level_t level,
    std::chrono::seconds delay,
    Scrub::delay_cause_t delay_cause,
    utime_t scrub_clock_now)
{
  auto& trgt = (level == scrub_level_t::deep ? m_deep_target : m_shallow_target);
  trgt.delay_on_failure(delay, delay_cause, scrub_clock_now);
}


pg_scrubbing_status_t ScrubJob::get_job_schedule(utime_t now_is) const
{
  if (state != Scrub::qu_state_t::registered) {
    return pg_scrubbing_status_t{utime_t{},
				 0,
				 pg_scrub_sched_status_t::not_queued,
				 false,
				 scrub_level_t::shallow,
				 false};
  }

  const auto& nxt_target = closest_target(now_is);

  // are we ripe for scrubbing?
  if (nxt_target.is_ripe(now_is)) {
    return pg_scrubbing_status_t{nxt_target.get_sched_time(),
                                 0,
                                 pg_scrub_sched_status_t::queued,
                                 nxt_target.is_high_priority(),
                                 nxt_target.level(),
                                 !nxt_target.is_high_priority()};
  }

  // 'not_before' is in the future.
  return pg_scrubbing_status_t{nxt_target.get_sched_time(),
			       0,
			       pg_scrub_sched_status_t::scheduled,
			       false,
			       nxt_target.level(),
			       !nxt_target.is_high_priority()};
}


std::string ScrubJob::scheduling_state(
    utime_t now_is,
    [[maybe_unused]] bool is_deep_expected) const
{
  // if not in the OSD scheduling queues, not a candidate for scrubbing
  if (state != qu_state_t::registered) {
    return "no scrub is scheduled";
  }

  // fix the compare function RRR
  const auto& nxt_target = closest_target(now_is);

  std::string_view lvl_desc =
      (nxt_target.level() == scrub_level_t::deep) ? "deep " : "";

  // if the time has passed, we are surely in the queue
  if (now_is > nxt_target.get_sched_time()) {
    return fmt::format("queued for {}scrub", lvl_desc);
  }

  return fmt::format(
      "{}scrub scheduled @ {:s} (orig:{:s})", lvl_desc,
      nxt_target.get_sched_time(), nxt_target.get_target_time());
}


utime_t ScrubJob::get_sched_time() const
{
  auto time_now = ceph_clock_now();
  return closest_target(time_now).get_sched_time();
}

// std::string ScrubJob::scheduling_state(utime_t now_is, bool is_deep_expected)
//     const
// {
//   // if not in the OSD scheduling queues, not a candidate for scrubbing
//   if (state != qu_state_t::registered) {
//     return "no scrub is scheduled";
//   }
// 
//   // if the time has passed, we are surely in the queue
//   if (now_is > schedule.not_before) {
//     // we are never sure that the next scrub will indeed be shallow:
//     return fmt::format("queued for {}scrub", (is_deep_expected ? "deep " : ""));
//   }
// 
//   return fmt::format(
//       "{}scrub scheduled @ {:s} ({:s})", (is_deep_expected ? "deep " : ""),
//       schedule.not_before, schedule.scheduled_at);
// }

std::ostream& ScrubJob::gen_prefix(std::ostream& out, std::string_view fn) const
{
  return out << log_msg_prefix << fn << ": ";
}

// clang-format off
std::string_view ScrubJob::qu_state_text(qu_state_t st)
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

// a very temporary version of cmp_targets()
namespace {

int cmp_targets(utime_t scrub_clock_now, const SchedTarget& l, const SchedTarget& r)
{
  // ignore most data in this temporary version:
  auto upd_time_l = (l.get_sched_time() < scrub_clock_now) ? utime_t::max() : l.get_target_time();
  auto upd_time_r = (r.get_sched_time() < scrub_clock_now) ? utime_t::max() : r.get_target_time();

  if (upd_time_l < upd_time_r) {
    return -1;
  } else if (upd_time_l > upd_time_r) {
    return 1;
  } else {
    return 0;
  }
  // the original version:
//   bool l_ripe = l.is_ripe(scrub_clock_now);
//   bool r_ripe = r.is_ripe(scrub_clock_now);
//   if (l_ripe) {
//     if (r_ripe) {
//       return l.cmp_ripe(r);
//     }
//     return -1;
//   }
//   if (r_ripe) {
//     return 1;
//   }
//   return l.cmp_future(r);
}

}  // namespace

SchedTarget& ScrubJob::closest_target(utime_t scrub_clock_now)
{
  if (cmp_targets(scrub_clock_now, m_shallow_target, m_deep_target) < 0) {
    return m_shallow_target;
  } else {
    return m_deep_target;
  }
}

const SchedTarget& ScrubJob::closest_target(utime_t scrub_clock_now) const
{
  if (cmp_targets(scrub_clock_now, m_shallow_target, m_deep_target) < 0) {
    return m_shallow_target;
  } else {
    return m_deep_target;
  }
}

void ScrubJob::dump(ceph::Formatter* f, utime_t now_is) const
{
  f->open_object_section("scheduling");
  f->dump_stream("pgid") << m_pgid;
  f->dump_stream("sched_time") << get_sched_time();
  const auto& nearest = closest_target(now_is);
  f->dump_stream("deadline") << nearest.get_deadline();

  nearest.dump("nearest", f);
  m_shallow_target.dump("shallow_target", f);
  m_deep_target.dump("deep_target", f);
  f->dump_bool("forced", nearest.is_high_priority());
  f->dump_bool("blocked", blocked);
  f->close_section();
}

// void ScrubJob::dump(ceph::Formatter* f) const
// {
//   f->open_object_section("scrub");
//   f->dump_stream("pgid") << pgid;
//   f->dump_stream("sched_time") << schedule.not_before;
//   f->dump_stream("orig_sched_time") << schedule.scheduled_at;
//   f->dump_stream("deadline") << schedule.deadline;
//   f->dump_bool("forced",
// 	       schedule.scheduled_at == PgScrubber::scrub_must_stamp());
//   f->close_section();
// }
