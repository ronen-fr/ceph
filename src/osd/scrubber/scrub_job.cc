// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_job.h"
#include "pg_scrubber.h"

using qu_state_t = Scrub::qu_state_t;
using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using SchedTarget = Scrub::SchedTarget;

namespace {
utime_t add_double(utime_t t, double d)
{
  double int_part;
  double frac_as_ns = 1'000'000'000 * std::modf(d, &int_part);
  return utime_t{
      t.sec() + static_cast<int>(int_part),
      static_cast<int>(t.nsec() + frac_as_ns)};
}
}  // namespace



// ////////////////////////////////////////////////////////////////////////// //
// SchedTarget

void SchedTarget::reset()
{
  // a bit convoluted, but the standard way to guarantee we keep the
  // same set of member defaults as the constructor
  *this = SchedTarget{sched_info.pgid, sched_info.level};
}

bool SchedTarget::over_deadline(utime_t now_is) const
{
  return sched_info.urgency > urgency_t::off && now_is >= sched_info.deadline;
}

bool SchedTarget::is_periodic() const
{
  return sched_info.urgency == urgency_t::periodic_regular;
}

// utime_t SchedTarget::sched_time() const
// {
//   return sched_info.not_before;
// }

void SchedTarget::up_urgency_to(urgency_t u)
{
  sched_info.urgency = std::max(sched_info.urgency, u);
}

void SchedTarget::delay_on_failure(
      std::chrono::seconds delay,
      delay_cause_t delay_cause,
      utime_t scrub_clock_now)
{
  sched_info.not_before =
      std::max(scrub_clock_now, sched_info.not_before) + utime_t{delay};
  last_issue = delay_cause;
}


void SchedTarget::update_periodic_shallow(
    const pg_info_t& pg_info,
    const Scrub::sched_conf_t& config,
    utime_t time_now)
{
  ceph_assert(sched_info.level == scrub_level_t::shallow);
  ceph_assert(!in_queue);

  if (is_high_priority()) {
    // shouldn't be called for high-urgency scrubs
    return;
  }

  if (pg_info.stats.stats_invalid && config.mandatory_on_invalid) {
    sched_info.urgency = urgency_t::must_repair; /// \todo review priority
    sched_info.target = time_now;
    sched_info.not_before = time_now;
    // we will force a deadline in this case
    if (config.max_shallow && *config.max_shallow > 0.1) {
      sched_info.deadline = add_double(time_now, *config.max_shallow);
    } else {
      sched_info.deadline = add_double(time_now, config.shallow_interval);
    }
  } else {
    auto base = pg_info.stats.stats_invalid ? time_now
					    : pg_info.history.last_scrub_stamp;
    sched_info.target = add_double(base, config.shallow_interval);
    // if in the past - do not delay. Otherwise - add a random delay
    if (sched_info.target > time_now) {
      double r = rand() / (double)RAND_MAX;
      sched_info.target +=
	  config.shallow_interval * config.interval_randomize_ratio * r;
    }
    sched_info.not_before = sched_info.target;
    sched_info.urgency = urgency_t::periodic_regular;

    if (config.max_shallow && *config.max_shallow > 0.1) {
      sched_info.deadline = add_double(sched_info.target, *config.max_shallow);

#ifdef NOT_YET
      if (time_now > sched_info.deadline) {
	sched_info.urgency = urgency_t::overdue;
      }
#endif
    } else {
      sched_info.deadline = utime_t::max();
    }
  }

  // does not match the original logic, but seems to be required
  // for testing (standalone/scrub-test):
  /// \todo fix the tests and remove this
  sched_info.deadline = add_double(sched_info.target, config.max_deep);
}


void SchedTarget::update_periodic_deep(
    const pg_info_t& pg_info,
    const Scrub::sched_conf_t& config,
    utime_t time_now)
{
  ceph_assert(sched_info.level == scrub_level_t::deep);
  ceph_assert(!in_queue);

  if (is_high_priority()) {
    // shouldn't be called for high-urgency scrubs
    return;
  }

  // a special case for a PG with deep errors: no periodic shallow
  // scrubs are to be performed, and the next deep scrub is scheduled
  // instead (at shallow scrubs interval)

  /// \todo in a followup PR (note: scrub-store structure should be
  /// modified in order to avoid this special case).

  // note that (based on existing code) we do not require an immediate
  // deep scrub if no stats are available (only a shallow one)
  auto base = pg_info.stats.stats_invalid
		  ? time_now
		  : pg_info.history.last_deep_scrub_stamp;

  sched_info.target = add_double(base, config.deep_interval);
  // if in the past - do not delay. Otherwise - add a random delay
  if (sched_info.target > time_now) {
    double r = rand() / (double)RAND_MAX;
    sched_info.target +=
	config.deep_interval * config.interval_randomize_ratio * r;
  }
  sched_info.not_before = sched_info.target;
  sched_info.deadline = add_double(sched_info.target, config.max_deep);

  sched_info.urgency = urgency_t::periodic_regular;
#ifdef NOT_YET
  if (time_now > sched_info.deadline) {
    sched_info.urgency = urgency_t::overdue;
  }
#endif
  auto_repairing = false;
}

void SchedTarget::set_oper_shallow_target(
    scrub_type_t rpr,
    utime_t scrub_clock_now)
{
  ceph_assert(sched_info.level == scrub_level_t::shallow);
  ceph_assert(rpr != scrub_type_t::do_repair);
  ceph_assert(!in_queue);

  up_urgency_to(urgency_t::operator_requested);
  sched_info.target = std::min(scrub_clock_now, sched_info.target);
  sched_info.not_before = std::min(sched_info.not_before, scrub_clock_now);
  auto_repairing = false;
  last_issue = delay_cause_t::none;
}

void SchedTarget::set_oper_deep_target(
    scrub_type_t rpr,
    utime_t scrub_clock_now)
{
  ceph_assert(sched_info.level == scrub_level_t::deep);
  ceph_assert(!in_queue);

  if (rpr == scrub_type_t::do_repair) {
    up_urgency_to(urgency_t::must_repair);
    do_repair = true;
  } else {
    up_urgency_to(urgency_t::operator_requested);
  }
  sched_info.target = std::min(scrub_clock_now, sched_info.target);
  sched_info.not_before = std::min(sched_info.not_before, scrub_clock_now);
  auto_repairing = false;
  last_issue = delay_cause_t::none;
}

// both targets compared are assumed to be 'ripe', i.e. their not_before is
// in the past
std::weak_ordering cmp_ripe_targets(
    const Scrub::SchedTarget& l,
    const Scrub::SchedTarget& r)
{
  return cmp_ripe_entries(l.queued_element(), r.queued_element());
}

std::weak_ordering cmp_future_targets(
    const Scrub::SchedTarget& l,
    const Scrub::SchedTarget& r)
{
  return cmp_ripe_entries(l.queued_element(), r.queued_element());
}

std::weak_ordering
cmp_targets(utime_t t, const Scrub::SchedTarget& l, const Scrub::SchedTarget& r)
{
  return cmp_entries(t, l.queued_element(), r.queued_element());
}


std::string SchedTarget::fmt_print() const
{
  return fmt::format("{},q:{},ar:{},issue:{}", sched_info,
 	in_queue ? "+" : "-", auto_repairing ? "+" : "-", last_issue);
}

// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

#define dout_subsys ceph_subsys_osd
#undef dout_context
#define dout_context (m_scrubber.cct)
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}



// debug usage only
namespace std {
ostream& operator<<(ostream& out, const Scrub::ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}
}  // namespace std

namespace Scrub {

ScrubJob::ScrubJob(PgScrubber& scrubber, const spg_t& pg)
    : m_scrubber{scrubber}
    , m_pgid{pg}
, m_shallow_target{m_pgid, scrub_level_t::shallow}
, m_deep_target{m_pgid, scrub_level_t::deep}
    //, log_msg_prefix{fmt::format("osd.{}: scrub-job:pg[{}]:", node_id, pgid)}
{}

void ScrubJob::mark_target_dequeued(scrub_level_t scrub_level)
{
  auto& trgt = get_target(scrub_level);
  trgt.clear_queued();
}

void ScrubJob::operator_forced_targets(
    scrub_level_t level,
    scrub_type_t scrub_type,
    utime_t now_is)
{
  // the dequeue might fail, as we might be scrubbing that same target now,
  // but that's OK
  dequeue_target(level);
  if (level == scrub_level_t::shallow) {
    shallow_target.set_oper_shallow_target(scrub_type, now_is);
  } else {
    deep_target.set_oper_deep_target(scrub_type, now_is);
  }
  requeue_entry(level);
}


// void ScrubJob::update_schedule(
//     const Scrub::scrub_schedule_t& adjusted,
//     bool reset_failure_penalty)
// {
//   dout(15) << fmt::format(
// 		  "was: nb:{:s}({:s}). Called with: rest?{} {:s} ({})",
// 		  schedule.not_before, schedule.scheduled_at,
// 		  reset_failure_penalty, adjusted.scheduled_at,
// 		  registration_state())
// 	   << dendl;
//   schedule.scheduled_at = adjusted.scheduled_at;
//   schedule.deadline = adjusted.deadline;
// 
//   if (reset_failure_penalty || (schedule.not_before < schedule.scheduled_at)) {
//     schedule.not_before = schedule.scheduled_at;
//   }
// 
//   updated = true;
//   dout(10) << fmt::format(
// 		  "adjusted: nb:{:s} ({:s}) ({})", schedule.not_before,
// 		  schedule.scheduled_at, registration_state())
// 	   << dendl;
// }
// 
// void ScrubJob::delay_on_failure(
//     std::chrono::seconds delay,
//     Scrub::delay_cause_t delay_cause,
//     utime_t scrub_clock_now)
// {
//   schedule.not_before =
//       std::max(scrub_clock_now, schedule.not_before) + utime_t{delay};
//   last_issue = delay_cause;
// }

std::string ScrubJob::scheduling_state(utime_t now_is, bool is_deep_expected)
    const
{
  // if not in the OSD scheduling queues, not a candidate for scrubbing
  if (state != qu_state_t::registered) {
    return "no scrub is scheduled";
  }

  // if the time has passed, we are surely in the queue
  if (now_is > schedule.not_before) {
    // we are never sure that the next scrub will indeed be shallow:
    return fmt::format("queued for {}scrub", (is_deep_expected ? "deep " : ""));
  }

  return fmt::format(
      "{}scrub scheduled @ {:s} ({:s})", (is_deep_expected ? "deep " : ""),
      schedule.not_before, schedule.scheduled_at);
}

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

void ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << schedule.not_before;
  f->dump_stream("orig_sched_time") << schedule.scheduled_at;
  f->dump_stream("deadline") << schedule.deadline;
  f->dump_bool("forced",
	       schedule.scheduled_at == PgScrubber::scrub_must_stamp());
  f->close_section();
}

}  // namespace Scrub
