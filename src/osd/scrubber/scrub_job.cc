// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_job.h"
#include "pg_scrubber.h"

using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using sched_conf_t = Scrub::sched_conf_t;
using scrub_schedule_t = Scrub::scrub_schedule_t;
using ScrubJob = Scrub::ScrubJob;
using delay_ready_t = Scrub::delay_ready_t;

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

using SchedTarget = Scrub::SchedTarget;

void SchedTarget::reset()
{
  // a bit convoluted, but the standard way to guarantee we keep the
  // same set of member defaults as the constructor
  *this = SchedTarget{sched_info.pgid, sched_info.level};
}

bool SchedTarget::over_deadline(utime_t now_is) const
{
  return now_is >= sched_info.schedule.deadline;
}

bool SchedTarget::is_periodic() const
{
  return sched_info.urgency == urgency_t::periodic_regular;
}

void SchedTarget::up_urgency_to(urgency_t u)
{
  sched_info.urgency = std::max(sched_info.urgency, u);
}


void SchedTarget::update_periodic_shallow(
    const Scrub::sched_params_t& suggested, // determine_initial_schedule() makes sense for the shallow target
    const Scrub::sched_conf_t& app_conf,
    //const pg_info_t& pg_info,
    utime_t scrub_clock_now,
    delay_ready_t modify_ready_targets)
{
  ceph_assert(sched_info.level == scrub_level_t::shallow);
  //ceph_assert(!in_queue);



  if (!is_periodic()) {
    // shouldn't be called for high-urgency scrubs
    return;
  }




  if (pg_info.stats.stats_invalid && config.mandatory_on_invalid) {
    sched_info.urgency = urgency_t::must_repair;
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


void SchedTarget::update_periodic_shallow(
    const pg_info_t& pg_info,
    const Scrub::sched_conf_t& config,
    utime_t scrub_clock_now)
{
  ceph_assert(sched_info.level == scrub_level_t::shallow);
  //ceph_assert(!in_queue);

  if (is_high_priority()) {
    // shouldn't be called for high-urgency scrubs
    return;
  }

  if (pg_info.stats.stats_invalid && config.mandatory_on_invalid) {
    sched_info.urgency = urgency_t::must_repair;
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

  // RRR to complete

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
//   sched_info.urgency = (time_now > sched_info.deadline)
// 			   ? urgency_t::overdue
// 			   : urgency_t::periodic_regular;
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
  ceph_assert(!queued);

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


void SchedTarget::push_nb_out(
    std::chrono::seconds delay,
    delay_cause_t delay_cause,
    utime_t scrub_clock_now)
{
  sched_info.not_before =
      std::max(scrub_clock_now, sched_info.not_before) + utime_t{delay};
  last_issue = delay_cause;
}


// // both targets compared are assumed to be 'ripe', i.e. their not_before is
// // in the past
// std::weak_ordering cmp_ripe_targets(
//     const Scrub::SchedTarget& l,
//     const Scrub::SchedTarget& r)
// {
//   return cmp_ripe_entries(l.queued_element(), r.queued_element());
// }
// 
// std::weak_ordering cmp_future_targets(
//     const Scrub::SchedTarget& l,
//     const Scrub::SchedTarget& r)
// {
//   return cmp_ripe_entries(l.queued_element(), r.queued_element());
// }

// 
// std::weak_ordering
// cmp_targets(utime_t t, const Scrub::SchedTarget& l, const Scrub::SchedTarget& r)
// {
//   return cmp_entries(t, l.queued_element(), r.queued_element());
// }


std::string SchedTarget::fmt_print() const {
  return fmt::format("{},q:{:c},ar:{:c},rpr:{:c},issue:{}", sched_info,
                     queued ? '+' : '-', auto_repairing ? '+' : '-',
                     do_repair ? '+' : '-', last_issue);
}

// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

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

ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    : pgid{pg}
    , whoami{node_id}
    , cct{cct}
    , log_msg_prefix{fmt::format("osd.{} scrub-job:pg[{}]:", node_id, pgid)}
{}

// debug usage only
namespace std {
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}
}  // namespace std

void ScrubJob::adjust_shallow_schedule(const Scrub::sched_params_t& suggested,
                                       const Scrub::sched_conf_t& app_conf,
                                       utime_t scrub_clock_now,
                                       delay_ready_t modify_ready_targets) {
  // RRR fix the dout to show just the shallow target
  dout(10) << fmt::format(
                  "{} current h.p.:{:c} conf:{} also-ready?{:c} "
                  "sjob@entry:{}",
                  suggested,
                  shallow_target.sched_info.is_high_priority() ? 'y' : 'n',
                  app_conf,
                  (modify_ready_targets == delay_ready_t::delay_ready) ? 'y'
                                                                       : 'n',
                  *this)
           << dendl;

  auto& sh_times = shallow_target.sched_info.schedule;  // shorthand

  bool high_priority = (suggested.is_must == must_scrub_t::mandatory);
  utime_t adj_not_before = suggested.proposed_time;
  utime_t adj_target = suggested.proposed_time;
  sh_times.deadline = adj_target;

  if (!high_priority) {
    // add a random delay to the proposed scheduled time - but only for periodic
    // scrubs that are not already eligible for scrubbing.
    if ((modify_ready_targets == delay_ready_t::delay_ready) ||
        adj_not_before > scrub_clock_now) {
      adj_target += app_conf.shallow_interval;
      double r = rand() / (double)RAND_MAX;
      adj_target +=
          app_conf.shallow_interval * app_conf.interval_randomize_ratio * r;
    }

    // the deadline can be updated directly into the scrub-job
    if (app_conf.max_shallow) {
      sh_times.deadline += *app_conf.max_shallow;
    } else {
      sh_times.deadline = utime_t{};
    }

    if (adj_not_before < adj_target) {
      adj_not_before = adj_target;
    }
  }

  sh_times.scheduled_at = adj_target;
  sh_times.not_before = adj_not_before;
  dout(10) << fmt::format("adjusted: nb:{:s} target:{:s} deadline:{:s} ({})",
                          sh_times.not_before, sh_times.scheduled_at,
                          sh_times.deadline, state_desc())
           << dendl;
}

void ScrubJob::adjust_schedule(
    const Scrub::sched_params_t& suggested,
    const Scrub::sched_conf_t& app_conf,
    utime_t scrub_clock_now,
    delay_ready_t modify_ready_targets)
{
  dout(10) << fmt::format(
		  "{} current h.p.:{:c} conf:{} also-ready?{:c} "
		  "sjob@entry:{}",
		  suggested, high_priority ? 'y' : 'n', app_conf,
		  (modify_ready_targets == delay_ready_t::delay_ready) ? 'y'
								       : 'n',
		  *this)
	   << dendl;

  high_priority = (suggested.is_must == must_scrub_t::mandatory);
  utime_t adj_not_before = suggested.proposed_time;
  utime_t adj_target = suggested.proposed_time;
  schedule.deadline = adj_target;

  if (!high_priority) {
    // add a random delay to the proposed scheduled time - but only for periodic
    // scrubs that are not already eligible for scrubbing.
    if ((modify_ready_targets == delay_ready_t::delay_ready) ||
	adj_not_before > scrub_clock_now) {
      adj_target += app_conf.shallow_interval;
      double r = rand() / (double)RAND_MAX;
      adj_target +=
	  app_conf.shallow_interval * app_conf.interval_randomize_ratio * r;
    }

    // the deadline can be updated directly into the scrub-job
    if (app_conf.max_shallow) {
      schedule.deadline += *app_conf.max_shallow;
    } else {
      schedule.deadline = utime_t{};
    }

    if (adj_not_before < adj_target) {
      adj_not_before = adj_target;
    }
  }

  schedule.scheduled_at = adj_target;
  schedule.not_before = adj_not_before;
  dout(10) << fmt::format(
		  "adjusted: nb:{:s} target:{:s} deadline:{:s} ({})",
		  schedule.not_before, schedule.scheduled_at, schedule.deadline,
		  state_desc())
	   << dendl;
}


void ScrubJob::merge_and_delay(
    const scrub_schedule_t& aborted_schedule,
    delay_cause_t issue,
    requested_scrub_t updated_flags,
    utime_t scrub_clock_now)
{
  // merge the schedule targets:
  schedule.scheduled_at =
      std::min(aborted_schedule.scheduled_at, schedule.scheduled_at);
  high_priority = high_priority || updated_flags.must_scrub;
  delay_on_failure(5s, issue, scrub_clock_now);

  // the new deadline is the minimum of the two
  schedule.deadline = std::min(aborted_schedule.deadline, schedule.deadline);
}


void ScrubJob::delay_on_failure(
    std::chrono::seconds delay,
    Scrub::delay_cause_t delay_cause,
    utime_t scrub_clock_now)
{
  schedule.not_before =
      std::max(scrub_clock_now, schedule.not_before) + utime_t{delay};
  last_issue = delay_cause;
}

std::string ScrubJob::scheduling_state(utime_t now_is, bool is_deep_expected)
    const
{
  // if not registered, not a candidate for scrubbing on this OSD (or at all)
  if (!registered) {
    return "not registered for scrubbing";
  }
  if (!target_queued) {
    // if not currently queued - we are being scrubbed
    return "scrubbing";
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
