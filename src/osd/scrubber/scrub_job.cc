// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_job.h"
#include "pg_scrubber.h"
#include "osd/OSD.h"

using namespace std::chrono;
using namespace std::chrono_literals;

using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using ScrubJob = Scrub::ScrubJob;

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



#define dout_subsys ceph_subsys_osd
#undef dout_context
#define dout_context (m_scrubber.get_pg_cct())
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn)
{
  return t->gen_prefix(*_dout, fn);
}

// debug usage only
namespace std {
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}
}  // namespace std



namespace Scrub {




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


void SchedTarget::push_nb_out(
    std::chrono::seconds delay,
    delay_cause_t delay_cause,
    utime_t scrub_clock_now)
{
  sched_info.not_before =
      std::max(scrub_clock_now, sched_info.not_before) + utime_t{delay};
  last_issue = delay_cause;
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

ScrubJob::ScrubJob(
    PgScrubber& scrubber,
    const spg_t& pg)
    : m_scrubber{scrubber}
    , m_pgid{pg}
    , m_shallow_target{pg, scrub_level_t::shallow}
    , m_deep_target{pg, scrub_level_t::deep}
    , m_osd_scrub{m_scrubber.m_osds->get_scrub_services()}
    , m_queue{m_scrubber.m_osds->get_scrub_services().get_scrub_queue()}
{}

void ScrubJob::reset()
{
  ceph_assert(!in_queue());
  m_shallow_target.reset();
  m_deep_target.reset();
}


bool ScrubJob::in_queue() const
{
  return m_shallow_target.in_queue || m_deep_target.in_queue;
}

void ScrubJob::mark_target_dequeued(scrub_level_t scrub_level)
{
  auto& trgt = get_target(scrub_level);
  trgt.clear_queued();
}


std::string_view ScrubJob::registration_state() const
{
  return in_queue() ? "in-queue" : "not-queued";
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
  dout(/*25*/ 1) << fmt::format("deep_target: {}", m_deep_target) << dendl;
  const int in_q_count = m_queue.dequeue_pg(m_pgid);
  m_shallow_target.clear_queued();
  m_deep_target.clear_queued();

  // re-calculate both targets as periodic scrubs (unless already
  // set for a higher priority scrub)

  const auto& info = m_scrubber.m_pg->get_pg_info(ScrubberPasskey{});

  if (modify_ready_tarets || !m_shallow_target.is_ripe(scrub_time_now)) {
    m_shallow_target.update_periodic_shallow(info, aconf, scrub_time_now);
    dout(/*25*/ 20) << fmt::format(
			   "shallow_target: {} (rand: {})", m_shallow_target,
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


// int ScrubJob::recalc_periodic_targets(
//     const Scrub::sched_conf_t& aconf,
//     utime_t scrub_time_now)
// {
//   // called with the queue lock held
// 
//   dout(/*25*/ 1) << fmt::format(
//                          "shallow_target: {} (rand: {})", m_shallow_target,
//                          aconf.interval_randomize_ratio)
//                   << dendl;
//   dout(/*25*/ 1) << fmt::format("deep_target: {}", m_deep_target) << dendl;
//   const int in_q_count = m_queue.dequeue_pg(m_pgid);
//   m_shallow_target.clear_queued();
//   m_deep_target.clear_queued();
// 
//   // re-calculate both targets as periodic scrubs (unless already
//   // set for a higher priority scrub)
// 
//   const auto& info = m_scrubber.m_pg->get_pg_info(ScrubberPasskey{});
// 
//   m_shallow_target.update_periodic_shallow(info, aconf, scrub_time_now);
//   dout(/*25*/ 20) << fmt::format(
//                          "shallow_target: {} (rand: {})", m_shallow_target,
//                          aconf.interval_randomize_ratio)
//                   << dendl;
// 
//   m_deep_target.update_periodic_deep(info, aconf, scrub_time_now);
//   dout(/*25*/ 20) << fmt::format("deep_target: {}", m_deep_target) << dendl;
// 
//   m_queue.enqueue_targets(
//       m_pgid, m_shallow_target.queued_element(),
//       m_deep_target.queued_element());
//   m_shallow_target.set_queued();
//   m_deep_target.set_queued();
//   dout(25) << fmt::format(
//                   "{} targets removed from queue; added {} & {}", in_q_count,
//                   m_shallow_target, m_deep_target)
//            << dendl;
// 
//   return in_q_count;
// }

// handling the various cases of 'need to update the periodic scrub targets'

/*
 * Note:
 * - this is the only targets-manipulating function that accepts disabled
 *   (urgency == off) targets;
 */
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

void ScrubJob::on_periods_change(const Scrub::sched_conf_t& aconf, utime_t scrub_time_now)
{
  dout(10) << fmt::format(
		  "before: {} and {}, scrubbing:{}",
		  m_shallow_target, m_deep_target, scrubbing)
	   << dendl;

  std::lock_guard l{m_osd_scrub.get_scrub_queue_lock()};
  if (scrubbing) { // RRR make sure we set this
    // both targets will be updated at the end of the scrub
    return;
  }

  const int in_q_count = m_queue.count_queued(m_pgid);
  if (in_q_count != 2) {
    // we do know that we are 'active primary' - so that means that
    // this PG was selected for scrubbing, but the scrubbing did not
    // start yet (start_scrubbing() was not yet called).
    return;
  }
  const int before_cnt = recalc_periodic_targets(aconf, false, scrub_time_now);
  ceph_assert(before_cnt == 2);
  dout(10) << fmt::format(
		  "updated both targets: {} & {}",
		  m_shallow_target, m_deep_target)
	   << dendl;
}

void ScrubJob::at_scrub_completion(const Scrub::sched_conf_t& aconf, utime_t scrub_time_now)
{
  std::lock_guard l{m_osd_scrub.get_scrub_queue_lock()};
  const int before_cnt = recalc_periodic_targets(aconf, true, scrub_time_now);
  dout(10) << fmt::format(
		  "updated targets: {} & {}",
		  m_shallow_target, m_deep_target)
	   << dendl;
  ceph_assert(before_cnt == 0);
}


// //

void ScrubJob::restore_target(scrub_level_t level)
{
  std::lock_guard l{m_osd_scrub.get_scrub_queue_lock()};

  // verify that the target is indeed not in the queue
  ceph_assert(!get_target(level).is_queued());
  auto trgt_in_queue = m_queue.dequeue_target(m_pgid, level);
  ceph_assert(!trgt_in_queue);

  auto& target = get_target(level);
  if (target.is_off()) {
    // which means that the PG is no longer "scrubable"
    return;
  }
  m_queue.enqueue_target(m_pgid, target.queued_element());
}

#if 0
void ScrubJob::merge_active_back(
    SchedTarget&& aborted_target,
    delay_cause_t issue,
    utime_t now_is)
{
  ceph_assert(scrubbing);
  ceph_assert(!deep_target.in_queue);
  ceph_assert(!shallow_target.in_queue);

  merge_and_delay(
      std::move(aborted_target), issue, 0s, delay_both_targets_t::no, now_is);

  dout(10) << fmt::format("{}: post: {}", __func__, *this) << dendl;
}
#endif



/**
 * mark for a deep-scrub after the current scrub ended with errors.
 * Note that no need to requeue the target, as it will be requeued
 * when the scrub ends.


RRR make sure this really still happens to the 'other target'

 */
void ScrubJob::mark_for_rescrubbing()
{
  std::lock_guard l{m_osd_scrub.get_scrub_queue_lock()};

  const auto clock_now = ceph_clock_now();
  m_deep_target.auto_repairing = true;

  // no need to take existing deep_target contents into account,
  // as the only higher priority is 'after_repair', and we know no
  // repair took place while we were scrubbing.
  m_deep_target.sched_info.target = clock_now;
  m_deep_target.sched_info.not_before = clock_now;
  m_deep_target.sched_info.urgency = urgency_t::must_repair;

  dout(10) << fmt::format(
		  "deep scrub w/ auto-repair required due to scrub errors. Target set to {}",
		  m_deep_target)
	   << dendl;
}

void ScrubJob::mark_for_after_repair()
{
  //dequeue, then manipulate, the deep target
  std::lock_guard l{m_osd_scrub.get_scrub_queue_lock()};

  [[maybe_unused]] /* RRR */ const auto was_there = m_queue.dequeue_target(m_pgid, scrub_level_t::deep);

  // RRR verify that I do not need the data in the removed target
  m_deep_target.sched_info.urgency = urgency_t::after_repair;
  m_deep_target.sched_info.target = {0, 0};
  m_deep_target.sched_info.not_before = ceph_clock_now();

  // requeue
  m_queue.enqueue_target(m_pgid, m_deep_target.queued_element());
  m_deep_target.set_queued();
}


void ScrubJob::remove_from_queue()
{
  auto& m = m_osd_scrub.get_scrub_queue_lock();
  dout(5) << "before targets removed from queue" << dendl; // RRR
  std::lock_guard l{m};

  const int in_q_count = m_queue.dequeue_pg(m_pgid);
  m_shallow_target.clear_queued();
  m_deep_target.clear_queued();
  dout(15) << fmt::format("{} targets removed from queue", in_q_count) << dendl;
}

std::string ScrubJob::alt_fmt_print(bool short_output) const
{
    if (short_output) {
      return fmt::format("pg[{}]:reg:{}", m_pgid, registration_state());
    }
    return fmt::format("pg[{}]:[t/s:{},t/d:{}],reg:{}", m_pgid,
	m_shallow_target, m_deep_target, registration_state());
}

std::ostream& ScrubJob::gen_prefix(
    std::ostream& out,
    std::string_view fn) const
{
  return m_scrubber.gen_prefix(out)
	 << fmt::format("ScrubJob:{}: ", fn);
}


void ScrubJob::operator_forced_targets(
    scrub_level_t level,
    scrub_type_t scrub_type,
    utime_t scrub_time_now)
{
  std::lock_guard l{m_osd_scrub.get_scrub_queue_lock()};

  // the dequeue might fail, as we might be scrubbing that same target now,
  // but that's OK
  std::ignore = m_queue.dequeue_target(m_pgid, level);
  mark_target_dequeued(level);
  if (level == scrub_level_t::shallow) {
    m_shallow_target.set_oper_shallow_target(scrub_type, scrub_time_now);
  } else {
    m_deep_target.set_oper_deep_target(scrub_type, scrub_time_now);
  }
  m_queue.enqueue_target(m_pgid, get_target(level).queued_element());
}


/**
 * Handle a scrub aborted mid-execution.
 * State on entry:
 * - no target is in the queue (both were dequeued when the scrub started);
 * - both 'shallow' & 'deep' targets are valid - set for the next scrub;
 * Process:
 * - merge the failing target with the corresponding 'next' target;
 * - make sure 'not-before' is somewhen in the future;
 * - requeue both targets.
 *
 * \todo use the number of ripe jobs to determine the delay
 */
void ScrubJob::on_abort(
    SchedTarget&& aborted_target,
    delay_cause_t issue,
    utime_t now_is)
{
  std::string delay_config = "osd_scrub_retry_delay";  // the default
  if (issue == delay_cause_t::backend_error) {
    delay_config = "osd_scrub_retry_pg_state";	// no dedicated config option
  }
  const seconds delay = 2s; // must use a small value for testing. RRR
  // RRR    seconds(++consec_aborts * cct->_conf.get_val<int64_t>(delay_config));

  std::lock_guard l{m_osd_scrub.get_scrub_queue_lock()};
  merge_delay_requeue(
      std::move(aborted_target), issue, delay, ScrubJob::delay_both_targets_t::no,
      now_is);
}



void ScrubJob::merge_and_delay(
    SchedTarget&& aborted_target,
    delay_cause_t issue,
    std::chrono::seconds delay,
    ScrubJob::delay_both_targets_t delay_both,
    utime_t now_is)
{
  ceph_assert(scrubbing);
  ceph_assert(!in_queue());

  scrubbing = false;  // RRR is this the place?
  auto& upd_target = get_target(aborted_target.level());
  dout(10) << fmt::format(
		  "pre-abort:{} next:{} delay both targets?{}", aborted_target,
		  upd_target,
		  ((delay_both == ScrubJob::delay_both_targets_t::yes) ? "yes"
								       : "no"))
	   << dendl;

  // merge the targets:
  auto sched_to = std::min(
      aborted_target.queued_element().target,
      upd_target.queued_element().target);
  auto delay_to = now_is + utime_t{delay};

  if (aborted_target.queued_element().urgency >
      upd_target.queued_element().urgency) {
    // maintain all the info from the aborted target
    upd_target = aborted_target;
  }
  upd_target.sched_info.target = sched_to;
  upd_target.sched_info.not_before = delay_to;
  upd_target.last_issue = issue;

  if (delay_both == delay_both_targets_t::yes) {
    auto& second_target = get_target(!aborted_target.level());
    second_target.sched_info.not_before = delay_to;
    second_target.last_issue = issue;
  }
  dout(20) << fmt::format(
		  "post [c.target/base:{}] [c.target/abrtd:{}] {}s delay",
		  upd_target, aborted_target, delay.count())
	   << dendl;
}


void ScrubJob::merge_delay_requeue(
    SchedTarget&& aborted_target,
    delay_cause_t issue,
    std::chrono::seconds delay,
    ScrubJob::delay_both_targets_t delay_both,
    utime_t now_is)
{
  merge_and_delay(std::move(aborted_target), issue, delay, delay_both, now_is);
  m_queue.enqueue_targets(
      m_pgid, m_shallow_target.queued_element(),
      m_deep_target.queued_element());
  m_shallow_target.set_queued();
  m_deep_target.set_queued();

  dout(10) << fmt::format(
		  "post {}s delay -> {}", delay/*.count()*/, *this)
	   << dendl;
}





// logging, dumping, etc.

SchedTarget& ScrubJob::get_target(scrub_level_t lvl)
{
  return (lvl == scrub_level_t::deep) ? m_deep_target : m_shallow_target;
}

SchedTarget ScrubJob::get_moved_target(scrub_level_t s_or_d)
{
  auto& moved_trgt = get_target(s_or_d);
  SchedTarget cp = moved_trgt;
  ceph_assert(!cp.in_queue);
  moved_trgt.reset();
  return cp;
}

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

// note: a static function
std::string ScrubJob::parse_sched_state(const pg_scrubbing_status_t& sched_stat)
{
  const std::string_view lvl_desc =
      (sched_stat.m_is_deep == scrub_level_t::deep ? "deep " : "");
  switch (sched_stat.m_sched_status) {
    case pg_scrub_sched_status_t::not_queued:
    default:
      return "no scrub is scheduled";
    case pg_scrub_sched_status_t::queued:
      return fmt::format("queued for {}scrub", lvl_desc);
    case pg_scrub_sched_status_t::delayed:
      return fmt::format("delayed {}scrub", lvl_desc);
    case pg_scrub_sched_status_t::scheduled:
      return fmt::format(
	  "{}scrub scheduled @ {}", lvl_desc, sched_stat.m_scheduled_at);
  }
}

pg_scrubbing_status_t ScrubJob::get_schedule(utime_t now_is) const
{
  if (!in_queue()) {
    return pg_scrubbing_status_t{
	utime_t{},
	0,
	pg_scrub_sched_status_t::not_queued,
	false,
	scrub_level_t::shallow,
	false};
  }

  const SchedTarget& closest = closest_target(now_is);
  pg_scrubbing_status_t rep_stat{
      closest.get_sched_time(),
      0,  // no relevant value for 'duration'
      pg_scrub_sched_status_t::queued,
      false,  // not scrubbing at this time
      closest.level(),
      closest.is_periodic()};

  // are we ripe for scrubbing?
  if (closest.is_ripe(now_is)) {
    // we are waiting for our turn at the OSD.
    rep_stat.m_sched_status = pg_scrub_sched_status_t::queued;
  } else if (closest.was_delayed()) {
    // were we already delayed once (or more)?
    rep_stat.m_sched_status = pg_scrub_sched_status_t::delayed;
  } else {
    // we are scheduled for scrubbing
    rep_stat.m_sched_status = pg_scrub_sched_status_t::scheduled;
  }
  return rep_stat;
}

utime_t ScrubJob::get_sched_time(utime_t scrub_clock_now) const
{
  //if (!in_queue()) {
  //return utime_t{};
  //}
  return closest_target(scrub_clock_now).get_sched_time();
}

std::string ScrubJob::scheduling_state() const
{
  const auto sched_stat = get_schedule(ceph_clock_now());
  const std::string_view lvl_desc =
      (sched_stat.m_is_deep == scrub_level_t::deep ? "deep " : "");
  switch (sched_stat.m_sched_status) {
    case pg_scrub_sched_status_t::not_queued:
    default:
      return "no scrub is scheduled";
    case pg_scrub_sched_status_t::queued:
      return fmt::format("queued for {}scrub", lvl_desc);
    case pg_scrub_sched_status_t::delayed:
      return fmt::format("delayed {}scrub", lvl_desc);
    case pg_scrub_sched_status_t::scheduled:
      return fmt::format(
	  "{}scrub scheduled @ {}", lvl_desc, sched_stat.m_scheduled_at);
  }
}

}  // namespace Scrub


#if 0

void ScrubJob::update_schedule(const Scrub::scrub_schedule_t& adjusted)
{
  schedule = adjusted;
  penalty_timeout = utime_t(0, 0);  // helps with debugging

  // 'updated' is changed here while not holding jobs_lock. That's OK, as
  // the (atomic) flag will only be cleared by select_pg_and_scrub() after
  // scan_penalized() is called and the job was moved to the to_scrub queue.
  updated = true;
  dout(10) << fmt::format(
		  "adjusted: {:s} ({})", schedule.scheduled_at,
		  registration_state())
	   << dendl;
}

std::string ScrubJob::scheduling_state(utime_t now_is, bool is_deep_expected)
    const
{
  // if not in the OSD scheduling queues, not a candidate for scrubbing
  if (state != qu_state_t::registered) {
    return "no scrub is scheduled";
  }

  // if the time has passed, we are surely in the queue
  // (note that for now we do not tell client if 'penalized')
  if (now_is > schedule.scheduled_at) {
    // we are never sure that the next scrub will indeed be shallow:
    return fmt::format("queued for {}scrub", (is_deep_expected ? "deep " : ""));
  }

  return fmt::format(
      "{}scrub scheduled @ {:s}", (is_deep_expected ? "deep " : ""),
      schedule.scheduled_at);
}

std::ostream& ScrubJob::gen_prefix(std::ostream& out, std::string_view fn) const
{
  return out << m_scrubber.gen_prefix() << "scrub-job:pg[" << pgid << "]:";
}

// clang-format off
// std::string_view ScrubJob::qu_state_text(qu_state_t st)
// {
//   switch (st) {
//     case qu_state_t::not_registered: return "not registered w/ OSD"sv;
//     case qu_state_t::registered: return "registered"sv;
//     case qu_state_t::unregistering: return "unregistering"sv;
//   }
//   // g++ (unlike CLANG), requires an extra 'return' here
//   return "(unknown)"sv;
// }
// clang-format on

void ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << schedule.scheduled_at;
  f->dump_stream("deadline") << schedule.deadline;
  f->dump_bool("forced",
	       schedule.scheduled_at == PgScrubber::scrub_must_stamp());
  f->close_section();
}
#endif

#if 0
/*
 * Note:
 * - this is the only targets-manipulating function that accepts disabled
 *   (urgency == off) targets;
 * - and (partially because of that), here is where we may decide to 'upgrade'
 *   the next shallow scrub to a deep scrub.
 */
void ScrubJob::init_and_queue_targets(
    const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    utime_t scrub_clock_now)
{
  const int in_q_count = dequeue_targets();

  shallow_target.update_as_shallow(info, aconf, scrub_clock_now);
  dout(25) << fmt::format(
		  "{}: shallow_target: {} (rand: {})", __func__, shallow_target,
		  aconf.interval_randomize_ratio)
	   << dendl;
  deep_target.update_as_deep(info, aconf, scrub_clock_now);
  dout(25) << fmt::format("{}: deep_target: {}", __func__, deep_target)
	   << dendl;

  // if 'randomly selected', we will modify the deep target to coincide
  // with the shallow one
  std::string log_as_upgraded = "";
  const bool upgrade_to_deep =
      (in_q_count == 0) && shallow_target.is_periodic() &&
      deep_target.is_periodic() &&
      Scrub::random_bool_with_probability(aconf.deep_randomize_ratio);

  if (upgrade_to_deep && (deep_target.sched_info.not_before >
			  shallow_target.sched_info.not_before)) {
    deep_target.sched_info.target = std::min(
	shallow_target.sched_info.target, deep_target.sched_info.target);
    deep_target.sched_info.not_before = shallow_target.sched_info.not_before;
    shallow_target.sched_info.not_before = add_double(
	shallow_target.sched_info.not_before, aconf.shallow_interval);
    log_as_upgraded = " (upgraded)";
  }

  scrub_queue.enqueue_targets(
      pgid, shallow_target.queued_element(), deep_target.queued_element());
  shallow_target.set_queued();
  deep_target.set_queued();
  dout(10) << fmt::format(
		  "{}: {} targets removed from queue; added {} & {}{}",
		  __func__, in_q_count, shallow_target, deep_target,
		  log_as_upgraded)
	   << dendl;
}
#endif
