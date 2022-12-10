// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./osd_scrub_sched.h"
#include <compare>

#include "osd/OSD.h"

#include "pg_scrubber.h"

using namespace ::std::literals;


/*
dev notes re PG scrub status in the listing

now, more or less:
2022-11-23T14:56:23.227+0200 7efd451f7640 20 osd.1 pg_epoch: 49 pg[1.a( v 47'13 (0'0,47'13] local-lis/les=48/49 n=1 ec=42/42 lis/c=48/48 les/c/f=49/49/0 sis=48) [1,6,2] r=0 lpr=48 crt=47'13 lcod 47'12 mlcod 47'12
 active+clean planned REQ_SCRUB] scrubber<NotActive>: RRR m_active_trgt set
2022-11-23T14:56:23.227+0200 7efd451f7640 20 osd.1 pg_epoch: 49 pg[1.a( v 47'13 (0'0,47'13] local-lis/les=48/49 n=1 ec=42/42 lis/c=48/48 les/c/f=49/49/0 sis=48) [1,6,2] r=0 lpr=48 crt=47'13 lcod 47'12 mlcod 47'12
 active+clean+scrubbing [ 1.a:  REQ_SCRUB ]  planned REQ_SCRUB] prepare_stats_for_publish reporting purged_snaps []

When not scrubbing:
We should only show specific flags if "special", i.e. we have a non-periodic as one of the two
targets.
- OK to use the 'nearest', as if any will be 'must' - it will be;
- not interested in target times etc'.
- are we interested in 'delay reason'? not sure
- urgency - yes
- a/r, rpr, 
- upgradable - yes.




*/





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
using TargetRefW = Scrub::TargetRefW;
using urgency_t = Scrub::urgency_t;
using delay_cause_t = Scrub::delay_cause_t;
using SchedEntry = Scrub::SchedEntry;

namespace {
utime_t add_double(utime_t t, double d)
{
  return utime_t{t.sec() + static_cast<int>(d), t.nsec()};
}
}

/*
 *  NOTE: not used for deciding which target should be scrubbed! (as it does not
 *   take the 'ripeness' into account)
 */
std::partial_ordering SchedTarget::operator<=>(const SchedTarget& r) const
{
  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.urgency <=> urgency; cmp != 0) {
    return cmp;
  }
  // if both have deadline - use it. The earlier is better.
  if (deadline && r.deadline) {
    if (auto cmp = *deadline <=> *r.deadline; cmp != 0) {
      return cmp;
    }
  }
  if (auto cmp = target <=> r.target; cmp != 0) {
    return cmp;
  }
  if (auto cmp = r.auto_repairing <=> auto_repairing; cmp != 0) {
    return cmp;
  }
  if (auto cmp = r.is_deep() <=> is_deep(); cmp != 0) {
    return cmp;
  }
  return not_before <=> r.not_before;
}

/*
 * The ordering is different for ripe and for future jobs. For the former, we
 * sort based on
 *       - urgency
 *       - deadline
 *       - target
 *       - auto-repair
 *       - deep over shallow
 *
 * For the targets that have not reached their not-before time, we sort
 * based on
 *       - not-before
 *       - urgency
 *       - deadline
 *       - target
 *       - auto-repair
 *       - deep over shallow
 *
 * But how do we know which targets are ripe? (without accessing the clock
 * multiple times each time we want to order the targets?)
 * We'd have to keep a separate ephemeral flag for each target, and update it
 * before sorting. And yes - this is not a perfect solution.
 */
std::partial_ordering Scrub::clock_based_cmp(
    const SchedTarget& l,
    const SchedTarget& r)
{
  // NOTE: it is assumed that the 'eph_ripe_for_sort' flag is already set for
  // both targets

  // for 'higher is better' sub elements - the 'r.' is on the left
  if (auto cmp = r.eph_ripe_for_sort <=> l.eph_ripe_for_sort; cmp != 0) {
    return cmp;
  }
  if (l.eph_ripe_for_sort) {
    if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
      return cmp;
    }
    // if both have deadline - use it. The earlier is better.
    if (l.deadline && r.deadline) {
      if (auto cmp = *l.deadline <=> *r.deadline; cmp != 0) {
	return cmp;
      }
    }
    if (auto cmp = l.target <=> r.target; cmp != 0) {
      return cmp;
    }
    if (auto cmp = r.auto_repairing <=> l.auto_repairing; cmp != 0) {
      return cmp;
    }
    if (auto cmp = r.is_deep() <=> l.is_deep(); cmp != 0) {
      return cmp;
    }
    return l.not_before <=> r.not_before;
  }

  // none of the contestants is ripe for scrubbing
  if (auto cmp = l.not_before <=> r.not_before; cmp != 0) {
    return cmp;
  }
  if (auto cmp = r.urgency <=> l.urgency; cmp != 0) {
    return cmp;
  }
  // if both have deadline - use it. The earlier is better.
  if (l.deadline && r.deadline) {
    if (auto cmp = *l.deadline <=> *r.deadline; cmp != 0) {
      return cmp;
    }
  }
  if (auto cmp = l.target <=> r.target; cmp != 0) {
    return cmp;
  }
  if (auto cmp = r.auto_repairing <=> l.auto_repairing; cmp != 0) {
    return cmp;
  }
  return r.is_deep() <=> l.is_deep();
}

// 'dout' defs for SchedTarget
#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix_target(_dout, this)

template <class T>
static ostream& _prefix_target(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}


/**
 * A SchedTarget names both a PG to scrub and the level (deepness) of scrubbing.
 * SchedTarget objects are what the OSD schedules in the scrub queue, then
 * initiates the scrubbing of the PG.
 *
 * The Targets are owned by the ScrubJob (which is shared between the PG and the
 * OSD's scrub queue).
 */
SchedTarget::SchedTarget(
    ScrubJob& owning_job,
    scrub_level_t base_type,
    std::string dbg)
    : pgid{owning_job.pgid}
    , whoami{owning_job.whoami}
    , cct{owning_job.cct}
    , base_target_level{base_type}
    , dbg_val{dbg}
{
  ceph_assert(cct);
  m_log_prefix =
      fmt::format("osd.{} pg[{}] ScrubTrgt: ", whoami, pgid.pgid, dbg_val);
}

std::ostream& SchedTarget::gen_prefix(std::ostream& out) const
{
  return out << m_log_prefix;
}


bool SchedTarget::check_and_redraw_upgrade()
{
  bool current_coin = upgradeable;
  // and redraw for the next time:
  upgradeable =
      (rand() % 100) < (cct->_conf->osd_deep_scrub_randomize_ratio * 100.0);
  return current_coin;
}

void SchedTarget::set_oper_deep_target(scrub_type_t rpr)
{
  ceph_assert(base_target_level == scrub_level_t::deep);
  ceph_assert(!scrubbing);
  if (rpr == scrub_type_t::do_repair) {
    urgency = std::max(urgency_t::must, urgency);
    do_repair = true;
  } else {
    urgency = std::max(urgency_t::operator_requested, urgency);
  }
  target = std::min(ceph_clock_now(), target);
  not_before = std::min(not_before, ceph_clock_now());
  auto_repairing = false;
  last_issue = delay_cause_t::none;
  dout(20) << fmt::format(
		  "{}: repair?{} final:{}", __func__,
		  ((rpr == scrub_type_t::do_repair) ? "+" : "-"), *this)
	   << dendl;
}

void SchedTarget::set_oper_shallow_target(scrub_type_t rpr)
{
  ceph_assert(base_target_level == scrub_level_t::shallow);
  ceph_assert(!scrubbing);
  ceph_assert(rpr != scrub_type_t::do_repair);

  urgency = std::max(urgency_t::operator_requested, urgency);
  target = std::min(ceph_clock_now(), target);
  not_before = std::min(not_before, ceph_clock_now());
  auto_repairing = false;
  last_issue = delay_cause_t::none;
}


using TargetRef = Scrub::TargetRef;

void Scrub::ScrubJob::operator_forced_targets(
    scrub_level_t level,
    scrub_type_t scrub_type)
{
  std::unique_lock l{targets_lock};
  auto& trgt = get_modif_trgt(level);
  if (level == scrub_level_t::shallow) {
    trgt.set_oper_shallow_target(scrub_type);
  } else {
    trgt.set_oper_deep_target(scrub_type);
  }
  determine_closest();
}


void Scrub::ScrubJob::operator_periodic_targets(
    scrub_level_t level,
    utime_t upd_stamp,
    const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    utime_t now_is)
{
  // the 'stamp' was "faked" to trigger a "periodic" scrub.
  std::unique_lock l{targets_lock};
  auto& trgt = get_modif_trgt(level);

  trgt.urgency = std::max(urgency_t::periodic_regular, trgt.urgency);
  if (level == scrub_level_t::shallow) {
    trgt.target = add_double(upd_stamp, aconf.shallow_interval);
    // we do set a deadline for the operator-induced scrubbing. That will
    // allow us to avoid some limiting preconditions.
    trgt.deadline = add_double(
	upd_stamp, aconf.max_shallow.value_or(aconf.shallow_interval));
  } else {
    trgt.target = add_double(upd_stamp, aconf.deep_interval);
    trgt.deadline = add_double(upd_stamp, aconf.deep_interval);
  }
  trgt.not_before = std::min(trgt.not_before, now_is);
  trgt.last_issue = delay_cause_t::none;
  if (now_is > trgt.deadline) {
    trgt.urgency = std::max(urgency_t::overdue, trgt.urgency);
  }
  determine_closest();
}


void SchedTarget::push_nb_out(std::chrono::seconds delay)
{
  not_before = std::max(ceph_clock_now(), not_before) + utime_t{delay};
}

void SchedTarget::push_nb_out(
    std::chrono::seconds delay,
    delay_cause_t delay_cause)
{
  push_nb_out(delay);
  last_issue = delay_cause;
}

void SchedTarget::pg_state_failure()
{
  // if not in a state to be scrubbed (active & clean) - we won't retry it
  // for some time
  push_nb_out(/* RRR conf */ 10s, delay_cause_t::pg_state);
}

void SchedTarget::level_not_allowed()
{
  push_nb_out(3s, delay_cause_t::flags);
}


/// \todo time the delay to a period which is based on the wait for
/// the end of the forbidden hours.
void SchedTarget::wrong_time()
{
  // wrong time / day / load
  // should be 60s: push_nb_out(/* RRR conf */ 60s, delay_cause_t::time);
  // but until we fix the tests (that expect immediate retry) - we'll use 3s
  push_nb_out(3s, delay_cause_t::time);
}

void SchedTarget::on_local_resources()
{
  // too many scrubs on our own OSD. The delay we introduce should be
  // minimal: after all, we expect all other PG tried to fail as well.
  // This should be revisited once we separate the resource-counters for
  // deep and shallow scrubs.
  push_nb_out(2s, delay_cause_t::local_resources);
}

void SchedTarget::dump(std::string_view sect_name, ceph::Formatter* f) const
{
  f->open_object_section(sect_name);
  /// \todo improve the performance of u_time dumps here
  f->dump_stream("pg") << pgid;
  f->dump_stream("base_level")
      << (base_target_level == scrub_level_t::deep ? "deep" : "shallow");
  f->dump_stream("effective_level") << effective_lvl();
  f->dump_stream("urgency") << fmt::format("{}", urgency);
  f->dump_stream("target") << target;
  f->dump_stream("not_before") << not_before;
  f->dump_stream("deadline") << deadline.value_or(utime_t{});
  f->dump_bool("auto_rpr", auto_repairing);
  f->dump_bool("forced", !is_periodic());
  f->dump_stream("last_delay") << fmt::format("{}", last_issue);
  f->close_section();
}


SchedTarget& SchedTarget::operator=(const SchedTarget& r)
{
  ceph_assert(base_target_level == r.base_target_level);
  ceph_assert(pgid == r.pgid);
  // the above also guarantees that we have the same cct
  urgency = r.urgency;
  not_before = r.not_before;
  deadline = r.deadline;
  target = r.target;
  scrubbing = r.scrubbing;  // to reconsider
  deep_or_upgraded = r.deep_or_upgraded;
  last_issue = r.last_issue;
  auto_repairing = r.auto_repairing;
  do_repair = r.do_repair;
  marked_for_dequeue = r.marked_for_dequeue;
  eph_ripe_for_sort = r.eph_ripe_for_sort;
  dbg_val = r.dbg_val;
  return *this;
}

// an aux used by SchedTarget::update_target()
// RRR make sure we are not un-penalizing failed PGs

void SchedTarget::update_as_shallow(
    const pg_info_t& pg_info,
    const Scrub::sched_conf_t& config,
    utime_t time_now)
{
  ceph_assert(base_target_level == scrub_level_t::shallow);
  if (!is_periodic()) {
    // shouldn't be called for high-urgency scrubs
    return;
  }

  if (pg_info.stats.stats_invalid && config.mandatory_on_invalid) {
    urgency = urgency_t::must;
    target = time_now;
    not_before = time_now;
    if (config.max_shallow && *config.max_shallow > 0.1) {
      deadline = add_double(time_now, *config.max_shallow);
    }
  } else {
    auto base = pg_info.stats.stats_invalid ? time_now
					    : pg_info.history.last_scrub_stamp;
    target = add_double(base, config.shallow_interval);
    // if in the past - do not delay. Otherwise - add a random delay
    if (target > time_now) {
      double r = rand() / (double)RAND_MAX;
      target += config.shallow_interval * config.interval_randomize_ratio * r;
    }
    not_before = target;
    urgency = urgency_t::periodic_regular;

    if (config.max_shallow && *config.max_shallow > 0.1) {
      deadline = add_double(target, *config.max_shallow);

      if (time_now > deadline) {
	urgency = urgency_t::overdue;
      }
    }
  }
  last_issue = delay_cause_t::none;

  // prepare the 'upgrade lottery' for when it will be needed (i.e. when
  // we schedule the next shallow scrub)
  std::ignore = check_and_redraw_upgrade();

  // RRR does not match the original logic, but seems to be required
  // for testing (standalone/scrub-test):
  deadline = add_double(target, config.max_deep);
}

void SchedTarget::update_as_deep(
    const pg_info_t& pg_info,
    const Scrub::sched_conf_t& config,
    utime_t time_now)
{
  ceph_assert(base_target_level == scrub_level_t::deep);
  ceph_assert(is_periodic());

  auto base = pg_info.stats.stats_invalid
		  ? time_now
		  : pg_info.history.last_deep_scrub_stamp;

  target = add_double(base, config.deep_interval);
  // if in the past - do not delay. Otherwise - add a random delay
  if (target > time_now) {
    double r = rand() / (double)RAND_MAX;
    target += config.deep_interval * config.interval_randomize_ratio * r;
  }
  not_before = target;
  deadline = add_double(target, config.max_deep);

  urgency =
      (time_now > deadline) ? urgency_t::overdue : urgency_t::periodic_regular;
  auto_repairing = false;
  // so that we can refer to 'upgraded..' for both shallow & deep targets:
  deep_or_upgraded = true;
}


// /////////////////////////////////////////////////////////////////////////
// ScrubJob

#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

template <class T>
static ostream& _prefix(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}

using qu_state_t = Scrub::qu_state_t;
using ScrubJob = Scrub::ScrubJob;

ScrubJob::ScrubJob(CephContext* cct, const spg_t& pg, int node_id)
    : pgid{pg}
    , whoami{node_id}
    , cct{cct}
    , shallow_target{*this, scrub_level_t::shallow, "cs"}
    , deep_target{*this, scrub_level_t::deep, "cd"}
    , closest_target{std::ref(shallow_target)}
    , next_shallow{*this, scrub_level_t::shallow, "ns"}
    , next_deep{*this, scrub_level_t::deep, "nd"}
{
  m_log_msg_prefix = fmt::format("osd.{} pg[{}] ScrubJob: ", whoami, pgid.pgid);
}

// debug usage only
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}

std::ostream& ScrubJob::gen_prefix(std::ostream& out) const
{
  return out << m_log_msg_prefix; 
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
  if (closest_target.get().is_ripe(now_is)) {
    return fmt::format("queued for {}scrub", (is_deep_expected ? "deep " : ""));
  }

  return fmt::format(
      "{}scrub scheduled @ {}",
      (is_deep_expected ? "deep " : ""),  // replace with deep_or_upgraded
      closest_target.get().not_before);
}


bool ScrubJob::verify_targets_disabled() const
{
  return shallow_target.urgency <= urgency_t::off &&
	 deep_target.urgency <= urgency_t::off &&
	 next_shallow.urgency <= urgency_t::off &&
	 next_deep.urgency <= urgency_t::off;
}


/**
 * delay the next time we'll be able to scrub the PG by pushing the
 * not_before time forward.
 * \todo use the number of ripe jobs to determine the delay
 */
void ScrubJob::at_failure(scrub_level_t lvl, delay_cause_t issue)
{
  std::unique_lock l{targets_lock};
  auto& aborted_target = get_current_trgt(lvl);
  dout(15) << fmt::format("{}: pre-abort: {}", __func__, aborted_target)
	   << dendl;

  aborted_target.clear_scrubbing();
  aborted_target.last_issue = issue;
  ++consec_aborts;

  // if there is a 'next' target - it might have higher priority than
  // what was just run. Let's merge the two.

  auto base_lvl = aborted_target.base_target_level;
  if (get_next_trgt(base_lvl).is_viable()) {
    // we already have plans for the next scrub once we manage to finish the
    // one that just failed. The 'N' one is high-priority for sure. The
    // failed one - may be.
    merge_targets(base_lvl, 5s*consec_aborts);
  } else {
    aborted_target.push_nb_out(5s*consec_aborts, issue);
  }
  dout(10) << fmt::format(
		  "{}: post [c.target/base:{}] [c.target/abrtd:{}] {}s delay", __func__,
		  get_current_trgt(base_lvl), get_current_trgt(lvl), 5*consec_aborts)
	   << dendl;
  determine_closest();
}



std::string_view ScrubJob::state_desc() const
{
  return ScrubQueue::qu_state_text(state.load(std::memory_order_relaxed));
}

/*
 * note that we use the 'closest target' to determine what scrub will
 * take place first. THus - we are not interested in the 'urgency' (apart
 * from making sure it's not 'off') of the targets compared.
 * (Note - we should have some logic around 'penalized' urgency, but we
 * don't have it yet)
 */
void ScrubJob::determine_closest()
{
  if (shallow_target.urgency == urgency_t::off) {
    closest_target = std::ref(deep_target);
  } else if (deep_target.urgency == urgency_t::off) {
    closest_target = std::ref(shallow_target);
  } else {
    closest_target = std::ref(shallow_target);
    if (shallow_target.not_before > deep_target.not_before) {
      closest_target = std::ref(deep_target);
    }
  }
}


void ScrubJob::mark_for_dequeue()
{
  // disable scheduling
  shallow_target.urgency = urgency_t::off;
  deep_target.urgency = urgency_t::off;
  next_shallow.urgency = urgency_t::off;
  next_deep.urgency = urgency_t::off;

  // mark for dequeue
  shallow_target.marked_for_dequeue = true;
  deep_target.marked_for_dequeue = true;
  next_shallow.marked_for_dequeue = true;
  next_deep.marked_for_dequeue = true;
}

void ScrubJob::clear_marked_for_dequeue()
{
  shallow_target.marked_for_dequeue = false;
  deep_target.marked_for_dequeue = false;
  next_shallow.marked_for_dequeue = false;
  next_deep.marked_for_dequeue = false;
}

/**
 * get a ref to the selected target of the 'current' set of targets, or - if
 * that target is being scrubbed - the corresponding 'next' target for this PG.
 * Note that racy if not called under the jobs lock
 */
TargetRef ScrubJob::get_modif_trgt(scrub_level_t lvl)
{
  auto& trgt = get_current_trgt(lvl);
  if (trgt.scrubbing) {
    return get_next_trgt(lvl);
  }
  return trgt;
}

TargetRef ScrubJob::get_current_trgt(scrub_level_t lvl)
{
  return (lvl == scrub_level_t::deep) ? deep_target
				      : shallow_target;
}

TargetRef ScrubJob::get_next_trgt(scrub_level_t lvl)
{
  return (lvl == scrub_level_t::deep) ? next_deep
				      : next_shallow;
}


/**
 * mark for a deep-scrub after the current scrub ended with errors.
 */
void ScrubJob::mark_for_rescrubbing()
{
  std::unique_lock l{targets_lock};

  auto& targ = get_modif_trgt(scrub_level_t::deep);
  targ.auto_repairing = true;
  targ.urgency = urgency_t::must;  // no need, I think, to use max(...)
  targ.target = ceph_clock_now();  // replace with time_now()
  targ.not_before = targ.target;
  determine_closest();

  // fix scrub-job dout!
#ifdef NOTYET
  dout(10) << fmt::format(
		  "{}: need deep+a.r. after scrub errors. Target set to {}",
		  __func__, deep_target->target)
	   << dendl;
#endif
}


void ScrubJob::merge_targets(scrub_level_t lvl, std::chrono::seconds delay)
{
  // must not! std::unique_lock l{targets_lock};

  auto delay_to = ceph_clock_now() + utime_t{delay};
  auto& c_target = get_current_trgt(lvl);
  auto& n_target = get_next_trgt(lvl);

  c_target.auto_repairing = c_target.auto_repairing || n_target.auto_repairing;
  if (n_target > c_target) {
    // use the next target's urgency - but modify NB.
    c_target = n_target;
  } else {
    // we will retry this target (unchanged, save for the 'no-before' field)
    // should we also cancel the next scrub?
  }
  c_target.not_before = delay_to;
  n_target.urgency = urgency_t::off;
  c_target.scrubbing = false;
}


/*
  - called after the last-stamps were updated;
  - not 'active' or 'q/a' anymore!
  - note that we may already have 'next' targets, which should be
    merged (they would probably (check) have higher urgency)

 later on:
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
    utime_t time_now)
{
  std::unique_lock l{targets_lock};

  // the job's shallow target
  if (auto& trgt = get_modif_trgt(scrub_level_t::shallow); trgt.is_periodic()) {
    if (trgt.urgency == urgency_t::penalized) {
      trgt.urgency = urgency_t::periodic_regular;
    }
    trgt.update_as_shallow(pg_info, aconf, time_now);
  }

  // the job's deep target
  if (auto& trgt = get_modif_trgt(scrub_level_t::deep); trgt.is_periodic()) {
    if (trgt.urgency == urgency_t::penalized) {
      trgt.urgency = urgency_t::periodic_regular;
    }
    trgt.update_as_deep(pg_info, aconf, time_now);
  }

  // if, by chance, the 'd' target is determined to be very shortly
  // after the 's' target - we'll "merge" them
  //   if (false /* for now */ && d_targ.target > s_targ.target &&
  //       d_targ.target < (s_targ.target + utime_t(10s))) {
  //     // better have a deep scrub
  //     s_targ.deep_or_upgraded = true;
  //   }

  determine_closest();
}

/*
from 20.11.22: Difference from initial_shallow_target():

- the handling of 'must_scrub' & 'need_auto' - that can be removed,
  as we will not be handling high-priority existing entries here;

- the interval_randomized_ratio that is added to shallow target in
  initial_shallow_target() is not added here; Should either be added
  in both, or have a parameter to control this.

- there seems to be a confusion in that we add the 'interval_randomized_ratio'
  to the 'not_before' time, instead of to the 'not_before' time. This is not
  consistent with the original code (I think).

- 'check_and_redraw should only be called here (although currently it is
  called in both places).

- the 'last_issue' is not updated here. Should it be?

- the 'overdue' is not updated here. Should it be?

and, btw:
- shouldn't we move ScrubQueue::set_initial_target() to ScrubJob?
*/



// should we use '<'overdue instead of '<='?
static bool to_change_on_conf(urgency_t u)
{
  return (u > urgency_t::off) && (u < urgency_t::overdue);
}

/*
Following a change in the 'scrub period' parameters -
recomputing the targets:
- won't affect 'must' targets;
- maybe: won't *delay* targets that were already tried and failed (have a failure reason)
- should it affect ripe jobs?


*/
bool ScrubJob::on_periods_change(
    const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    utime_t now_is)
{
  std::unique_lock l{targets_lock};
  // note: is_primary() was verified by the caller

  // we are not interested in currently running jobs. Those will either
  // have their targets updated based on up-to-date stamps and conf when done,
  // or already have a 'next' target with a higher urgency
  bool something_changed{false};

  // the job's shallow target
  if (auto& trgt = get_modif_trgt(scrub_level_t::shallow);
      to_change_on_conf(trgt.urgency)) {
    trgt.update_as_shallow(info, aconf, now_is);
    something_changed = true;
  }

  // the job's deep target
  if (auto& trgt = get_modif_trgt(scrub_level_t::deep);
      to_change_on_conf(trgt.urgency)) {
    trgt.update_as_deep(info, aconf, now_is);
    something_changed = true;
  }

  if (something_changed) {
    determine_closest();
  }
  return something_changed;
}

/**
 * \todo set_initial_targets() is now almost identical to at_scrub_completion().
 *   Consider merging them.
 */
void ScrubJob::set_initial_targets(
    const pg_info_t& info,
    const Scrub::sched_conf_t& aconf,
    utime_t time_now)
{
  std::unique_lock l{targets_lock};
  // is_primary() was verified by the caller

  // we are not interested in currently running jobs. Those will either
  // have their targets updated based on up-to-date stamps and conf when done,
  // or already have a 'next' target with a higher urgency
  bool something_changed{false};

  // the job's shallow target
  if (auto& trgt = get_modif_trgt(scrub_level_t::shallow); trgt.is_periodic()) {
    if (trgt.urgency == urgency_t::penalized) {
      trgt.urgency = urgency_t::periodic_regular;
    }
    trgt.update_as_shallow(info, aconf, time_now);
    something_changed = true;
  }

  // the job's deep target
  if (auto& trgt = get_modif_trgt(scrub_level_t::deep); trgt.is_periodic()) {
    if (trgt.urgency == urgency_t::penalized) {
      trgt.urgency = urgency_t::periodic_regular;
    }
    trgt.update_as_deep(info, aconf, time_now);
    something_changed = true;
  }

  if (something_changed) {
    determine_closest();
  }
}


void ScrubJob::un_penalize(utime_t now_is)
{
  auto per_trgt = [now_is, this](scrub_level_t lvl) mutable -> void {
    auto& trgt = get_modif_trgt(lvl);
    if (trgt.urgency == urgency_t::penalized) {
      // restored to either 'overdue' or 'periodic_regular'
      if (trgt.over_deadline(now_is)) {
	trgt.urgency = urgency_t::overdue;
      } else {
	trgt.urgency = urgency_t::periodic_regular;
      }
    }
  };

  std::unique_lock l{targets_lock};
  per_trgt(scrub_level_t::deep);
  per_trgt(scrub_level_t::shallow);

  penalized = false;
  determine_closest();
}

// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueue

#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix                                                            \
  *_dout << "osd." << osd_service.get_nodeid() << " scrub-queue::" << __func__ \
	 << " "


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

  // RRR but seems like our tests require:
  configs.max_deep =
      std::max(configs.max_shallow.value_or(0.0), configs.deep_interval);

  configs.interval_randomize_ratio = conf()->osd_scrub_interval_randomize_ratio;
  // configs.deep_randomize_ratio = conf()->osd_deep_scrub_randomize_ratio;
  configs.mandatory_on_invalid = conf()->osd_scrub_invalid_stats;

  dout(15) << fmt::format("updated config:{}", configs) << dendl;
  return configs;
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
  auto ret = scrub_job->state.compare_exchange_strong(
      expected_state, Scrub::qu_state_t::unregistering);

  if (ret) {

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
  dout(20) << fmt::format(
		  "pg[{}] state at entry: <{:.14}>", scrub_job->pgid,
		  qu_state_text(state_at_entry))
	   << dendl;
  scrub_job->clear_marked_for_dequeue();

  switch (state_at_entry) {
    case qu_state_t::registered:
      // just updating the schedule? not thru here!
      // update_job(scrub_job, suggested);
      break;

    case qu_state_t::not_registered:
      // insertion under lock
      {
	std::unique_lock lck{jobs_lock};

	if (state_at_entry != scrub_job->state) {
	  lck.unlock();
	  dout(5) << " scrub job state changed. Retrying." << dendl;
	  // retry
	  register_with_osd(scrub_job);
	  break;
	}

	scrub_job->in_queues = true;
	scrub_job->state = qu_state_t::registered;

	ceph_assert(
	    scrub_job->get_current_trgt(scrub_level_t::shallow).urgency >
	    urgency_t::off);
	ceph_assert(
	    scrub_job->get_current_trgt(scrub_level_t::deep).urgency >
	    urgency_t::off);
	to_scrub.emplace_back(
	    Scrub::SchedEntry{scrub_job, scrub_level_t::shallow});
	to_scrub.emplace_back(
	    Scrub::SchedEntry{scrub_job, scrub_level_t::deep});
      }
      break;

    case qu_state_t::unregistering:
      // restore to the queue
      {
	// must be under lock, as the job might be removed from the queue
	// at any minute
	std::lock_guard lck{jobs_lock};

	if (scrub_job->state == qu_state_t::not_registered) {
	  dout(5) << " scrub job state was already 'not registered'" << dendl;
	  to_scrub.emplace_back(
	      Scrub::SchedEntry{scrub_job, scrub_level_t::shallow});
	  to_scrub.emplace_back(
	      Scrub::SchedEntry{scrub_job, scrub_level_t::deep});

	} else {
	  // we expect to still be able to find the targets in the queue
	  auto found_in_q = [this, &scrub_job](scrub_level_t lvl) -> bool {
	    auto& trgt = scrub_job->get_current_trgt(lvl);
	    auto i = std::find_if(
		to_scrub.begin(), to_scrub.end(),
		[trgt, lvl](const SchedEntry& e) {
		  return e.job->pgid == trgt.pgid && e.s_or_d == lvl;
		});
	    return (i != to_scrub.end());
	  };
	  // the shallow/deep targets shouldn't have been removed from the
	  // queues
	  ceph_assert(found_in_q(scrub_level_t::shallow));
	  ceph_assert(found_in_q(scrub_level_t::deep));
	}
	scrub_job->in_queues = true;
	scrub_job->state = qu_state_t::registered;

	break;
      }
  }

  dout(10) << fmt::format(
		  "pg[{}] sched-state changed from {} to {} at (nb): {:s}",
		  scrub_job->pgid, qu_state_text(state_at_entry),
		  qu_state_text(scrub_job->state),
		  scrub_job->closest_target.get().not_before)
	   << dendl;
}


// the refactored "OSD::sched_all_scrubs()"
void ScrubQueue::on_config_times_change()
{
  dout(10) << "starting" << dendl;
  auto all_jobs = list_registered_jobs();
  int modified_cnt{0};
  auto now_is = time_now();

  for (const auto& [job, lvl] : all_jobs) {
    auto& trgt = job->get_current_trgt(lvl);
    dout(20) << fmt::format("examine {} ({})", job->pgid, trgt) << dendl;

    PgLockWrapper locked_g = osd_service.get_locked_pg(job->pgid);
    PGRef pg = locked_g.m_pg;
    if (!pg)
      continue;

    if (!pg->is_primary()) {
      dout(1) << fmt::format("{} is not primary", job->pgid) << dendl;
      continue;
    }

    auto applicable_conf = populate_config_params(pg->get_pgpool().info.opts);

    /// \todo consider sorting by pool, reducing the number of times we
    ///       call 'populate_config_params()'

    if (job->on_periods_change(pg->info, applicable_conf, now_is)) {
      dout(10) << fmt::format("{} ({}) - rescheduled", job->pgid, trgt)
	       << dendl;
      ++modified_cnt;
    }
    // auto-unlocked as 'locked_g' gets out of scope
  }

  dout(10) << fmt::format("{} planned scrubs rescheduled", modified_cnt)
	   << dendl;
}


// used under jobs_lock
void ScrubQueue::move_failed_pgs(utime_t now_is)
{
  // determine the penalty time, after which the job should be reinstated
  utime_t after = now_is;
  after += conf()->osd_scrub_sleep * 2 + utime_t{300'000ms};
  int punished_cnt{0};	// for log/debug only

  const auto per_trgt = [](Scrub::ScrubJobRef job,
			   scrub_level_t lvl) mutable -> void {
    auto& trgt = job->get_modif_trgt(lvl);
    if (trgt.is_periodic()) {
      trgt.urgency = urgency_t::penalized;
    }
    // and even if the target has high priority - still no point in retrying
    // too soon
    trgt.push_nb_out(5s);
  };

  for (auto& [job, lvl] : to_scrub) {
    // note that we will encounter the same PG (the same ScrubJob object) twice
    // - one for the shallow target and one for the deep one. But we handle both
    // targets on the first encounter - and reset the resource_failure flag.
    if (job->resources_failure) {
      per_trgt(job, scrub_level_t::deep);
      per_trgt(job, scrub_level_t::shallow);
      job->penalty_timeout = after;
      job->penalized = true;
      job->resources_failure = false;
      punished_cnt++;
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
std::string_view ScrubQueue::attempt_res_text(Scrub::schedule_result_t v)
{
  switch (v) {
    case Scrub::schedule_result_t::scrub_initiated: return "scrubbing"sv;
    case Scrub::schedule_result_t::none_ready: return "no ready job"sv;
    case Scrub::schedule_result_t::no_local_resources: return "local resources shortage"sv;
    case Scrub::schedule_result_t::already_started: return "denied as already started"sv;
    case Scrub::schedule_result_t::no_such_pg: return "pg not found"sv;
    case Scrub::schedule_result_t::bad_pg_state: return "prevented by pg state"sv;
    case Scrub::schedule_result_t::preconditions: return "not allowed"sv;
  }
  // g++ (unlike CLANG), requires an extra 'return' here
  return "(unknown)"sv;
}

/// \todo replace with the fmt::format version
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
    dout(10) << fmt::format(
		    "{}: PGs are blocked while scrubbing due to locked objects "
		    "({} PGs)",
		    __func__, blocked_pgs)
	     << dendl;
  }

  // sometimes we just skip the scrubbing
  if ((rand() / (double)RAND_MAX) < config->osd_scrub_backoff_ratio) {
    dout(20) << fmt::format(
		    ": lost coin flip, randomly backing off (ratio: {:f})",
		    config->osd_scrub_backoff_ratio)
	     << dendl;
    // return;
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

  // update the ephemeral 'consider as ripe for sorting' for all targets
  for (auto now = time_now(); auto& e : to_scrub) {
    e.target().update_ripe_for_sort(now);
  }

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    dout(20) << "starts" << dendl;
    auto all_jobs = list_registered_jobs();
    std::sort(all_jobs.begin(), all_jobs.end());
    for (const auto& [j, lvl] : all_jobs) {
      dout(20) << fmt::format(
		      "jobs: [{:s}] <<target: {}>>", *j,
		      j->get_current_trgt(lvl))
	       << dendl;
    }
  }

  auto was_started = select_pg_and_scrub(env_conditions);
  dout(20) << " done (" << ScrubQueue::attempt_res_text(was_started) << ")"
	   << dendl;
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
  if (to_scrub.empty()) {
    // let's save a ton of unneeded log messages
    dout(10) << "OSD has no PGs as primary" << dendl;
    return Scrub::schedule_result_t::none_ready;
  }
  dout(10) << fmt::format("jobs#:{} preconds: {}", to_scrub.size(), preconds)
	   << dendl;

  utime_t now_is = time_now();
  preconds.time_permit = scrub_time_permit(now_is);
  preconds.load_is_low = scrub_load_below_threshold();
  preconds.only_deadlined = !preconds.time_permit || !preconds.load_is_low;

  //  create a list of candidates (copying, as otherwise creating a deadlock):
  //  - remove invalid jobs (were marked as such when we lost our Primary role)
  //  - possibly restore penalized
  //  - create a copy of the ripe jobs

  std::unique_lock lck{jobs_lock};
  rm_unregistered_jobs();

  // pardon all penalized jobs that have deadlined
  scan_penalized(restore_penalized, now_is);
  restore_penalized = false;

  // change the urgency of all jobs that failed to reserve resources
  move_failed_pgs(now_is); // RRR verify that still needed

  //  collect all valid & ripe jobs. Note that we must copy,
  //  as when we use the lists we will not be holding jobs_lock (see
  //  explanation above)

  auto to_scrub_copy = collect_ripe_jobs(to_scrub, now_is);
  lck.unlock();

  return select_n_scrub(to_scrub_copy, preconds, now_is);
}

// must be called under jobs lock
void ScrubQueue::rm_unregistered_jobs()
{
  std::for_each(to_scrub.begin(), to_scrub.end(), [](auto& trgt) {
    // 'trgt' is one of the two entries belonging to a single job (single PG)
    if (trgt.job->state == qu_state_t::unregistering) {
      trgt.job->in_queues = false;
      trgt.job->state = qu_state_t::not_registered;
      trgt.job->mark_for_dequeue();
    } else if (trgt.job->state == qu_state_t::not_registered) {
      trgt.job->in_queues = false;
    }
  });

  to_scrub.erase(
    std::remove_if(
      to_scrub.begin(), to_scrub.end(),
      [](const auto& sched_entry) {
	return sched_entry.target().marked_for_dequeue;
      }),
    to_scrub.end());
}


// called under jobs lock
ScrubQueue::SchedulingQueue ScrubQueue::collect_ripe_jobs(
    SchedulingQueue& all_q,
    utime_t time_now)
{
  /// \todo consider rotating the list based on 'ripeness', then sorting only
  /// the ripe entries, and returning the first N entries.

  // copy ripe jobs
  ScrubQueue::SchedulingQueue ripes;
  ripes.reserve(all_q.size());

  std::copy_if(
      all_q.begin(), all_q.end(), std::back_inserter(ripes),
      [time_now](const auto& trgt) -> bool {
	return trgt.target().is_ripe(time_now) && !trgt.is_scrubbing();
      });
  std::sort(ripes.begin(), ripes.end());

  if (g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
    std::sort(all_q.begin(), all_q.end(), [](const auto& a, const auto& b) {
      return clock_based_cmp(a.target(), b.target()) ==
	     std::partial_ordering::less;
    });

    for (const auto& trgt : all_q) {
      if (!trgt.target().is_ripe(time_now)) {
	dout(20) << fmt::format(
			" not ripe: {} @ {} ({})", trgt.job->pgid,
			trgt.target().not_before, trgt.target())
		 << dendl;
      }
    }
  }

  // only if there are no ripe jobs, and as a help to the
  // readers of the log - sort the entire list
  if (ripes.empty()) {
    clock_based_sort(time_now);
  }

  return ripes;
}

Scrub::schedule_result_t ScrubQueue::select_n_scrub(
    SchedulingQueue& group,
    const Scrub::ScrubPreconds& preconds,
    utime_t now_is)
{
  dout(15) << fmt::format(
		  "ripe jobs #:{}. Preconds: {}", group.size(), preconds)
	   << dendl;

  for (auto& candidate : group) {
    // we expect the first job in the list to be a good candidate (if any)

    auto pgid = candidate.job->pgid;
    auto& trgt = candidate.target();
    dout(10) << fmt::format(
		    "initiating a scrub for pg[{}] ({}) [preconds:{}]", pgid,
		    trgt, preconds)
	     << dendl;

    // should we take 'urgency' (i.e. is_periodic()) into account here?
    // Current code does not. Should it?
    if (preconds.only_deadlined && trgt.is_periodic() &&
	!trgt.over_deadline(now_is)) {
      dout(15) << " not scheduling scrub for " << pgid << " due to "
	       << (preconds.time_permit ? "high load" : "time not permitting")
	       << dendl;
      trgt.wrong_time();
      continue;
    }

    // are we left with only penalized jobs?
    if (trgt.urgency == urgency_t::penalized) {
      dout(15) << " only penalized jobs left. Pardoning them all" << dendl;
      restore_penalized = true;
    }

    // lock the PG. Then - try to initiate a scrub. The PG might have changed,
    // or the environment may prevent us from scrubbing now.

    PgLockWrapper locked_g = osd_service.get_locked_pg(pgid);
    PGRef pg = locked_g.m_pg;
    if (!pg) {
      // the PG was dequeued in the short time span between creating the
      // candidates list (collect_ripe_jobs()) and here
      dout(5) << fmt::format("pg[{}] not found", pgid) << dendl;
      continue;
    }

    // Skip other kinds of scrubbing if only explicitly-requested repairing is
    // allowed
    // if (allow_requested_repair_only && !pg->get_planned_scrub().must_repair)
    // RRR verify the condition below
    if (preconds.allow_requested_repair_only && !trgt.do_repair) {
      dout(10) << __func__ << " skip " << pgid
	       << " because repairing is not explicitly requested on it"
	       << dendl;
      dout(20) << "failed (state/cond) " << pgid << dendl;
      trgt.pg_state_failure();
      continue;
    }

    auto scrub_attempt = pg->start_scrubbing(candidate);
    switch (scrub_attempt) {
      case Scrub::schedule_result_t::scrub_initiated:
	// the happy path. We are done
	dout(20) << "initiated for " << pgid << dendl;
	trgt.last_issue = delay_cause_t::none;
	return Scrub::schedule_result_t::scrub_initiated;

      case Scrub::schedule_result_t::already_started:
	// continue with the next job
	dout(20) << "already started " << pgid << dendl;
	continue;

      case Scrub::schedule_result_t::preconditions:
	// shallow scrub but shallow not allowed, or the same for deep scrub.
	// continue with the next job
	dout(20) << "failed (level not allowed) " << pgid << dendl;
	trgt.level_not_allowed();
	continue;

      case Scrub::schedule_result_t::bad_pg_state:
	// continue with the next job
	dout(20) << "failed (state/cond) " << pgid << dendl;
	trgt.pg_state_failure();
	continue;

      case Scrub::schedule_result_t::no_local_resources:
	// failure to secure local resources. No point in trying the other
	// PGs at this time. Note that this is not the same as replica resources
	// failure!
	dout(20) << "failed (local) " << pgid << dendl;
	trgt.on_local_resources();
	return Scrub::schedule_result_t::no_local_resources;

      case Scrub::schedule_result_t::none_ready:
      case Scrub::schedule_result_t::no_such_pg:
	// can't happen. Just for the compiler.
	dout(0) << "failed !!! " << pgid << dendl;
	return Scrub::schedule_result_t::none_ready;
    }
  }

  dout(20) << "returning 'none ready'" << dendl;
  return Scrub::schedule_result_t::none_ready;
}

double ScrubQueue::scrub_sleep_time(bool is_mandatory) const
{
  double regular_sleep_period = conf()->osd_scrub_sleep;

  if (is_mandatory || scrub_time_permit(time_now())) {
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

void ScrubQueue::scan_penalized(bool forgive_all, utime_t time_now)
{
  dout(20) << fmt::format("{}: forgive_all: {}", __func__, forgive_all)
	   << dendl;
  ceph_assert(ceph_mutex_is_locked(jobs_lock));

  for (auto& candidate : to_scrub) {
    // for now - assuming we can read-access the urgency w/o a lock:
    // std::unique_lock l(candidate.job->targets_lock);
    if (candidate.target().urgency == urgency_t::penalized) {
      if (forgive_all || candidate.target().deadline < time_now) {
	// un_penalize() locks the targets_lock
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

// part of the 'scrubber' section in the dump_scrubber()
void ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scheduling");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << get_sched_time();
  auto& nearest = closest_target.get();
  f->dump_stream("deadline")
    << nearest.deadline.value_or(utime_t{});
  // the closest target, following by it and the 2'nd target (we
  // do not bother to order those)
  nearest.dump("nearest", f);
  shallow_target.dump("shallow_target", f);
  deep_target.dump("deep_target", f);
  f->dump_bool("forced", !nearest.is_periodic());
  f->close_section();
}

void ScrubQueue::dump_scrubs(ceph::Formatter* f)
{
  // note: for compat sake, we dump both the 'old' and the 'new' formats:
  // old format: a list of PGs - each with one target & deadline;
  //    which means that:
  //    - we must sort the list by deadline (for all 'not ripe')
  // new format: a list of RRR

  auto now = time_now();
  std::lock_guard lck(jobs_lock);
  clock_based_sort(now);

  f->open_array_section("scrubs");
  std::for_each(to_scrub.cbegin(), to_scrub.cend(), [&f](const auto& j) {
    j.target().dump("sched-target", f);
  });
  f->close_section();
}

ScrubQueue::SchedulingQueue ScrubQueue::list_registered_jobs() const
{
  ScrubQueue::SchedulingQueue all_targets;
  all_targets.reserve(to_scrub.size());
  dout(20) << " size: " << all_targets.capacity() << dendl;
  std::lock_guard lck{jobs_lock};

  std::copy(to_scrub.cbegin(),
	       to_scrub.cend(),
	       std::back_inserter(all_targets));

  return all_targets;
}

void ScrubQueue::clock_based_sort(utime_t now_is)
{
  // update the ephemeral 'consider as ripe for sorting' for all targets
  for (auto& e : to_scrub) {
    e.target().update_ripe_for_sort(now_is);
  }
  std::sort(to_scrub.begin(), to_scrub.end(), [](const auto& a, const auto& b) {
    return clock_based_cmp(a.target(), b.target()) ==
	   std::partial_ordering::less;
  });
}

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
