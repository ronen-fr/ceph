// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd/OSD.h"
#include "osd_scrub_sched.h"
#include "scrub_queue.h"
#include "osd/osd_types_fmt.h"

using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;


#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
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

utime_t ScrubQueue::scrub_clock_now() const
{
  return ceph_clock_now();
}

// ////////////////////////////////////////////////////////////////////////// //
// CPU load tracking and related

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
		    "loadavg per cpu {} < max {} = yes", loadavg_per_cpu,
		    conf()->osd_scrub_load_threshold)
	     << dendl;
    return true;
  }

  // allow scrub if below daily avg and currently decreasing
  if (loadavgs[0] < daily_loadavg && loadavgs[0] < loadavgs[2]) {
    dout(20) << fmt::format(
		    "loadavg {} < daily_loadavg {} and < 15m avg {} = yes",
		    loadavgs[0], daily_loadavg, loadavgs[2])
	     << dendl;
    return true;
  }

  dout(20) << fmt::format(
		  "loadavg {} >= max {} and ( >= daily_loadavg {} or >= 15m "
		  "avg {} ) = no",
		  loadavgs[0], conf()->osd_scrub_load_threshold, daily_loadavg,
		  loadavgs[2])
	   << dendl;
  return false;
}


// checks for half-closed ranges. Modify the (p<till)to '<=' to check for
// closed.
static inline bool isbetween_modulo(int64_t from, int64_t till, int p)
{
  // the 1st condition is because we have defined from==till as "always true"
  return (till == from) || ((till >= from) ^ (p >= from) ^ (p < till));
}

bool ScrubQueue::scrub_time_permit() const
{
  utime_t now = scrub_clock_now();
  time_t tt = now.sec();
  tm bdt;
  localtime_r(&tt, &bdt);

  bool day_permit = isbetween_modulo(
      conf()->osd_scrub_begin_week_day, conf()->osd_scrub_end_week_day,
      bdt.tm_wday);
  if (!day_permit) {
    dout(20) << fmt::format(
		    "should run between week day {} - {} now {} - no",
		    conf()->osd_scrub_begin_week_day,
		    conf()->osd_scrub_end_week_day, bdt.tm_wday)
	     << dendl;
    return false;
  }

  bool time_permit = isbetween_modulo(
      conf()->osd_scrub_begin_hour, conf()->osd_scrub_end_hour, bdt.tm_hour);
  dout(20) << fmt::format(
		  "should run between {} - {} now {} = {}",
		  conf()->osd_scrub_begin_hour, conf()->osd_scrub_end_hour,
		  bdt.tm_hour, (time_permit ? "yes" : "no"))
	   << dendl;
  return time_permit;
}

milliseconds ScrubQueue::required_sleep_time(bool high_priority_scrub) const
{
  milliseconds regular_sleep_period =
      milliseconds{int64_t(1000 * conf()->osd_scrub_sleep)};

  if (high_priority_scrub || scrub_time_permit()) {
    return regular_sleep_period;
  }

  // relevant if scrubbing started during allowed time, but continued into
  // forbidden hours
  milliseconds extended_sleep =
      milliseconds{int64_t(1000 * conf()->osd_scrub_extended_sleep)};
  dout(20) << "w/ extended sleep (" << extended_sleep << ")" << dendl;
  return std::max(extended_sleep, regular_sleep_period);
}


// ////////////////////////////////////////////////////////////////////////// //
// queue manipulation - implementing the ScrubQueueOps interface

using QSchedTarget = Scrub::QSchedTarget;
using urgency_t = Scrub::urgency_t;

namespace {

// the 'identification' function for the 'to_scrub' queue
// (would have been a key in a map, where we not sorting the entries
// by different fields)
auto same_key(const QSchedTarget& t, spg_t pgid, scrub_level_t s_or_d)
{
  return t.is_valid && t.pgid == pgid && t.level == s_or_d;
}
auto same_pg(const QSchedTarget& t, spg_t pgid)
{
  return t.is_valid && t.pgid == pgid;
}
}  // namespace


void ScrubQueue::queue_entries(
    spg_t pgid,
    const QSchedTarget& shallow,
    const QSchedTarget& deep)
{
  dout(20) << fmt::format(
		  "{}: pg[{}]: queuing <{}> & <{}>", __func__, pgid, shallow,
		  deep)
	   << dendl;
  ceph_assert(shallow.pgid == pgid && deep.pgid == pgid);
  ceph_assert(shallow.is_valid && deep.is_valid);

  std::unique_lock l{jobs_lock};

  // now - add the new targets
  to_scrub.push_back(shallow);
  to_scrub.push_back(deep);
}

void ScrubQueue::remove_entry(spg_t pgid, scrub_level_t s_or_d)
{
  dout(20) << fmt::format(
		  "{}: removing {}/{} from the scrub-queue", __func__, pgid,
		  s_or_d)
	   << dendl;
  std::unique_lock l{jobs_lock};
  auto i = std::find_if(
      to_scrub.begin(), to_scrub.end(), [pgid, s_or_d](const QSchedTarget& t) {
	return same_key(t, pgid, s_or_d);
      });
  if (i != to_scrub.end()) {
    i->is_valid = false;
  }
}

void ScrubQueue::remove_entries(spg_t pgid, int known_cnt)
{
  dout(20) << fmt::format(
		  "{}: dequeuing pg[{}] ({} entries)", __func__, pgid,
		  known_cnt)
	   << dendl;

  std::unique_lock l{jobs_lock};
  if (known_cnt) {
    for (auto& e : to_scrub) {
      if (same_pg(e, pgid)) {
	e.is_valid = false;
	if (--known_cnt <= 0) {
	  break;
	}
      }
    }
  }
}

void ScrubQueue::cp_and_queue_target(QSchedTarget t)
{
  dout(20) << fmt::format("{}: restoring {} to the scrub-queue", __func__, t)
	   << dendl;
  ceph_assert(t.urgency > urgency_t::off);
  std::unique_lock l{jobs_lock};
  t.is_valid = true;
  to_scrub.push_back(t);
}

void ScrubQueue::dump_scrubs(ceph::Formatter* f)
{
  std::lock_guard lck(jobs_lock);
  normalize_the_queue(scrub_clock_now());

  f->open_array_section("scrubs");
  std::for_each(to_scrub.cbegin(), to_scrub.cend(), [&f](const auto& j) {
    j.dump("sched-target", f);
  });
  f->close_section();
}


// ////////////////////////////////////////////////////////////////////////// //
// initiating a scrub

using ScrubPreconds = Scrub::ScrubPreconds;
using schedule_result_t = Scrub::schedule_result_t;

void ScrubQueue::sched_scrub(
    const ceph::common::ConfigProxy& config,
    bool is_recovery_active)
{
  utime_t scrub_tick_time = scrub_clock_now();
  dout(10) << fmt::format(
		  "time now:{}, is_recovery_active:{}", scrub_tick_time,
		  is_recovery_active)
	   << dendl;

  // do the OSD-wide environment conditions, and the availability of scrub
  // resources, allow us to start a scrub?
  auto maybe_env_cond =
      preconditions_to_scrubbing(config, is_recovery_active, scrub_tick_time);
  if (!maybe_env_cond) {
    return;
  }
  auto preconds = maybe_env_cond.value();

  std::unique_lock l{jobs_lock};

  // partition and sort the queue
  if (bool not_empty = normalize_the_queue(scrub_tick_time); !not_empty) {
    dout(10) << fmt::format(
		  "no eligible scrub targets")
	   << dendl;
    return;
  }

  // pop the first job from the queue, as a candidate
  auto cand = to_scrub.front();
  to_scrub.pop_front();

  l.unlock();

  PgLockWrapper locked_g = osd_service.get_locked_pg(cand.pgid);
  PGRef pg = locked_g.m_pg;
  if (!pg) {
    // the PG was deleted in the sort time since unlocking the queue
    dout(5) << fmt::format("pg[{}] not found", cand.pgid) << dendl;
    return;
  }

  pg->start_scrubbing(scrub_tick_time, cand.level, preconds);
}


tl::expected<ScrubPreconds, schedule_result_t>
ScrubQueue::preconditions_to_scrubbing(
    const ceph::common::ConfigProxy& config,
    bool is_recovery_active,
    utime_t scrub_clock_now)
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
    return tl::unexpected(schedule_result_t::lost_coin_flip);
  }

  // fail fast if no resources are available
  if (!can_inc_scrubs()) {
    dout(10) << __func__ << ": OSD cannot inc scrubs" << dendl;
    return tl::unexpected(schedule_result_t::no_local_resources);
  }

  // if there is a PG that is just now trying to reserve scrub replica resources
  // - we should wait and not initiate a new scrub
  if (is_reserving_now()) {
    dout(20) << __func__ << ": scrub resources reservation in progress"
	     << dendl;
    return tl::unexpected(schedule_result_t::repl_reservation_in_progress);
  }

  Scrub::ScrubPreconds env_conditions;
  env_conditions.time_permit = scrub_time_permit();
  env_conditions.load_is_low = scrub_load_below_threshold();
  env_conditions.only_deadlined =
      !env_conditions.time_permit || !env_conditions.load_is_low;

  if (is_recovery_active && !config->osd_scrub_during_recovery) {
    if (!config->osd_repair_during_recovery) {
      dout(15) << __func__ << ": not scheduling scrubs due to active recovery"
	       << dendl;
      return tl::unexpected(schedule_result_t::recovery_is_active);
    }
    dout(10) << __func__
	     << " will only schedule explicitly requested repair due to active "
		"recovery"
	     << dendl;
    env_conditions.allow_requested_repair_only = true;
  }

  return env_conditions;
}

/**
 * the refactored "OSD::sched_all_scrubs()"
 *
 * Process:
 * - scan the queue for entries that are "periodic"
 * - notify the PGs of those entries, that they should recalculate their
 *   scrub scheduling
 */
void ScrubQueue::on_config_times_change()
{
  std::set<spg_t> handled;
  std::unique_lock l{jobs_lock};

  for (const auto& e : to_scrub) {
    if (e.is_valid && e.urgency == urgency_t::periodic_regular) {
      if (handled.count(e.pgid))
	continue;

      handled.insert(e.pgid);
      dout(15) << fmt::format("resched {} ({})", e.pgid, e.level) << dendl;
      osd_service.send_sched_recalc_to_pg(e.pgid);
    }
  }
}

// void ScrubQueue::on_config_times_change()
// {
//   dout(10) << "starting" << dendl;
//   auto all_jobs = list_registered_jobs();
//   int modified_cnt{0};
//   auto now_is = time_now();
// 
//   for (const auto& [job, lvl] : all_jobs) {
//     auto& trgt = job->get_current_trgt(lvl);
//     dout(20) << fmt::format("examine {} ({})", job->pgid, trgt) << dendl;
// 
//     PgLockWrapper locked_g = osd_service.get_locked_pg(job->pgid);
//     PGRef pg = locked_g.m_pg;
//     if (!pg)
//       continue;
// 
//     if (!pg->is_primary()) {
//       dout(1) << fmt::format("{} is not primary", job->pgid) << dendl;
//       continue;
//     }
// 
//     auto applicable_conf = populate_config_params(pg->get_pgpool().info.opts);
// 
//     /// \todo consider sorting by pool, reducing the number of times we
//     ///       call 'populate_config_params()'
// 
//     if (job->on_periods_change(pg->info, applicable_conf, now_is); true) {
//       dout(10) << fmt::format("{} ({}) - rescheduled", job->pgid, trgt)
// 	       << dendl;
//       ++modified_cnt;
//     }
//     // auto-unlocked as 'locked_g' gets out of scope
//   }
// 
//   dout(10) << fmt::format("{} planned scrubs rescheduled", modified_cnt)
// 	   << dendl;
// }



// ////////////////////////////////////////////////////////////////////////// //
// topic 1

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
  // concerned.

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

  // but seems like our tests require: \todo fix!
  configs.max_deep =
      std::max(configs.max_shallow.value_or(0.0), configs.deep_interval);

  configs.interval_randomize_ratio = conf()->osd_scrub_interval_randomize_ratio;
  // configs.deep_randomize_ratio = conf()->osd_deep_scrub_randomize_ratio;
  configs.mandatory_on_invalid = conf()->osd_scrub_invalid_stats;

  dout(15) << fmt::format("updated config:{}", configs) << dendl;
  return configs;
}



// ////////////////////////////////////////////////////////////////////////// //
// container low-level operations. Will be extracted, and implemented by Sam's

// must be called under the lock
bool ScrubQueue::normalize_the_queue(utime_t scrub_clock_now)
{
  // erase all 'invalid' entries
  to_scrub.erase(
      std::remove_if(
	  to_scrub.begin(), to_scrub.end(),
	  [](const auto& sched_entry) { return !sched_entry.is_valid; }),
      to_scrub.end());

  // partition into 'ripe' and to those not eligible for scrubbing
  auto not_ripe = std::stable_partition(
      to_scrub.begin(), to_scrub.end(),
      [scrub_clock_now](const auto& sched_entry) {
	return sched_entry.is_ripe(scrub_clock_now);
      });

  // sort the 'ripe' entries by their specific criteria
  std::sort(to_scrub.begin(), not_ripe, [](const auto& lhs, const auto& rhs) {
    return cmp_ripe_entries(lhs, rhs) < 0;
  });

  // and those with not-before in the future - mostly by their 'not-before'
  // time
  std::sort(not_ripe, to_scrub.end(), [](const auto& lhs, const auto& rhs) {
    return cmp_future_entries(lhs, rhs) < 0;
  });

  return not_ripe != to_scrub.begin();
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

  dout(20) << fmt::format(
		  "== false. {} (local) + {} (remote) >= max ({})",
		  scrubs_local, scrubs_remote, conf()->osd_max_scrubs)
	   << dendl;
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
	     << " (max " << conf()->osd_max_scrubs << ", local " << scrubs_local
	     << ")" << dendl;
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
