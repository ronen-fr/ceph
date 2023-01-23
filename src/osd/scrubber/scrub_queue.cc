// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "osd/OSD.h"
#include "osd/osd_types_fmt.h"

#include "osd_scrub_sched.h"
#include "scrub_queue.h"

using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;

#define dout_context (cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix_target(_dout, this)

template <class T>
static ostream& _prefix_target(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}

ScrubQueue::ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds)
    : cct{cct}
    , osd_service{osds}
    , m_osd_resources{[this](std::string msg) { log_fwd(msg); }, cct->_conf}
    , m_queue_impl{std::make_unique<ScrubQueueImp>(*this /*cct, osds*/)}
{
  log_prefix = fmt::format("osd.{} scrub-queue::", osd_service.get_nodeid());

  // initialize the daily loadavg with current 15min loadavg
  if (double loadavgs[3]; getloadavg(loadavgs, 3) == 3) {
    daily_loadavg = loadavgs[2];
  } else {
    derr << "OSD::init() : couldn't read loadavgs\n" << dendl;
    daily_loadavg = 1.0;
  }
}

std::ostream& ScrubQueue::gen_prefix(std::ostream& out) const
{
  return out << log_prefix;
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
		    "{}: should run between week day {} - {} now {} - no",
		    __func__, conf()->osd_scrub_begin_week_day,
		    conf()->osd_scrub_end_week_day, bdt.tm_wday)
	     << dendl;
    return false;
  }

  bool time_permit = isbetween_modulo(
      conf()->osd_scrub_begin_hour, conf()->osd_scrub_end_hour, bdt.tm_hour);
  dout(20) << fmt::format(
		  "{}: should run between {} - {} now {} = {}", __func__,
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
  dout(20)
      << fmt::format(
	     "{}: scrubbing started during allowed time, but continued into "
	     "forbidden hours. regular_sleep_period {} extended_sleep {}",
	     __func__, regular_sleep_period, extended_sleep)
      << dendl;
  return std::max(extended_sleep, regular_sleep_period);
}


// ////////////////////////////////////////////////////////////////////////// //
// queue manipulation - implementing the ScrubQueueOps interface

using SchedEntry = Scrub::SchedEntry;
using urgency_t = Scrub::urgency_t;

namespace {

// the 'identification' function for the 'to_scrub' queue
// (would have been a key in a map, where we not sorting the entries
// by different fields)
auto same_key(const SchedEntry& t, spg_t pgid, scrub_level_t s_or_d)
{
  return t.is_valid && t.pgid == pgid && t.level == s_or_d;
}
}  // namespace


bool ScrubQueue::queue_entries(spg_t pgid, SchedEntry shallow, SchedEntry deep)
{
  dout(20) << fmt::format(
		  "{}: pg[{}]: queuing <{}> & <{}>", __func__, pgid, shallow,
		  deep)
	   << dendl;
  ceph_assert(shallow.pgid == pgid && deep.pgid == pgid);

  if (shallow.urgency == urgency_t::off || deep.urgency == urgency_t::off) {
    dout(20) << fmt::format(
		    "{}: pg[{}]: one of the entries is 'off' - not queuing",
		    __func__, pgid)
	     << dendl;
    return false;
  }

  shallow.is_valid = true;
  deep.is_valid = true;

  std::unique_lock l{jobs_lock};
  // now - add the new targets
  m_queue_impl->push_entry(shallow);
  m_queue_impl->push_entry(deep);
  return true;
}

void ScrubQueue::remove_entry(spg_t pgid, scrub_level_t s_or_d)
{
  dout(20) << fmt::format(
		  "{}: removing {}/{} from the scrub-queue", __func__, pgid,
		  s_or_d)
	   << dendl;
  std::unique_lock l{jobs_lock};
  std::ignore = m_queue_impl->remove_entry(pgid, s_or_d);
}


void ScrubQueue::cp_and_queue_target(SchedEntry t)
{
  dout(20) << fmt::format("{}: restoring {} to the scrub-queue", __func__, t)
	   << dendl;
  ceph_assert(t.urgency > urgency_t::off);
  std::unique_lock l{jobs_lock};
  t.is_valid = true;
  m_queue_impl->push_entry(t);
}

void ScrubQueue::dump_scrubs(ceph::Formatter* f)
{
  std::lock_guard lck(jobs_lock);
  m_queue_impl->dump_scrubs(f);
}


// void ScrubQueue::dump_scrubs(ceph::Formatter* f)
// {
//   std::lock_guard lck(jobs_lock);
//   normalize_the_queue();
//
//   f->open_array_section("scrubs");
//   std::for_each(to_scrub.cbegin(), to_scrub.cend(), [&f](const auto& j) {
//     j.dump("sched-target", f);
//   });
//   f->close_section();
// }

Scrub::ScrubResources& ScrubQueue::resource_bookkeeper()
{
  return m_osd_resources;
}

const Scrub::ScrubResources& ScrubQueue::resource_bookkeeper() const
{
  return m_osd_resources;
}

void ScrubQueue::log_fwd(std::string_view text)
{
  dout(20) << text << dendl;
}


// ////////////////////////////////////////////////////////////////////////// //
// initiating a scrub

using ScrubPreconds = Scrub::ScrubPreconds;
using schedule_result_t = Scrub::schedule_result_t;


// RRR change description.
// the interface used by the tick.
void ScrubQueue::initiate_a_scrub(
    const ceph::common::ConfigProxy& config,
    bool is_recovery_active)
{
  utime_t scrub_tick_time = scrub_clock_now();
  dout(10) << fmt::format(
		  "time now:{}, is_recovery_active:{}", scrub_tick_time,
		  is_recovery_active)
	   << dendl;

  // is there an active 'scrub-loop'? (i.e. - are we in the middle of
  // the asynchronous process of going over the ready-to-scrub PGs, trying
  // them one by one?)
  std::scoped_lock both_locks(jobs_lock, m_loop_lock);

  if (m_initiation_loop) {
    // RRR add a timeout here
    dout(10)
	<< fmt::format(
	       "{}: already looking for scrub candidate (since{}). skipping",
	       __func__, m_initiation_loop->loop_id)
	<< dendl;
    return;
  }

  // do the OSD-wide environment conditions, and the availability of scrub
  // resources, allow us to start a scrub?
  auto maybe_env_cond =
      preconditions_to_scrubbing(config, is_recovery_active, scrub_tick_time);
  if (!maybe_env_cond) {
    return;
  }
  auto preconds = maybe_env_cond.value();

  //std::unique_lock l{jobs_lock};

  auto queue_stats = m_queue_impl->get_stats(scrub_tick_time);
  if (queue_stats.num_ready == 0) {
    dout(10) << fmt::format(
		    "{}: no eligible scrub targets in the {} entries", __func__,
		    queue_stats.num_total)
	     << dendl;
    return;
  }

  auto maybe_cand = m_queue_impl->pop_ready_pg(scrub_tick_time);
  ceph_assert(maybe_cand);
  dout(10) << fmt::format(
		  "{}: scrub candidate is {}. {} still in the queue", __func__,
		  maybe_cand->pgid, queue_stats.num_total - 1)
	   << dendl;

  // as we will be (asynchronously) going over the ready-to-scrub PGs, let us
  // maintain the 'loop' object. It will be used to identify the current
  // 'scrub-loop' (and will be reset when the loop ends). It would also be
  // used to limit the number of PGs tried.

  //auto max_pgs_to_try = std::min<uint32_t, uint32_t>(queue_stats.num_ready, /* conf */ 40);
  auto max_pgs_to_try = queue_stats.num_ready + 5 /* RRR dev testing */;
  m_initiation_loop = std::make_optional<ScrubStartLoop>(
      scrub_tick_time, max_pgs_to_try, preconds, maybe_cand->pgid,
      maybe_cand->level);

  // send a message to that PG to start scrubbing
  osd_service.queue_for_scrub_initiation(
      maybe_cand->pgid, maybe_cand->level, m_initiation_loop->loop_id,
      preconds);
}


void ScrubQueue::scrub_next_in_queue(utime_t loop_id)
{
  std::scoped_lock lock(jobs_lock, m_loop_lock);

  // are we indeed in the middle of a 'scrub-loop'?
  if (!m_initiation_loop) {
    dout(20) << fmt::format("{}: no active scrub-loop ({} suggested)", __func__, loop_id)
	     << dendl;
    return;
  }

  // verify that we are not receiving a message from a previous loop
  if (m_initiation_loop->loop_id != loop_id) {
    dout(10) << fmt::format(
		    "{}: loop-id mismatch. skipping. ({} != {})", __func__,
		    m_initiation_loop->loop_id, loop_id)
	     << dendl;
    return;
  }

  // are we allowed to continue the loop?
  if (++m_initiation_loop->retries_done >= m_initiation_loop->retries_budget) {
    dout(10) << fmt::format(
		    "{}: reached the max number of PGs to try. ending the loop",
		    __func__)
	     << dendl;
    m_initiation_loop.reset();
    return;
  }

  utime_t scrub_tick_time = scrub_clock_now();
  auto queue_stats = m_queue_impl->get_stats(scrub_tick_time);
  if (queue_stats.num_ready == 0) {
    dout(10) << fmt::format(
		    "{}: no eligible scrub targets in the {} entries", __func__,
		    queue_stats.num_total)
	     << dendl;
    m_initiation_loop.reset();
    return;
  }

  auto maybe_cand = m_queue_impl->pop_ready_pg(scrub_tick_time);
  ceph_assert(maybe_cand);

  // a sanity check: do not try the same PG twice
  if (maybe_cand->pgid == m_initiation_loop->first_pg_tried &&
      maybe_cand->level == m_initiation_loop->first_level_tried) {
    dout(10) << fmt::format(
		    "{}: retrying the first sched target. ending the loop",
		    __func__)
	     << dendl;
    m_initiation_loop.reset();
    return;
  }
  dout(10) << fmt::format(
		  "{}: scrub candidate is {}. {} candidates in the queue",
		  __func__, maybe_cand->pgid, queue_stats.num_total)
	   << dendl;

  // send a message to that PG to start scrubbing
  osd_service.queue_for_scrub_initiation(
      maybe_cand->pgid, maybe_cand->level, m_initiation_loop->loop_id,
      m_initiation_loop->env_restrictions);
}

void ScrubQueue::initiation_loop_done(utime_t loop_id)
{
  std::scoped_lock lock(m_loop_lock);

  // are we indeed in the middle of a 'scrub-loop'?
  if (!m_initiation_loop) {
    dout(10) << fmt::format("{}: no active scrub-loop. skipping", __func__)
	     << dendl;
    return;
  }
  // verify that we are not receiving a message from a previous loop
  if (m_initiation_loop->loop_id != loop_id) {
    dout(10) << fmt::format(
		    "{}: loop-id mismatch. skipping. ({} != {})", __func__,
		    m_initiation_loop->loop_id, loop_id)
	     << dendl;
    return;
  }

  m_initiation_loop.reset();
}

void ScrubQueue::sched_scrub(
    const ceph::common::ConfigProxy& config,
    bool is_recovery_active)
{
  //   utime_t scrub_tick_time = scrub_clock_now();
  //   dout(10) << fmt::format(
  // 		  "time now:{}, is_recovery_active:{}", scrub_tick_time,
  // 		  is_recovery_active)
  // 	   << dendl;
  //
  //   // do the OSD-wide environment conditions, and the availability of scrub
  //   // resources, allow us to start a scrub?
  //   auto maybe_env_cond =
  //       preconditions_to_scrubbing(config, is_recovery_active, scrub_tick_time);
  //   if (!maybe_env_cond) {
  //     return;
  //   }
  //   auto preconds = maybe_env_cond.value();
  //
  //   std::unique_lock l{jobs_lock};
  //
  //   // partition and sort the queue
  //   if (bool not_empty = normalize_the_queue(); !not_empty) {
  //     dout(10) << fmt::format("{}: no eligible scrub targets", __func__) << dendl;
  //     return;
  //   }
  //
  //   // pop the first job from the queue, as a candidate
  //   auto cand = to_scrub.front();
  //   to_scrub.pop_front();
  //   l.unlock();
  //
  //   auto locked_g = osd_service.get_locked_pg(cand.pgid);
  //   if (!locked_g) {
  //     // the PG was deleted in the short time since unlocking the queue
  //     dout(5) << fmt::format("{}: pg[{}] not found", __func__, cand.pgid)
  // 	    << dendl;
  //     return;
  //   }
  //   locked_g->pg()->start_scrubbing(scrub_tick_time, cand.level, preconds);
}


tl::expected<ScrubPreconds, schedule_result_t>
ScrubQueue::preconditions_to_scrubbing(
    const ceph::common::ConfigProxy& config,
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
  if ((rand() / (double)RAND_MAX) < config->osd_scrub_backoff_ratio) {
    dout(20) << fmt::format(
		    "{}: lost coin flip, randomly backing off (ratio: {:f})",
		    __func__, config->osd_scrub_backoff_ratio)
	     << dendl;
    return tl::unexpected(schedule_result_t::failure);
  }

  // fail fast if no resources are available
  if (!resource_bookkeeper().can_inc_scrubs()) {
    dout(10) << fmt::format("{}: OSD cannot inc scrubs", __func__) << dendl;
    return tl::unexpected(schedule_result_t::failure);
  }

  // if there is a PG that is just now trying to reserve scrub replica resources
  // - we should wait and not initiate a new scrub
  if (is_reserving_now()) {
    dout(10) << fmt::format(
		    "{}: scrub resources reservation in progress", __func__)
	     << dendl;
    return tl::unexpected(schedule_result_t::failure);
  }

  Scrub::ScrubPreconds env_conditions;
  env_conditions.time_permit = scrub_time_permit();
  env_conditions.load_is_low = scrub_load_below_threshold();
  env_conditions.only_deadlined =
      !env_conditions.time_permit || !env_conditions.load_is_low;

// RRR ???
  if (is_recovery_active && !config->osd_scrub_during_recovery) {
    if (!config->osd_repair_during_recovery) {
      dout(15) << fmt::format(
		      "{}: not scheduling scrubs due to active recovery",
		      __func__)
	       << dendl;
      return tl::unexpected(schedule_result_t::failure);
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

/**
 * the refactored "OSD::sched_all_scrubs()"
 *
 * Scans the queue for entries that are "periodic", and messages the PGs
 * named in those entries to recalculate their scrub scheduling
 */
void ScrubQueue::on_config_times_change()
{
  std::unique_lock l{jobs_lock};
  auto to_notify = m_queue_impl->get_pgs([](const SchedEntry& e) -> bool {
    return e.urgency == urgency_t::periodic_regular;
  });
  l.unlock();

  for (const auto& p : to_notify) {
    dout(15) << fmt::format("{}: rescheduling {}", __func__, p) << dendl;
    osd_service.send_sched_recalc_to_pg(p);
  }
}

// void ScrubQueue::on_config_times_change()
// {
//   std::set<spg_t> to_notify;
//   std::unique_lock l{jobs_lock};
//   for (const auto& e : to_scrub) {
//     if (e.is_valid && e.urgency == urgency_t::periodic_regular) {
//       to_notify.insert(e.pgid);
//     }
//   }
//   l.unlock();
//
//   for (const auto& p : to_notify) {
//     dout(15) << fmt::format("{}: rescheduling {}", __func__, p) << dendl;
//     osd_service.send_sched_recalc_to_pg(p);
//   }
// }

// ////////////////////////////////////////////////////////////////////////// //
// auxiliaries

Scrub::sched_conf_t ScrubQueue::populate_config_params(
    const pool_opts_t& pool_conf) const
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

  // the max allowed delay between scrubs.
  // For deep scrubs - there is no equivalent of scrub_max_interval. Per the
  // documentation, once deep_scrub_interval has passed, we are already
  // "overdue", at least as far as the "ignore allowed load" window is
  // concerned.

  configs.max_deep = configs.deep_interval + configs.shallow_interval;

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
  configs.deep_randomize_ratio = conf()->osd_deep_scrub_randomize_ratio;
  configs.mandatory_on_invalid = conf()->osd_scrub_invalid_stats;

  dout(15) << fmt::format("updated config:{}", configs) << dendl;
  return configs;
}


// ////////////////////////////////////////////////////////////////////////// //
// container low-level operations. Will be extracted, and implemented by a
// dedicated container class

// used in ut/debug logs
constexpr int ordering_as_int(std::weak_ordering cmp) noexcept
{
  return (cmp < 0) ? -1 : ((cmp == 0) ? 0 : 1);
}

// must be called under the lock
bool ScrubQueue::normalize_the_queue()
{
  return true;
  //   // erase all 'invalid' entries
  //   to_scrub.erase(
  //       std::remove_if(
  // 	  to_scrub.begin(), to_scrub.end(),
  // 	  [](const auto& sched_entry) { return !sched_entry.is_valid; }),
  //       to_scrub.end());
  //
  //   auto nowis = scrub_clock_now();
  //   // partition into 'ripe' and to those not eligible for scrubbing
  //   auto not_ripe = std::stable_partition(
  //       to_scrub.begin(), to_scrub.end(),
  //       [nowis](const auto& sched_entry) { return sched_entry.is_ripe(nowis); });
  //
  //   // sort the 'ripe' entries by their specific criteria
  //   std::sort(to_scrub.begin(), not_ripe, [](const auto& lhs, const auto& rhs) {
  // #ifdef DEBUG
  //     std::cout << fmt::format(
  // 	"-r- comparing {} / {} -> {}\n", lhs, rhs,
  // 	ordering_as_int(cmp_ripe_entries(lhs, rhs)));
  // #endif
  //     return cmp_ripe_entries(lhs, rhs) < 0;
  //   });
  //
  //   // sort those with not-before in the future - mostly by their 'not-before'
  //   // time
  //   std::sort(not_ripe, to_scrub.end(), [](const auto& lhs, const auto& rhs) {
  // #ifdef DEBUG
  //     std::cout << fmt::format(
  // 	"-x- comparing {} / {} -> {}\n", lhs, rhs,
  // 	ordering_as_int(cmp_future_entries(lhs, rhs)));
  // #endif
  //     return cmp_future_entries(lhs, rhs) < 0;
  //   });
  //
  //   const int ready_cnt = std::distance(to_scrub.begin(), not_ripe);
  //   const int future_cnt = std::distance(not_ripe, to_scrub.end());
  //   dout(10) << fmt::format(
  // 		  "{}: ready: {}, future: {} total queue size: {}", __func__,
  // 		  ready_cnt, future_cnt, to_scrub.size())
  // 	   << dendl;
  //
  //   // dump the queue
  //   {
  //     static const int max_to_log = 10;
  //
  //     // top of the ready-queue
  //     int ready_n = std::min(ready_cnt, max_to_log);
  //     if (ready_n && g_conf()->subsys.should_gather<ceph_subsys_osd, 10>()) {
  //       dout(10) << fmt::format(
  // 		      "{}: top ({} of {}) of the ready-queue:", __func__,
  // 		      ready_n, ready_cnt)
  // 	       << dendl;
  //       for (int i = 0; i < ready_n; ++i) {
  // 	dout(10) << fmt::format(" ready:  {}", to_scrub[i]) << dendl;
  //       }
  //     }
  //
  //     // and some of the targets with 'not-before' in the future
  //     int future_n = std::min(future_cnt, max_to_log);
  //     if (future_n && g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
  //       dout(10) << fmt::format(
  // 		      "{}: top ({} of {}) of the future targets:", __func__,
  // 		      future_n, future_cnt)
  // 	       << dendl;
  //       int k = future_n;
  //       for (auto e = not_ripe; k > 0; --k, ++e) {
  // 	dout(20) << fmt::format(" future: {}", *e) << dendl;
  //       }
  //     }
  //   }
  //   return not_ripe != to_scrub.begin();
}


void ScrubQueue::clear_pg_scrub_blocked(spg_t blocked_pg)
{
  dout(5) << fmt::format("{}: pg[{}] is unblocked", __func__, blocked_pg)
	  << dendl;
  --blocked_scrubs_cnt;
  ceph_assert(blocked_scrubs_cnt >= 0);
}

void ScrubQueue::mark_pg_scrub_blocked(spg_t blocked_pg)
{
  dout(5) << fmt::format(
		 "{}: pg[{}] is blocked on an object", __func__, blocked_pg)
	  << dendl;
  ++blocked_scrubs_cnt;
}

int ScrubQueue::get_blocked_pgs_count() const
{
  return blocked_scrubs_cnt;
}


// ////////////////////////////////////////////////////////////////////////// //
// SchedLoopHolder

using SchedLoopHolder = Scrub::SchedLoopHolder;

SchedLoopHolder::~SchedLoopHolder()
{
  // we may have failed without handling the sched-loop
  // state. Let's just ignore it ('success()' does not cause any harm)
  conclude_candidates_selection();
}

/// tell the ScrubQueue to terminate the sched-loop (the process of trying
/// to schedule the queue elements for scrubbing)
void SchedLoopHolder::conclude_candidates_selection()
{
  if (m_loop_id) {
    m_queue.initiation_loop_done(*m_loop_id);
    m_loop_id.reset();
  }
}

void SchedLoopHolder::go_for_next_in_queue()
{
  if (m_loop_id) {
    // we must have failed to schedule a scrub
    m_queue.scrub_next_in_queue(*m_loop_id);
    m_loop_id.reset();
  }
}


// ////////////////////////////////////////////////////////////////////////// //
// ScrubQueueImp: container low-level operations


void ScrubQueueImp::push_entry(const SchedEntry& entry)
{
  //dout(10) << fmt::format("{}: {}", __func__, entry) << dendl;
  to_scrub.push_back(entry);
  normalize_queue(parent_queue.scrub_clock_now());
}

bool ScrubQueueImp::remove_entry(spg_t pgid, scrub_level_t s_or_d)
{
  auto i = std::find_if(
      to_scrub.begin(), to_scrub.end(),
      [pgid, s_or_d](const auto& e) { return same_key(e, pgid, s_or_d); });
  if (i == to_scrub.end()) {
    return false;
  }
  to_scrub.erase(i);
  normalize_queue(parent_queue.scrub_clock_now());
  return true;
}


ScrubQueueStats ScrubQueueImp::get_stats(utime_t scrub_clock_now) const
{
  // assuming the queue is normalized
  auto first_not_ripe = std::find_if_not(
      to_scrub.cbegin(), to_scrub.cend(),
      [scrub_clock_now](const auto& e) { return e.is_ripe(scrub_clock_now); });

  return ScrubQueueStats{
      static_cast<uint_fast16_t>(
	  std::distance(to_scrub.cbegin(), first_not_ripe)),
      static_cast<uint_fast16_t>(to_scrub.size())};
}

std::optional<SchedEntry> ScrubQueueImp::pop_ready_pg(utime_t scrub_clock_now)
{
  normalize_queue(scrub_clock_now);
  const auto& e = to_scrub.front();
  if (e.is_ripe(scrub_clock_now)) {
    to_scrub.pop_front();
    return e;
  }
  return std::nullopt;
}


void ScrubQueueImp::dump_scrubs(ceph::Formatter* f) const {}

std::set<spg_t> ScrubQueueImp::get_pgs(ScrubQueueImp_IF::EntryPred pred) const
{
  std::set<spg_t> res;
  for (const auto& e : to_scrub) {
    if (pred(e)) {
      res.insert(e.pgid);
    }
  }
  return res;
}

std::vector<SchedEntry> ScrubQueueImp::get_entries(
    ScrubQueueImp_IF::EntryPred pred) const
{
  std::vector<SchedEntry> res;
  auto pre_allocated = std::min(to_scrub.size(), (size_t)(100));
  res.reserve(pre_allocated);

  for (const auto& e : to_scrub) {
    if (pred(e)) {
      res.push_back(e);
    }
  }
  return res;
}

std::deque<SchedEntry>::iterator ScrubQueueImp::normalize_queue(utime_t nowis)
{
  // partition into 'ripe' and to those not eligible for scrubbing
  auto not_ripe = std::stable_partition(
      to_scrub.begin(), to_scrub.end(),
      [nowis](const auto& sched_entry) { return sched_entry.is_ripe(nowis); });

  // sort the 'ripe' entries by their specific criteria
  std::sort(to_scrub.begin(), not_ripe, [](const auto& lhs, const auto& rhs) {
#ifdef DEBUGss
    std::cout << fmt::format(
	"-r- comparing {} / {} -> {}\n", lhs, rhs,
	ordering_as_int(cmp_ripe_entries(lhs, rhs)));
#endif
    return cmp_ripe_entries(lhs, rhs) < 0;
  });

  // sort those with not-before in the future - mostly by their 'not-before'
  // time
  std::sort(not_ripe, to_scrub.end(), [](const auto& lhs, const auto& rhs) {
#ifdef DEBUGss
    std::cout << fmt::format(
	"-x- comparing {} / {} -> {}\n", lhs, rhs,
	ordering_as_int(cmp_future_entries(lhs, rhs)));
#endif
    return cmp_future_entries(lhs, rhs) < 0;
  });

  //    const int ready_cnt = std::distance(to_scrub.begin(), not_ripe);
  //    const int future_cnt = std::distance(not_ripe, to_scrub.end());
  //     dout(10) << fmt::format(
  //   		  "{}: ready: {}, future: {} total queue size: {}", __func__,
  //   		  ready_cnt, future_cnt, to_scrub.size())
  //   	   << dendl;

  // dump the queue
#if 0
    {
      static const int max_to_log = 10;
  
      // top of the ready-queue
      int ready_n = std::min(ready_cnt, max_to_log);
      if (ready_n && g_conf()->subsys.should_gather<ceph_subsys_osd, 10>()) {
        dout(10) << fmt::format(
  		      "{}: top ({} of {}) of the ready-queue:", __func__,
  		      ready_n, ready_cnt)
  	       << dendl;
        for (int i = 0; i < ready_n; ++i) {
  	dout(10) << fmt::format(" ready:  {}", to_scrub[i]) << dendl;
        }
      }
  
      // and some of the targets with 'not-before' in the future
      int future_n = std::min(future_cnt, max_to_log);
      if (future_n && g_conf()->subsys.should_gather<ceph_subsys_osd, 20>()) {
        dout(10) << fmt::format(
  		      "{}: top ({} of {}) of the future targets:", __func__,
  		      future_n, future_cnt)
  	       << dendl;
        int k = future_n;
        for (auto e = not_ripe; k > 0; --k, ++e) {
  	dout(20) << fmt::format(" future: {}", *e) << dendl;
        }
      }
    }
#endif
  return not_ripe;
}
