// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab


#include "./pg_scrub_sched.h"

#include "debug.h"

#include "crimson/osd/pg_backend.h"
#include "crimson/osd/scrubber/scrub_machine_cr.h"

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

namespace crimson::osd {

PgScrubSched::PgScrubSched(PG& pg)
    : m_pg{pg}
    , m_pg_id{pg.get_pgid()}
    , m_scrubber{m_pg.m_scrubber.get()}
    , m_osds{m_pg.get_shard_services()}
{
}


bool PgScrubSched::forced_scrub(Formatter* f, scrub_level_t depth)
{
  const bool deep = (depth == scrub_level_t::deep);

  logger().debug("{}: pg({}) depth:{}", __func__, m_pg_id, deep ? "deep" : "shallow");

  // no need to check whether Primary, as PGCommand has already verified that
  //
  // if (!m_pg.is_primary()) {
  //  // msg to f - not a primary
  //  return false;
  //}

  // RRR complete auto interval_configured = [this](pool_opts_t pool_conf, Option::value_t

  // the max-interval for the specific scrub requested depends on pool & global conf
  const double pool_max_interval =
    deep ? m_pg.get_pool().info.opts.value_or(pool_opts_t::DEEP_SCRUB_INTERVAL, 0.0)
	 : m_pg.get_pool().info.opts.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);

  const double max_interval = (pool_max_interval > 0.0)
				? pool_max_interval
				: (deep ? m_pg.get_cct()->_conf->osd_deep_scrub_interval
					: m_pg.get_cct()->_conf->osd_scrub_max_interval);

  utime_t stamp = ceph_clock_now();
  stamp -= (max_interval + 100.0);

  m_pg.set_specific_scrub_stamp(depth, stamp);
  m_pg.scrub_requested(depth, scrub_type_t::not_repair);

  f->open_object_section("result");
  f->dump_bool("deep", deep);
  f->dump_stream("stamp") << stamp;
  f->close_section();
  return true;
}

/*
 *  implementation note:
 *  PG::sched_scrub() is called only once per a specific scrub session.
 *  That call commits us to the whatever choices are made (deep/shallow, etc').
 *  Unless failing to start scrubbing, the 'planned scrub' flag-set is 'frozen' into
 *  PgScrubber's m_flags, then cleared.
 */
bool PgScrubSched::sched_scrub()
{
  logger().debug("PgScrubSched::{}: pg({}) {}:{}", __func__, m_pg_id,
		 (m_pg.peering_state.is_active() ? "<active>" : "<not-active>"),
		 (m_pg.peering_state.is_clean() ? "<clean>" : "<not-clean>"));
  // ceph_assert(ceph_mutex_is_locked(_lock));
  // ceph_assert(!is_scrubbing());

  if (!m_pg.peering_state.is_primary() || !m_pg.peering_state.is_active() ||
      !m_pg.peering_state.is_clean()) {
    return false;
  }

  if (scrub_queued) {
    // only applicable to the very first time a scrub event is queued
    // (until handled and posted to the scrub FSM)
    logger().warn("{}: already queued", __func__);
    // for now RRR // return false;
  }

  // analyse the combination of the requested scrub flags, the osd/pool configuration
  // and the PG status to determine whether we should scrub now, and what type of scrub
  // should that be.
  auto updated_flags = verify_scrub_mode();
  if (!updated_flags) {
    // the stars do not align for starting a scrub for this PG at this time
    // (due to configuration or priority issues)
    // The reason was already reported by the callee.
    logger().warn("{}: failed to initiate a scrub", __func__);
    return false;
  }

  // try to reserve the local OSD resources. If failing: no harm. We will
  // be retried by the OSD later on.
  if (!m_scrubber->reserve_local()) {
    logger().warn("{}: failed to reserve locally", __func__);
    return false;
  }

  // can commit to the updated flags now, as nothing will stop the scrub
  m_pg.m_planned_scrub = *updated_flags;

  // An interrupted recovery repair could leave this set.
  m_pg.state_clear(PG_STATE_REPAIR);

  // Pass control to the scrubber. It is the scrubber that handles the replicas'
  // resources reservations.
  m_scrubber->set_op_parameters(m_pg.m_planned_scrub);

  logger().info("{}: queueing", __func__);

  scrub_queued = true;
  // m_pg.shard_services.queue_for_scrub(this, Scrub::scrub_prio_t::low_priority); // RRR
  // !!!!

  // the following works, but then the process_event() is done in-place. We'd like
  // to go thru the queue here RRR
//  std::ignore = m_osds.start_operation<LocalScrubEvent>(
//    &m_pg, m_osds, m_pg.get_pg_whoami(), m_pg_id, m_pg.get_osdmap_epoch(),
//    m_pg.get_osdmap_epoch(), crimson::osd::Scrub::StartScrub{});

  m_scrubber->queue_regular_scrub();
  return true;
}


std::optional<requested_scrub_t> PgScrubSched::verify_scrub_mode() const
{
  logger().info("{}: processing pg {}", __func__, m_pg_id);

  bool allow_deep_scrub = !(test_osdmap_flag(CEPH_OSDMAP_NODEEP_SCRUB) ||
			    test_pool_flag(pg_pool_t::FLAG_NODEEP_SCRUB));
  bool allow_regular_scrub =
    !(test_osdmap_flag(CEPH_OSDMAP_NOSCRUB) || test_pool_flag(pg_pool_t::FLAG_NOSCRUB));
  bool has_deep_errors =
    (m_pg.get_peering_state().get_info().stats.stats.sum.num_deep_scrub_errors > 0);
  bool try_to_auto_repair = (get_pg_cct()->_conf->osd_scrub_auto_repair &&
			     true /* RRR m_pg.get_backend()->auto_repair_supported()*/);

  auto upd_flags = m_pg.m_planned_scrub;

  upd_flags.time_for_deep = false;
  // Clear these in case user issues the scrub/repair command during
  // the scheduling of the scrub/repair (e.g. request reservation)
  upd_flags.deep_scrub_on_error = false;
  upd_flags.auto_repair = false;

  if (upd_flags.must_scrub && !upd_flags.must_deep_scrub && has_deep_errors) {
    logger().error(
      "{}: osd.{} pg:{}: Regular scrub request, deep-scrub details will be lost",
      __func__, get_shard_num(), m_pg_id);
  }

  if (!upd_flags.must_scrub) {
    // All periodic scrub handling goes here because must_scrub is
    // always set for must_deep_scrub and must_repair.

    bool can_start_periodic =
      verify_periodic_scrub_mode(allow_deep_scrub, try_to_auto_repair,
				 allow_regular_scrub, has_deep_errors, upd_flags);
    if (!can_start_periodic) {
      return std::nullopt;
    }
  }

  //  scrubbing while recovering?

  bool prevented_by_recovery =
    m_osds.is_recovery_active() && !get_pg_cct()->_conf->osd_scrub_during_recovery &&
    (!get_pg_cct()->_conf->osd_repair_during_recovery || !upd_flags.must_repair);

  if (prevented_by_recovery) {
    logger().warn("{}: scrubbing prevented during recovery", __func__);
    return std::nullopt;
  }

  upd_flags.need_auto = false;
  return upd_flags;
}

bool PgScrubSched::verify_periodic_scrub_mode(bool allow_deep_scrub,
					      bool try_to_auto_repair,
					      bool allow_regular_scrub,
					      bool has_deep_errors,
					      requested_scrub_t& planned) const

{
  ceph_assert(!planned.must_deep_scrub && !planned.must_repair);

  if (!allow_deep_scrub && has_deep_errors) {
    // RRR   osd->clog->error()
    //      << "osd." << osd->whoami << " pg " << info.pgid
    //      << " Regular scrub skipped due to deep-scrub errors and nodeep-scrub set";
    return false;
  }

  if (allow_deep_scrub) {
    // Initial entry and scheduled scrubs without nodeep_scrub set get here

    planned.time_for_deep =
      is_time_for_deep(allow_deep_scrub, allow_regular_scrub, has_deep_errors, planned);

    if (try_to_auto_repair) {
      if (planned.time_for_deep) {
	logger().info("{}: auto repair with deep scrubbing", __func__);
	planned.auto_repair = true;
      } else if (allow_regular_scrub) {
	logger().info("{}: auto repair with scrubbing, rescrub if errors found",
		      __func__);
	planned.deep_scrub_on_error = true;
      }
    }
  }

  logger().info("{}: updated flags: {}. Allow_regular_scrub: {}", __func__, planned,
		allow_regular_scrub);

  // NOSCRUB so skip regular scrubs
  if (!allow_regular_scrub && !planned.time_for_deep) {
    return false;
  }

  return true;
}

bool PgScrubSched::is_time_for_deep(bool allow_deep_scrub,
				    bool allow_scrub,
				    bool has_deep_errors,
				    const requested_scrub_t& planned) const
{
  logger().info("{}: need_auto? {}  allow_deep_scrub? {}", __func__, planned.need_auto,
		allow_deep_scrub);

  if (!allow_deep_scrub)
    return false;

  if (planned.need_auto) {
    logger().warn("{}: need repair after scrub errors", __func__);
    return true;
  }

  if (ceph_clock_now() >= next_deepscrub_interval())
    return true;

  if (has_deep_errors) {
    // RRR osd->clog->info() << "osd." << get_shard_num << " pg " << m_pg_id
    // RRR 		      << " Deep scrub errors, upgrading scrub to deep-scrub";
    return true;
  }

  // we only flip coins if 'allow_scrub' is asserted. Otherwise - as this function is
  // called often, we will probably be deep-scrubbing most of the time.
  if (allow_scrub) {
    bool deep_coin_flip =
      (rand() % 100) < get_pg_cct()->_conf->osd_deep_scrub_randomize_ratio * 100;

    logger().debug("{}: time_for_deep={} deep_coin_flip={}", __func__,
		   planned.time_for_deep, deep_coin_flip);

    if (deep_coin_flip)
      return true;
  }

  return false;
}


double PgScrubSched::next_deepscrub_interval() const
{
  double deep_scrub_interval =
    m_pg.get_pool().info.opts.value_or(pool_opts_t::DEEP_SCRUB_INTERVAL, 0.0);
  if (deep_scrub_interval <= 0.0)
    deep_scrub_interval = get_pg_cct()->_conf->osd_deep_scrub_interval;
  return m_pg.get_peering_state().get_info().history.last_deep_scrub_stamp +
	 deep_scrub_interval;
}


bool PgScrubSched::test_pool_flag(int fl) const
{
  return m_pg.get_pool().info.has_flag(fl);
}


bool PgScrubSched::test_osdmap_flag(int fl) const
{
  return m_pg.get_osdmap()->test_flag(fl);
}

CephContext* PgScrubSched::get_pg_cct() const
{
  return m_pg.get_cct();
}

pg_shard_t PgScrubSched::get_shard_num() const
{
  return m_pg.get_pg_whoami();
}

int PgScrubSched::get_scrub_priority()
{
  const int64_t pool_scrub_priority =
    m_pg.get_pool().info.opts.value_or(pool_opts_t::SCRUB_PRIORITY, (int64_t)0);

  return pool_scrub_priority > 0 ? pool_scrub_priority
				 : get_pg_cct()->_conf->osd_scrub_priority;
}


}  // namespace crimson::osd
