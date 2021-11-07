// -*- mode:C++; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab

#include "./crimson/osd/scrubber/scrubber.h"

#include <iostream>
#include <vector>
#include <unordered_map>
#include <map>

#include "debug.h"

#include "common/errno.h"
#include "crimson/common/log.h"
#include "crimson/os/futurized_collection.h"
//#include "crimson/os/futurized_store.h"
#include "crimson/osd/osd.h"
//#include "crimson/osd/osd_operations/pg_scrub_event.h"
#include "crimson/osd/osd_operations/scrub_event.h"
#include "crimson/osd/pg_backend.h"
#include "crimson/osd/scrubber/pg_scrub_sched.h"
#include "crimson/osd/scrubber/scrubber_backend.h"
#include "crimson/osd/scrubber/scrub_machine_cr.h"
//#include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDScrub.h"
#include "messages/MOSDScrubReserve.h"

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

using std::ostream;
//using scrub_flags_t;
using namespace ::crimson::osd::Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;

//using ::crimson::osd::Scrub::PreemptionNoted;
using Scrub::PreemptionNoted;
using ::crimson::osd::ScrubEventFwd;

#if 0
#define dout_context (m_pg->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this->m_pg)

template <class T> static ostream &_prefix(std::ostream *_dout, T *t) {
  return t->gen_prefix(*_dout) << " scrubber pg(" << t->get_pgid() << ") ";
}
#endif



#ifdef WITH_SEASTAR
#define prefix_temp "--prefix-- "
#define RRLOG(LVL, S) logger().debug("{}", std::string{prefix_temp}+(S))
#else
#define RRLOG(LVL, S) { dout << (S) << dendl; }
#endif

//namespace crimson::osd {


ostream& operator<<(ostream& out, const scrub_flags_t& sf)
{
  if (sf.auto_repair)
    out << " AUTO_REPAIR";
  if (sf.check_repair)
    out << " CHECK_REPAIR";
  if (sf.deep_scrub_on_error)
    out << " DEEP_SCRUB_ON_ERROR";
  if (sf.required)
    out << " REQ_SCRUB";

  return out;
}


ostream& operator<<(ostream& out, const requested_scrub_t& sf)
{
  if (sf.must_repair)
    out << " MUST_REPAIR";
  if (sf.auto_repair)
    out << " planned AUTO_REPAIR";
  if (sf.check_repair)
    out << " planned CHECK_REPAIR";
  if (sf.deep_scrub_on_error)
    out << " planned DEEP_SCRUB_ON_ERROR";
  if (sf.must_deep_scrub)
    out << " MUST_DEEP_SCRUB";
  if (sf.must_scrub)
    out << " MUST_SCRUB";
  if (sf.time_for_deep)
    out << " TIME_FOR_DEEP";
  if (sf.need_auto)
    out << " NEED_AUTO";
  if (sf.req_scrub)
    out << " planned REQ_SCRUB";

  return out;
}


// moved from the .h (due to temp compilation issues)
eversion_t PgScrubber::get_last_update_applied() const
{
  return m_pg->get_peering_state().get_last_update_applied();
}

// RRR int PgScrubber::pending_active_pushes() const { return m_pg->active_pushes; }
int PgScrubber::pending_active_pushes() const
{
  logger().warn("{}: NOT IMPLEMENTED", __func__);
  return 0;
}

bool PgScrubber::state_test(uint64_t m) const
{
  return m_pg->peering_state.state_test(m);
}

void PgScrubber::state_set(uint64_t m)
{
  m_pg->peering_state.state_set(m);
}

void PgScrubber::state_clear(uint64_t m)
{
  m_pg->peering_state.state_clear(m);
}

[[nodiscard]] bool PgScrubber::is_primary() const
{
  return m_pg->peering_state.is_primary();
}

CephContext* PgScrubber::get_pg_cct() const
{
  return m_pg->get_cct();
}

void PgScrubber::requeue_waiting() const
{
  ; /* RRR m_pg->requeue_ops(m_pg->waiting_for_scrub); */
}

//void PgScrubber::do_scrub_event(const crimson::osd::PgScrubEvent& evt, PeeringCtx& rctx)
//{
//  logger().warn("{}: processing event: {}", __func__, evt.get_desc());
//
//  m_fsm->process_event(evt.get_event());
//}

void PgScrubber::queue_local_trigger(ScrubEventFwd trigger,
				     epoch_t epoch_queued,
				     std::chrono::milliseconds delay,
				     std::string_view desc)
{
  logger().debug("{} q-ing func: {} (this:{:p})", __func__, desc, (void*)this);

  std::ignore = m_pg->get_shard_services().start_operation<crimson::osd::ScrubEvent2>(
    m_pg, m_pg->get_shard_services(), m_pg->get_pg_whoami(), m_pg->get_pgid(), delay,
    trigger, epoch_queued, desc);
}

/*
 * if the incoming message is from a previous interval, it must mean
 * PrimaryLogPG::on_change() was called when that interval ended. We can safely
 * discard the stale message.
 */
bool PgScrubber::check_interval(epoch_t epoch_to_verify)
{
  return epoch_to_verify >= m_pg->get_same_interval_since();
}


bool PgScrubber::is_message_relevant(epoch_t epoch_to_verify)
{
  if (!m_active) {
    // not scrubbing. We can assume that the scrub was already terminated, and
    // we can silently discard the incoming event.
    logger().debug("scrubber: {} denied - not active (ep. {})", __func__,
		   epoch_to_verify);
    return false;
  }

  // RRR remind myself why we are checking against the 'start epoch' and not the interval.
  //  Maybe the replica should wait for the map?
  // is this a message from before we started this scrub?
  //  if (epoch_to_verify < m_epoch_start) {
  //    logger().debug("scrubber: {} denied - epoch({}) < start({})", __func__,
  //		   epoch_to_verify, m_epoch_start);
  //    return false;
  //  }

  // has a new interval started?
  if (!check_interval(epoch_to_verify)) {
    // if this is a new interval, on_change() has already terminated that
    // old scrub.
    logger().debug("scrubber: {} denied - new interval (ep. {})", __func__,
		   epoch_to_verify);
    return false;
  }

  ceph_assert(is_primary());

  // were we instructed to abort?
  return verify_against_abort(epoch_to_verify);
}


bool PgScrubber::verify_against_abort(epoch_t epoch_to_verify)
{
  if (!should_abort()) {
    return true;
  }

  logger().debug("{} aborting. incoming epoch: {}. Last epoch aborted: {}", __func__,
		 epoch_to_verify, m_last_aborted);

  // if we were not aware of the abort before - kill the scrub.
  if (epoch_to_verify > m_last_aborted) {
    scrub_clear_state();
    m_last_aborted = std::max(epoch_to_verify, m_epoch_start);
  }
  return false;
}


bool PgScrubber::should_abort() const
{
  if (m_flags.required) {
    return false;  // not stopping 'required' scrubs for configuration changes
  }

  // const PGPool& p1 = m_pg->get_pool();
  // auto has1 = p1.info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB);

  if (m_is_deep) {
    if (get_osdmap()->test_flag(CEPH_OSDMAP_NODEEP_SCRUB) ||
	m_pg->get_pool().info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB)
	/* m_pg->pool.info.has_flag(pg_pool_t::FLAG_NODEEP_SCRUB)*/) {
      logger().debug("nodeep_scrub set, aborting");
      return true;
    }
  }

  if (get_osdmap()->test_flag(CEPH_OSDMAP_NOSCRUB) ||
      m_pg->get_pool().info.has_flag(pg_pool_t::FLAG_NOSCRUB)) {
    logger().debug("noscrub set, aborting");
    return true;
  }

  return false;
}

//   initiating state-machine events --------------------------------

/*
 * a note re the checks performed before sending scrub-initiating messages:
 *
 * For those ('StartScrub', 'AfterRepairScrub') scrub-initiation messages that
 * possibly were in the queue while the PG changed state and became unavailable
 * for scrubbing:
 *
 * The check_interval() catches all major changes to the PG. As for the other
 * conditions we may check (and see is_message_relevant() above):
 *
 * - we are not 'active' yet, so must not check against is_active(), and:
 *
 * - the 'abort' flags were just verified (when the triggering message was
 * queued). As those are only modified in human speeds - they need not be
 * queried again.
 *
 * Some of the considerations above are also relevant to the replica-side
 * initiation
 * ('StartReplica' & 'StartReplicaNoWait').
 */

void PgScrubber::initiate_regular_scrub(epoch_t epoch_queued)
{
  RRLOG(15, ( (fmt::format("{}:epoch: {}", __func__, epoch_queued) )) );

  // we may have lost our Primary status while the message languished in the queue
  if (check_interval(epoch_queued)) {
    RRLOG(10, ( (fmt::format("{}: scrubber event -->> StartScrub epoch: {}", __func__, epoch_queued)) ));
    //dout(10) << "scrubber event -->> StartScrub epoch: " << epoch_queued << dendl;
    reset_epoch(epoch_queued);
    m_fsm->process_event(StartScrub{});
    RRLOG(10, ( (fmt::format("{}: scrubber event --<< StartScrub", __func__)) ));
    //dout(10) << "scrubber event --<< StartScrub" << dendl;
  }
}

// an aux used by PgScrubSched:
void PgScrubber::queue_regular_scrub()
{
  queue_local_trigger(&ScrubPgIF::initiate_regular_scrub, m_pg->get_osdmap_epoch(), 1ms, "StartScrub");
}

void PgScrubber::initiate_scrub_after_repair(epoch_t epoch_queued)
{
  RRLOG(10, ( (fmt::format("{}: scrubber event -->> StartScrub epoch: {}", __func__, epoch_queued)) ));

  //logger().debug("scrubber: {} epoch: {}", __func__, epoch_queued);

  // we may have lost our Primary status while the message languished in the
  // queue
  if (check_interval(epoch_queued)) {
    logger().debug("{}: scrubber event -->> AfterRepairScrub epoch: {}", __func__,
		   epoch_queued);
    reset_epoch(epoch_queued);
    m_fsm->process_event(StartScrub{}); // no more 'after repair' shortcut
    RRLOG(10, ( (fmt::format("{}: scrubber event --<< AfterRepairScrub", __func__)) ));
  }
}

void PgScrubber::send_scrub_unblock(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->process_event(Scrub::Unblocked{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_scrub_resched(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::InternalSchedScrub{});
    logger().debug("scrubber event --<< {}", __func__);
  }
}

void PgScrubber::send_start_replica(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_primary()) {
    // shouldn't happen. Ignore
    logger().debug("got a replica scrub request while Primary!");
    return;
  }
  if (check_interval(epoch_queued)) {
    m_fsm->my_states();
    // save us some time by not waiting for updates if there are none
    // to wait for. Affects the transition from NotActive into either
    // ReplicaWaitUpdates or ActiveReplica.
    if (pending_active_pushes())
      m_fsm->process_event(Scrub::StartReplica{});
    else
      m_fsm->process_event(Scrub::StartReplicaNoWait{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_sched_replica(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (check_interval(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::SchedReplica{});  // retest for map availability
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::active_pushes_notification(epoch_t epoch_queued)
{
  // note: Primary only
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::ActivePushesUpd{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::update_applied_notification(epoch_t epoch_queued)
{
  // note: Primary only
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::UpdatesApplied{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::digest_update_notification(epoch_t epoch_queued)
{
  // note: Primary only
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::DigestUpdate{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_replica_maps_ready(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::GotReplicas{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_replica_pushes_upd(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (check_interval(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::ReplicaPushesUpd{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_remotes_reserved(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  // note: scrub is not active yet
  if (check_interval(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::RemotesReserved{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_reservation_failure(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (check_interval(epoch_queued)) {  // do not check for 'active'!
    m_fsm->my_states();
    m_fsm->process_event(Scrub::ReservationFailure{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

// ----- Crimson-specific event forwarders

void PgScrubber::send_full_reset(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (check_interval(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::FullReset{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_chunk_free(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (check_interval(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::SelectedChunkFree{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_chunk_busy(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (check_interval(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::ChunkIsBusy{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_requests_sent(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (check_interval(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::ReplicaRequestsSent{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_local_map_done(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::IntLocalMapDone{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_oninit_done(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  //if (is_message_relevant(epoch_queued)) {
  //  m_fsm->my_states();
    m_fsm->process_event(Scrub::OnInitDone{});
  //}
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_get_next_chunk(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->my_states();
    m_fsm->process_event(Scrub::NextChunk{});
  }
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_scrub_is_finished(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  // can't check for "active"
  // if (is_message_relevant(epoch_queued)) {
  //  m_fsm->my_states();
  m_fsm->process_event(Scrub::ScrubFinished{});
  //}
  logger().debug("scrubber event --<< {}", __func__);
}

void PgScrubber::send_maps_compared(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  // if (is_message_relevant(epoch_queued)) {
  //  m_fsm->my_states();
  m_fsm->process_event(Scrub::MapsCompared{});
  //}
  logger().debug("scrubber event --<< {}", __func__);
}

// -----------------

bool PgScrubber::is_reserving() const
{
  return m_fsm->is_reserving();
}

void PgScrubber::reset_epoch(epoch_t epoch_queued)
{
  logger().debug("state_deep? {}", state_test(PG_STATE_DEEP_SCRUB));
  m_fsm->assert_not_active();

  m_epoch_start = epoch_queued;
  m_needs_sleep = true;
  m_is_deep = state_test(PG_STATE_DEEP_SCRUB);
}

unsigned int PgScrubber::scrub_requeue_priority(Scrub::scrub_prio_t with_priority) const
{
  unsigned int qu_priority = m_flags.priority;

  if (with_priority == Scrub::scrub_prio_t::high_priority) {
    qu_priority =
      std::max(qu_priority, (unsigned int)m_pg->get_cct()->_conf->osd_client_op_priority);
  }
  return qu_priority;
}

unsigned int PgScrubber::scrub_requeue_priority(Scrub::scrub_prio_t with_priority,
						unsigned int suggested_priority) const
{
  if (with_priority == Scrub::scrub_prio_t::high_priority) {
    suggested_priority = std::max(
      suggested_priority, (unsigned int)m_pg->get_cct()->_conf->osd_client_op_priority);
  }
  return suggested_priority;
}

// ///////////////////////////////////////////////////////////////////// //
// scrub-op registration handling

bool PgScrubber::is_scrub_registered() const
{
  return !m_scrub_reg_stamp.is_zero();
}

void PgScrubber::register_with_osd()
{
  //if (is_primary()) {
    // add myself to the OSD's scrub-jobs queue
    m_scrub_reg_stamp = m_osds.get_scrub_services().add_to_osd_queue(
      m_pg_id, ceph_clock_now(), 0.0, 0.0, ScrubQueue::must_scrub_t::not_mandatory);
  //}
}

void PgScrubber::unregister_from_osd()
{
  m_osds.get_scrub_services().remove_from_osd_queue(m_pg_id);
}

#if 0
void PgScrubber::reg_next_scrub(const requested_scrub_t& request_flags)
{
  if (!is_primary()) {
    // normal. No warning is required.
    return;
  }

  logger().debug("{}: pg({}) planned: must? {}. need-auto? {}. stamp: {}", __func__, m_pg_id, request_flags.must_scrub,
		 request_flags.need_auto,
		 m_pg->get_peering_state().get_info().history.last_scrub_stamp);

  ceph_assert(!is_scrub_registered());

  utime_t reg_stamp;
  ScrubQueue::must_scrub_t must{ScrubQueue::must_scrub_t::not_mandatory};

  if (request_flags.must_scrub || request_flags.need_auto) {
    // Set the smallest time that isn't utime_t()
    reg_stamp = PgScrubber::scrub_must_stamp();
    must = ScrubQueue::must_scrub_t::mandatory;
  } else if (/* RRR m_pg->info.stats.stats_invalid && */ false /* RRR for now */ && 
	     m_pg->get_cct()->_conf->osd_scrub_invalid_stats) {
    reg_stamp = ceph_clock_now();
    must = ScrubQueue::must_scrub_t::mandatory;
  } else {
    reg_stamp = m_pg->get_peering_state().get_info().history.last_scrub_stamp;
  }

  logger().debug("{}: pg({}) must: {} required:{} flags: {} stamp: {}",  __func__, m_pg_id, must,
		 m_flags.required, request_flags, reg_stamp);

  const double scrub_min_interval =
    m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
  const double scrub_max_interval =
    m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);

  // note the sched_time, so we can locate this scrub, and remove it later
  m_scrub_reg_stamp = m_osds.m_scrub_queue.reg_pg_scrub(
    m_pg_id, reg_stamp, scrub_min_interval, scrub_max_interval, must);
  logger().debug("{}: pg({}) register next scrub, scrub time {}, must = {}",  __func__, m_pg_id,
		 m_scrub_reg_stamp, must);
}
#endif


ScrubQueue::sched_params_t PgScrubber::determine_scrub_time(const requested_scrub_t& request_flags)
{
  ScrubQueue::sched_params_t res;

  if (!is_primary()) {
    return res; // with ok_to_scrub set to 'false'
  }

  if (request_flags.must_scrub || request_flags.need_auto) {

    // Set the smallest time that isn't utime_t()
    res.suggested_stamp = PgScrubber::scrub_must_stamp();
    res.is_must = ScrubQueue::must_scrub_t::mandatory;
    // we do not need the interval data in this case

  } else if (m_pg->get_peering_state().get_info().stats.stats_invalid &&
							 m_pg->get_cct()->_conf->osd_scrub_invalid_stats) {
    res.suggested_stamp = ceph_clock_now();
    res.is_must = ScrubQueue::must_scrub_t::mandatory;

  } else {
    res.suggested_stamp = m_pg->get_peering_state().get_info().history.last_scrub_stamp;
    res.min_interval =
      m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
    res.max_interval =
      m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  }

  return res;
}

#if 0
void PgScrubber::unreg_next_scrub()
{
  logger().debug("{}: existing: {}. was registered? {}",  __func__, m_scrub_reg_stamp,
		 is_scrub_registered());
#if 0
  if (is_scrub_registered()) {
    m_osds.m_scrub_queue.unreg_pg_scrub(m_pg_id, m_scrub_reg_stamp);
    m_scrub_reg_stamp = utime_t{};
  }
#endif
  m_osds.m_scrub_queue.remove_from_osd_queue(m_pg_id);
}
#endif

#if 0
void PgScrubber::scrub_requested(scrub_level_t scrub_level,
				 scrub_type_t scrub_type,
				 requested_scrub_t& req_flags)
{
  logger().debug(
    "{}: {} {}. Prev stamp: {} registered? {}", __func__,
    (scrub_level == scrub_level_t::deep ? " deep " : " shallow "),
    (scrub_type == scrub_type_t::do_repair ? " repair-scrub " : " not-repair "),
    m_scrub_reg_stamp, is_scrub_registered());

  unreg_next_scrub();

  req_flags.must_scrub = true;
  req_flags.must_deep_scrub =
    (scrub_level == scrub_level_t::deep) || (scrub_type == scrub_type_t::do_repair);
  req_flags.must_repair = (scrub_type == scrub_type_t::do_repair);
  req_flags.need_auto = false;
  req_flags.req_scrub = true;

  logger().debug("{}: pg({}) planned: {}", __func__, m_pg_id, req_flags);

  reg_next_scrub(req_flags);
}
#else
void PgScrubber::scrub_requested(scrub_level_t scrub_level,
				 scrub_type_t scrub_type,
				 requested_scrub_t& req_flags)
{
  logger().debug(
    "{}: {} {}. Prev stamp: {} registered? {}", __func__,
    (scrub_level == scrub_level_t::deep ? " deep " : " shallow "),
    (scrub_type == scrub_type_t::do_repair ? " repair-scrub " : " not-repair "),
    m_scrub_reg_stamp, is_scrub_registered());

  m_scrub_reg_stamp = m_osds.m_scrub_queue.update_scrub_job(
  	m_pg_id, m_scrub_reg_stamp,
	[this, &req_flags, scrub_level, scrub_type]() mutable -> ScrubQueue::sched_params_t {

	  req_flags.must_scrub = true;
	  req_flags.must_deep_scrub =
	    (scrub_level == scrub_level_t::deep) || (scrub_type == scrub_type_t::do_repair);
	  req_flags.must_repair = (scrub_type == scrub_type_t::do_repair);
	  req_flags.need_auto = false;
	  req_flags.req_scrub = true;

	  logger().debug("scrub_requested: pg({}) planned: {}", m_pg_id, req_flags);
	  return determine_scrub_time(req_flags);
	});
  logger().debug("{}: pg({}): scrub set to {}", __func__, m_pg_id, m_scrub_reg_stamp);
}

#endif
// "un-queue" our scrub job; manipulate req_flags; recompute the desired scrub time; re-queue



#if 0
void PgScrubber::request_rescrubbing(requested_scrub_t& req_flags)
{
  logger().debug("{}: existing: {}. was registered? {}", __func__, m_scrub_reg_stamp,
		 is_scrub_registered());

  unreg_next_scrub();
  req_flags.need_auto = true;
  reg_next_scrub(req_flags);
}

#else

void PgScrubber::request_rescrubbing(requested_scrub_t& req_flags)
{
  logger().debug("{}: existing: {}. was registered? {}", __func__, m_scrub_reg_stamp,
		 is_scrub_registered());

  // "un-queue" our scrub job; manipulate req_flags; recompute the desired scrub time; re-queue
  m_scrub_reg_stamp = m_osds.m_scrub_queue.update_scrub_job(m_pg_id, m_scrub_reg_stamp,
  	[this, &req_flags]() mutable -> ScrubQueue::sched_params_t {
	  req_flags.need_auto = true;
	  return determine_scrub_time(req_flags);
	});
}
#endif

bool PgScrubber::reserve_local()
{
  // try to create the reservation object (which translates into asking the
  // OSD for the local scrub resource). If failing - undo it immediately

  m_local_osd_resource.emplace(m_pg, m_osds.m_scrub_queue);
  if (!m_local_osd_resource->is_reserved()) {
    m_local_osd_resource.reset();
    return false;
  }

  return true;
}

// ----------------------------------------------------------------------------

bool PgScrubber::has_pg_marked_new_updates() const
{
  auto last_applied = m_pg->peering_state.get_last_update_applied();
  logger().debug("{}: recovery last: {} vs. scrub's: {}", __func__, last_applied,
		 m_subset_last_update);

  return last_applied >= m_subset_last_update;
}

void PgScrubber::set_subset_last_update(eversion_t e)
{
  m_subset_last_update = e;
  logger().debug("{}: last-update: {}", __func__, e);
}


/*
 * setting:
 * - m_subset_last_update
 * - m_max_end
 * - end
 * - start
 * By:
 * - setting tentative range based on conf and divisor
 * - requesting a partial list of elements from the backend;
 * - handling some head/clones issues
 *
 * The selected range is set directly into 'm_start' and 'm_end'
 */
seastar::future<bool> PgScrubber::select_range()
{
  m_primary_scrubmap = ScrubMap{};
  m_be->new_chunk();

  /* get the start and end of our scrub chunk
   *
   * Our scrub chunk has an important restriction we're going to need to
   * respect. We can't let head be start or end.
   * Using a half-open interval means that if end == head,
   * we'd scrub/lock head and the clone right next to head in different
   * chunks which would allow us to miss clones created between
   * scrubbing that chunk and scrubbing the chunk including head.
   * This isn't true for any of the other clones since clones can
   * only be created "just to the left of" head.  There is one exception
   * to this: promotion of clones which always happens to the left of the
   * left-most clone, but promote_object checks the scrubber in that
   * case, so it should be ok.  Also, it's ok to "miss" clones at the
   * left end of the range if we are a tier because they may legitimately
   * not exist (see _scrub).
   */
  int min_idx = std::max<int64_t>(
    3, m_pg->get_cct()->_conf->osd_scrub_chunk_min / preemption_data.chunk_divisor());

  int max_idx = std::max<int64_t>(min_idx, m_pg->get_cct()->_conf->osd_scrub_chunk_max /
					     preemption_data.chunk_divisor());

  logger().debug("Scrubber: {}: Min: {} Max: {} Div: {}", __func__, min_idx, max_idx,
		 preemption_data.chunk_divisor());

  hobject_t start = m_start;
  hobject_t candidate_end;

  return m_pg->get_backend().list_objects(start, max_idx).then([this](auto obj_n_next) {
    auto& [objects, candidate_end] = obj_n_next;
    if (objects.empty()) {
      logger().debug("select_range(): empty range NOT IMPLEMENTED");
      // return seastar::make_ready_future<bool>(false);
    }
    hobject_t back = objects.back();
    while (candidate_end.is_head() && candidate_end == back.get_head()) {
      candidate_end = back;
      objects.pop_back();
      if (objects.empty()) {
	ceph_assert(0 ==
		    "Somehow we got more than 2 objects which"
		    "have the same head but are not clones");
      }
      back = objects.back();
    }

    if (candidate_end.is_head()) {
      ceph_assert(candidate_end != back.get_head());
      candidate_end = candidate_end.get_object_boundary();
    }

    // is that range free for us? if not - we will be rescheduled later by whoever
    // triggered us this time

    if (!m_pg->range_available_for_scrub(m_start, candidate_end)) {
      // we'll be requeued by whatever made us unavailable for scrub
      logger().debug(
	"PgScrubber::select_range(): scrub blocked somewhere in range [{} , {})", m_start,
	candidate_end);
      return seastar::make_ready_future<bool>(false);
    }

    m_end = candidate_end;
    if (m_end > m_max_end)
      m_max_end = m_end;

    logger().debug("select_range(): range selected: {} //// {} //// {}", m_start, m_end,
		   m_max_end);
    return seastar::make_ready_future<bool>(true);
  });
}

void PgScrubber::select_range_n_notify()
{
  std::ignore = select_range().then([this](bool got_chunk) {
    if (got_chunk) {

      logger().debug("select_range_n_notify(): selection OK");

      queue_local_trigger(&ScrubPgIF::send_chunk_free, m_pg->get_osdmap_epoch(), 0ms,
			  "SelectedChunkFree");

    } else {

      logger().debug("select_range_n_notify(): selected chunk is busy");
      queue_local_trigger(&ScrubPgIF::send_chunk_busy, m_pg->get_osdmap_epoch(), 0ms,
			  "SelectedChunkBusy");
    }
  });
}



bool PgScrubber::write_blocked_by_scrub(const hobject_t& soid)
{
  if (soid < m_start || soid >= m_end) {
    return false;
  }

  logger().debug("scrubber: {}: {} can preempt? {} already preempted? {}", __func__, soid,
		 preemption_data.is_preemptable(), preemption_data.was_preempted());

  if (preemption_data.is_preemptable()) {

    if (!preemption_data.was_preempted()) {
      logger().debug("{} preempted", soid);

      // signal the preemption
      preemption_data.do_preempt();

    } else {
      logger().debug("{} already preempted", soid);
    }
    return false;
  }
  return true;
}

bool PgScrubber::range_intersects_scrub(const hobject_t& start, const hobject_t& end)
{
  // does [start, end] intersect [scrubber.start, scrubber.m_max_end)
  return (start < m_max_end && end >= m_start);
}

/**
 *  if we are required to sleep:
 *	arrange a callback sometimes later.
 *	be sure to be able to identify a stale callback.
 *  Otherwise: "requeue" (i.e. - send an FSM event) immediately.
 */
void PgScrubber::add_delayed_scheduling()  // replace with an errorator, if returning anything
{
  milliseconds sleep_time{0ms};
  if (m_needs_sleep) {
    double scrub_sleep =
      1000.0 * m_osds.get_scrub_services().scrub_sleep_time(m_flags.required);
    sleep_time = milliseconds{long(scrub_sleep)};
  }
  logger().debug(" sleep: {} ms. needed? {} this:{:p}", sleep_time.count(), m_needs_sleep,
		 (void*)this);

  // MessageRef resched_event_msg = make_message<PGScrubResched>(m_pg,
  // m_pg->get_osdmap_epoch());

  if (sleep_time.count()) {

    // schedule a transition for some 'sleep_time' ms in the future

    m_needs_sleep = false;
    m_sleep_started_at = ceph_clock_now();

    // create a ref to this PG
    auto this_pg = m_pg;  // RRR to be replaced with a counted ref


    // the following log line is used by osd-scrub-test.sh
    logger().debug("scrubber: {} scrub state is PendingTimer, sleeping", __func__);

    std::ignore = seastar::sleep(sleep_time).then([scrbr = this, this_pg]() {
      // was there an interval change in the meantime?

      /* RRR just for now */ std::ignore = this_pg;

      if (false /* interval changed (and maybe other PG status checks?) */) { // RRR high
	// RRR lgeneric_subdout(g_ceph_context, osd, 10)
	// RRR   << "scrub_requeue_callback: Could not find "
	// RRR  << "PG " << pgid << " can't complete scrub requeue after sleep" );
	return seastar::make_ready_future<bool>(false);
      }

      scrbr->m_needs_sleep = true;
      scrbr->m_sleep_started_at = utime_t{};
      scrbr->queue_local_trigger(&ScrubPgIF::send_scrub_resched,
				 scrbr->m_pg->get_osdmap_epoch(), 0ms,
				 "InternalSchedScrub");
      return seastar::make_ready_future<bool>(true);
    });

  } else {

    // just a requeue

    queue_local_trigger(&ScrubPgIF::send_scrub_resched, m_pg->get_osdmap_epoch(), 1ms,
			"InternalSchedScrub"); // the 1ms is temp for debugging
  }
}

#if 0
void PgScrubber::add_delayed_scheduling()
{
  milliseconds sleep_time{0ms};
  if (m_needs_sleep) {
    double scrub_sleep = 1000.0 * m_osds.get_scrub_queuing().scrub_sleep_time(m_flags.required);
    sleep_time = milliseconds{long(scrub_sleep)};
  }
  logger().debug(" sleep: {} ms. needed? {}", sleep_time.count(), m_needs_sleep);

  if (sleep_time.count()) {
    // schedule a transition for some 'sleep_time' ms in the future

    m_needs_sleep = false;
    m_sleep_started_at = ceph_clock_now();

    // the following log line is used by osd-scrub-test.sh
    logger().debug(" scrub state is PendingTimer, sleeping");

    // the 'delayer' for crimson is different. Will be factored out.

    spg_t pgid = m_pg->get_pgid();
    auto callbk = new LambdaContext(
      [osds = m_osds, pgid, scrbr = this]([[maybe_unused]] int r) mutable {
	PGRef pg = osds->osd->lookup_lock_pg(pgid);
	auto pg = osds->get_pg(pgid);
	if (!pg) {
	  // RRR lgeneric_subdout(g_ceph_context, osd, 10)
	  // RRR   << "scrub_requeue_callback: Could not find "
	  // RRR  << "PG " << pgid << " can't complete scrub requeue after sleep" );
	  return;
	}
	scrbr->m_needs_sleep = true;
	// RRR lgeneric_dout(scrbr->get_pg_cct(), 7)
	// RRR << "scrub_requeue_callback: slept for "
	// RRR << ceph_clock_now() - scrbr->m_sleep_started_at << ", re-queuing scrub" );

	scrbr->m_sleep_started_at = utime_t{};
	osds->queue_for_scrub_resched(&(*pg), Scrub::scrub_prio_t::low_priority);
	pg->unlock();
      });

    std::lock_guard l(m_osds.sleep_lock);
    m_osds.sleep_timer.add_event_after(sleep_time.count() / 1000.0f, callbk);

  } else {
    // just a requeue
    m_osds.queue_for_scrub_resched(m_pg, Scrub::scrub_prio_t::high_priority);
  }
}
#endif

eversion_t PgScrubber::search_log_for_updates() const
{
  /* RRR
    auto& projected = m_pg->get_peering_state().get_projected_log().log;
    auto pi = find_if(
      projected.crbegin(), projected.crend(),
      [this](const auto& e) -> bool { return e.soid >= m_start && e.soid < m_end; });

    if (pi != projected.crend())
      return pi->version;

    // there was no relevant update entry in the log

    auto& log = m_pg->peering_state.get_pg_log().get_log().log;
    auto p = find_if(log.crbegin(), log.crend(), [this](const auto& e) -> bool {
      return e.soid >= m_start && e.soid < m_end;
    });

    if (p == log.crend())
      return eversion_t{};
    else
      return p->version;

      */
  return eversion_t{};
}

// RRR am I correct in using std::ignore here?

void PgScrubber::get_replicas_maps(bool replica_can_preempt)
{
  logger().debug(
    "scrubber: {} started in epoch/interval:  {}/ {} pg same_interval_since: {}",
    __func__, m_epoch_start, m_interval_start, m_pg->get_same_interval_since());

  bool do_have_replicas = false;

  m_primary_scrubmap_pos.reset();
  const auto not_me =
    boost::adaptors::filtered([this](pg_shard_t pgs) { return pgs != m_pg_whoami; });

  // ask replicas to scan and send maps
  std::ignore =
    seastar::parallel_for_each(m_pg->get_acting_recovery_backfill() | not_me, [this,
								      replica_can_preempt,
								      &do_have_replicas](
								       pg_shard_t shard) {
      //if (shard == m_pg->get_pg_whoami())
	//return seastar::make_ready_future<>();

      do_have_replicas = true;
      m_maps_status.mark_replica_map_request(shard);
      return request_scrub_map(shard, m_subset_last_update, m_start, m_end, m_is_deep,
			       replica_can_preempt);
    }).finally([this, do_have_replicas]() -> void {
      logger().info("PgScrubber::get_replicas_maps(): replica requests sent");

      /* RRR */ std::ignore = do_have_replicas;

      queue_local_trigger(&ScrubPgIF::send_requests_sent, m_pg->get_osdmap_epoch(), 10ms,
			  "ReplicaRequestsSent");
    });
}

bool PgScrubber::was_epoch_changed() const
{
  // for crimson we have m_pg->get_info().history.same_interval_since
  logger().debug("{}: epoch_start:{}  from pg:{}", __func__, m_interval_start,
		 m_pg->get_same_interval_since());

  return m_interval_start < m_pg->get_same_interval_since();
}

void PgScrubber::mark_local_map_ready()
{
  m_maps_status.mark_local_map_ready();
}

bool PgScrubber::are_all_maps_available() const
{
  return m_maps_status.are_all_maps_available();
}

std::string PgScrubber::dump_awaited_maps() const
{
  return m_maps_status.dump();
}

seastar::future<> PgScrubber::request_scrub_map(pg_shard_t replica,
						eversion_t version,
						hobject_t start,
						hobject_t end,
						bool deep,
						bool allow_preemption)
{
  ceph_assert(replica != m_pg_whoami);
  logger().debug("{}: requesting scrubmap from osd.{} {} (c@ep{})", __func__, replica,
		 (deep ? " deep" : " shallow"), get_osdmap_epoch());

  auto rep_scrub_op = make_message<MOSDRepScrub>(
    spg_t{m_pg_id.pgid, replica.shard}, version, get_osdmap_epoch(),
    m_pg->get_last_peering_reset(), start, end, deep, allow_preemption, m_flags.priority,
    false /* RRR todo m_pg->ops_blocked_by_scrub()*/);

  return m_osds.send_to_osd(replica.osd, rep_scrub_op, get_osdmap_epoch());
}

void PgScrubber::cleanup_store(ObjectStore::Transaction* t)
{
  if (!m_store)
    return;

  struct OnComplete : Context {
    std::unique_ptr<Scrub::Store> store;
    explicit OnComplete(std::unique_ptr<Scrub::Store>&& store) : store(std::move(store))
    {}
    void finish(int) override {}
  };
  m_store->cleanup(t);
  t->register_on_complete(new OnComplete(std::move(m_store)));
  ceph_assert(!m_store);
}

void PgScrubber::on_init()
{
  // going upwards from 'inactive'
  ceph_assert(!is_scrub_active());

  preemption_data.reset();
  m_pg->publish_stats_to_osd();
  m_interval_start = m_pg->get_same_interval_since();

  logger().debug("{}: start same_interval: {} (ep.: {})", __func__, m_interval_start,
		 m_pg->get_osdmap_epoch());

  m_be = std::make_unique<Scrub::ScrubberBE>(*this, m_pg->get_backend(), m_pg_whoami,
					     state_test(PG_STATE_REPAIR),
					     &m_primary_scrubmap,
					     m_pg->get_acting_recovery_backfill());

  //  create a new store
  ObjectStore::Transaction t;
  cleanup_store(&t);
  m_store.reset(Scrub::Store::create(
    &m_osds.get_store(), &t, m_pg_id,
    m_pg->get_collection_ref()->get_cid()));  // RRR find which type of store is needed

  // RRR  Scrub::Store::create(m_pg->osd->store, &t, m_pg->info.pgid, m_pg->coll));
  std::ignore = m_osds.get_store()
		  .do_transaction(m_pg->get_collection_ref(), std::move(t))
		  .then([this]() {
		    m_start = m_pg->get_pgid().pgid.get_hobj_start();
		    m_active = true;
		    queue_local_trigger(&ScrubPgIF::send_oninit_done,
					m_pg->get_osdmap_epoch(), 0ms, "OnInitDone");
		  });
}


void PgScrubber::on_replica_init()
{
  m_active = true;
}

void PgScrubber::_scan_snaps(ScrubMap& smap)
{
  logger().debug("Scrubber: _scan_snaps - NOT IMPLEMENTED!!");

#if 0
  hobject_t head;
  SnapSet snapset;

  // Test qa/standalone/scrub/osd-scrub-snaps.sh greps for the strings
  // in this function
  logger().debug("_scan_snaps starts");

  for (auto i = smap.objects.rbegin(); i != smap.objects.rend(); ++i) {

    const hobject_t& hoid = i->first;
    ScrubMap::object& o = i->second;

    logger().debug("{}: {}", __func__, hoid);

    ceph_assert(!hoid.is_snapdir());
    if (hoid.is_head()) {
      // parse the SnapSet
      bufferlist bl;
      if (o.attrs.find(SS_ATTR) == o.attrs.end()) {
	continue;
      }
      bl.push_back(o.attrs[SS_ATTR]);
      auto p = bl.cbegin();
      try {
	decode(snapset, p);
      } catch (...) {
	continue;
      }
      head = hoid.get_head();
      continue;
    }

    if (hoid.snap < CEPH_MAXSNAP) {
      // check and if necessary fix snap_mapper
      if (hoid.get_head() != head) {
	// RRR derr << __func__ << " no head for " << hoid << " (have " << head << ")" );
	continue;
      }
      set<snapid_t> obj_snaps;
      auto p = snapset.clone_snaps.find(hoid.snap);
      if (p == snapset.clone_snaps.end()) {
	// RRR derr << __func__ << " no clone_snaps for " << hoid << " in " << snapset );
	continue;
      }
      obj_snaps.insert(p->second.begin(), p->second.end());
      set<snapid_t> cur_snaps;
      int r = m_pg->snap_mapper.get_snaps(hoid, &cur_snaps);
      if (r != 0 && r != -ENOENT) {
	// RRR derr << __func__ << ": get_snaps returned " << cpp_strerror(r) );
	ceph_abort();
      }
      if (r == -ENOENT || cur_snaps != obj_snaps) {
	ObjectStore::Transaction t;
	OSDriver::OSTransaction _t(m_pg->osdriver.get_transaction(&t));
	if (r == 0) {
	  r = m_pg->snap_mapper.remove_oid(hoid, &_t);
	  if (r != 0) {
	    // RRR derr << __func__ << ": remove_oid returned " << cpp_strerror(r) );
	    ceph_abort();
	  }
	  m_pg->osd->clog->error()
	    << "osd." << m_pg->osd->whoami << " found snap mapper error on pg "
	    << m_pg->info.pgid << " oid " << hoid << " snaps in mapper: " << cur_snaps
	    << ", oi: " << obj_snaps << "...repaired";
	} else {
	  m_pg->osd->clog->error()
	    << "osd." << m_pg->osd->whoami << " found snap mapper error on pg "
	    << m_pg->info.pgid << " oid " << hoid << " snaps missing in mapper"
	    << ", should be: " << obj_snaps << " was " << cur_snaps << " r " << r
	    << "...repaired";
	}
	m_pg->snap_mapper.add_oid(hoid, obj_snaps, &_t);

	// wait for repair to apply to avoid confusing other bits of the system.
	{
	  logger().debug("{}: wait on repair!", __func__);

	  // RRR fix that

	  ceph::condition_variable my_cond;
	  ceph::mutex my_lock = ceph::make_mutex("PG::_scan_snaps my_lock");
	  int e = 0;
	  bool done;

	  t.register_on_applied_sync(new C_SafeCond(my_lock, my_cond, &done, &e));

	  e = m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t));
	  if (e != 0) {
	    // RRR derr << __func__ << ": queue_transaction got " << cpp_strerror(e) );
	  } else {
	    std::unique_lock l{my_lock};
	    my_cond.wait(l, [&done] { return done; });
	  }
	}
      }
    }
  }
#endif
}

seastar::future<> PgScrubber::build_primary_map_chunk()
{
  return build_scrub_map_chunk(m_primary_scrubmap, m_primary_scrubmap_pos, m_start, m_end,
			       m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow);
}

void PgScrubber::initiate_primary_map_build()
{
  epoch_t map_building_since = m_pg->get_osdmap_epoch();
  logger().debug("scrubber: {}() initiated @ep:{}", __func__, map_building_since);

  // RRR handle errors


  std::ignore = build_primary_map_chunk()
		  .then_wrapped([/*this*/](auto&& f) {
		    logger().debug("after bld 1");
		    // if (f.failed())
		    //  (void)f.discard_result();
		    // return std::move(f);
		    return seastar::make_ready_future<>();
		  })
		  .finally([this, map_building_since]() mutable {
		    logger().debug("initiate_primary_map_build(): map built");
		    queue_local_trigger(&ScrubPgIF::send_local_map_done,
					map_building_since, 20ms, "IntLocalMapDone");
		  })
    /*.then_wrapped([=](auto&& f) {
      logger().debug("initiate_primary_map_build(): map built - at the thenw");
      if (f.failed())
	(void)f.discard_result();
      return std::move(f);
    })*/
    ;
}


#if 0
seastar::future<> PgScrubber::build_replica_map_chunk()
{
  logger().debug(" interval start: {}  epoch: {} deep: {}", m_interval_start,
		 m_epoch_start, m_is_deep);

  return build_scrub_map_chunk(replica_scrubmap, replica_scrubmap_pos, m_start, m_end,
			       m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow)
    .then([this]() {
      m_cleaned_meta_map.clear_from(m_start);
      m_cleaned_meta_map.insert(replica_scrubmap);
      auto for_meta_scrub = clean_meta_map();
      return _scan_snaps(for_meta_scrub);
    });

  //     requeue_replica(m_replica_request_priority);
}
#endif

// clang-format off
void PgScrubber::build_replica_map_chunk()
{
  logger().debug("{}: interval start: {} epoch: {} deep: {}", __func__, m_interval_start,
		 m_epoch_start, m_is_deep);

  std::ignore =
    build_scrub_map_chunk(replica_scrubmap, replica_scrubmap_pos, m_start, m_end,
			  m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow)
      .then([this]() {
	   logger().debug("PgScrubber::build_replica_map_chunk(): map built s1");
	   cleaned_meta_map.clear_from(m_start);
	   cleaned_meta_map.insert(replica_scrubmap);
	   auto for_meta_scrub = ScrubberBE::clean_meta_map(cleaned_meta_map, m_end.is_max());
	   return _scan_snaps(for_meta_scrub);

   }).then([this]() {

	   logger().debug("PgScrubber::build_replica_map_chunk(): map built s2");

     // the local map has been created. Send it to the primary.
     // Note: once the message reaches the Primary, it may ask us for another
     // chunk - and we better be done with the current scrub. Thus - the preparation of
     // the reply message is separate, and we clear the scrub state before actually
     // sending it.

     auto reply = prep_replica_map_msg(PreemptionNoted::no_preemption);
 	   replica_handling_done();
	   queue_local_trigger(&ScrubPgIF::send_full_reset, m_interval_start, 0ms,"ReplicaFinalReset");
	   logger().debug("build_replica_map_chunk(): map built");
     send_replica_map(reply);

  	 return seastar::make_ready_future<>();

  }).then_wrapped([this](auto&& f) {

	  if (f.failed()) {
	    logger().warn("scrubber: build_replica_map_chunk() failed!!");
	    (void)f.discard_result();
  	  queue_local_trigger(&ScrubPgIF::send_full_reset, m_interval_start, 0ms,
	  	    	    "ReplicaFinalReset");
	  }
	  return seastar::make_ready_future<>();
  });

  //     requeue_replica(m_replica_request_priority);
}
// clang-format on

/*
 * Look for backup versions of modified objects, that should have been removed
 * sometime in the past (when their new, modified, version was written to all
 * shards). EC-specific.
 */
seastar::future<> PgScrubber::scan_rollback_obs(const vector<ghobject_t>& rollback_obs)
{
  if (rollback_obs.empty()) {
    return seastar::make_ready_future();
  }

  const eversion_t trimmed_to =
    m_pg->get_peering_state().get_last_rollback_info_trimmed_to_applied();

  logger().debug("scrubber: {}: (pg({})) count: {}  trimmed-to: {}", __func__, m_pg_id,
		 rollback_obs.size(), trimmed_to);

  ObjectStore::Transaction t;

  for (const auto& gobj : rollback_obs) {

    if (gobj.generation < trimmed_to.version) {

      logger().info(
	"{}: osd.{} pg {} found obsolete rollback obj {} generation < trimmed_to {} "
	"...repaired",
	__func__, 777, m_pg_id, gobj, trimmed_to);
      t.remove(m_pg->get_collection_ref()->get_cid(), gobj);
    }
  }

  if (t.empty()) {
    return seastar::make_ready_future();
  }

  logger().error(
    "scrubber: {}: (pg({})) queueing trans to clean up obsolete rollback objs", __func__,
    m_pg_id);
  return m_osds.get_store().do_transaction(m_pg->get_collection_ref(), std::move(t));
}


seastar::future<> PgScrubber::repair_oinfo_oid(ScrubMap& smap)
{
  return seastar::do_for_each(
    smap.objects.rbegin(), smap.objects.rend(),
    [/*&smap,*/ this](auto& i) mutable -> seastar::future<> {
      const hobject_t& hoid = i.first;
      ScrubMap::object& o = i.second;

      if (o.attrs.find(OI_ATTR) == o.attrs.end()) {
	return seastar::make_ready_future<>();
      }

      bufferlist bl;
      bl.push_back(o.attrs[OI_ATTR]);  // RRR no direct translation?

      try {
	// decoding the bl into 'oi' might fail
	object_info_t oi{bl};

	if (oi.soid == hoid) {
	  // all's good
	  return seastar::make_ready_future<>();
	}

	// send an error to the osd log. RRR
	logger().error(
	  "osd.{} found object info error on pg {} oid {} oid in object info: {} "
	  "...repaired",
	  0, m_pg_id, hoid, oi.soid);

	// fix object info, and update the scrub-map entry

	oi.soid = hoid;
	bl.clear();
	encode(oi, bl, get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));

	bufferptr bp(bl.c_str(), bl.length());
	o.attrs[OI_ATTR] = bp;

//#ifdef NOT_YET_RRR
	ObjectStore::Transaction t;
	//OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
	t.setattr(m_pg->get_collection_ref()->get_cid(), ghobject_t{hoid}, OI_ATTR, bl);

	return m_osds.get_store().do_transaction(m_pg->get_collection_ref(), std::move(t));
//#endif
	//return seastar::make_ready_future<>();
      } catch (...) {
	return seastar::make_ready_future<>();
      }
    });
  /*
   return store->get_attrs(
      coll,
      ghobject_t{oid, ghobject_t::NO_GEN, shard}).safe_then(
	[oid](auto &&attrs) -> load_metadata_ertr::future<loaded_object_md_t::ref>{
	  loaded_object_md_t::ref ret(new loaded_object_md_t());
	  if (auto oiiter = attrs.find(OI_ATTR); oiiter != attrs.end()) {
	    bufferlist bl;
	    bl.push_back(std::move(oiiter->second));
	    ret->os = ObjectState(
	      object_info_t(bl),
	      true);
	  } else {
	    logger().error(
	      "load_metadata: object {} present but missing object info",
	      oid);
	    return crimson::ct_error::object_corrupted::make();
	  }
   */
}


// we can plan on being called only once per chunk (barring preemption)
//  clang-format off
seastar::future<> PgScrubber::build_scrub_map_chunk(ScrubMap& map,
						    ScrubMapBuilder& pos,
						    hobject_t start,
						    hobject_t end,
						    scrub_level_t depth)
{
  pos.reset();

  pos.deep = depth == scrub_level_t::deep;
  map.valid_through = m_pg->get_peering_state().get_info().last_update;
  logger().debug("scrubber: {}: [{},{}) pos: {} Depth: {}. Valid: {}", __func__, start,
		 end, pos, pos.deep ? "deep" : "shallow", map.valid_through);

  return m_pg->get_backend()
    .list_range(start, end, pos.ls)
    .then([this, /*&map,*/ &pos, start, end](vector<ghobject_t> rollback_obs)
	                                   mutable -> seastar::future<> {

      logger().debug("PgScrubber::build_scrub_map_chunk() - debug point 37");

      if (pos.empty()) {
	logger().debug("PgScrubber::build_scrub_map_chunk() - dp37 - empty");
        return seastar::make_ready_future<>();
      }

      // check for & remove obsolete gen objects

      // not sure I understand the logic here. Why do we need to loop on the
      // list_range() if ls wasn't empty? or:
      // to ask: why no need to scrub rollbacks if 'ls' is empty?

      return scan_rollback_obs(rollback_obs); // EC-specific
    })
    .then([this, &map, &pos]() mutable -> seastar::future<> {
      logger().debug("PgScrubber::build_scrub_map_chunk() - sz:{} debug point 59",
      			pos.ls.size());
      pos.pos = 0;

      // scan the objects

      return m_pg->get_backend().scan_list(map, pos).safe_then(
    [this, &map/*, &pos*/]() mutable -> seastar::future<> {

        logger().debug("PgScrubber::build_scrub_map_chunk(): before repair_oinfo {}",
		       map.objects.size());

	// debug-print some of the entries
        {
          static int dbg_cc{4};
          if (!map.objects.empty() && (dbg_cc-- > 0)) {

            const auto&  [k, v] = *map.objects.cbegin();
            for (const auto& [ak, av] : v.attrs) {
              logger().debug(" BSMC-1/{}: {} - {}", k, ak, av);
	    }

	  }
        }

        return repair_oinfo_oid(map);
      },
    PGBackend::ll_read_errorator::all_same_way([]() {
        // RRR complete the error handling here

        logger().debug("PgScrubber::build_scrub_map_chunk() in error handling");
        // return PGBackend::stat_errorator::make_ready_future(); })
        return seastar::make_ready_future<>();
      }));
    })
    .then([&map]() {

      logger().debug("PgScrubber::build_scrub_map_chunk(): done. Got {} items",
   		       map.objects.size());
      return seastar::make_ready_future<>();
    });
}
//  clang-format on


void PgScrubber::run_callbacks()
{
  logger().debug("scrubber: {}: pg({}) cbs:{}", __func__, m_pg_id, m_callbacks.size());

  std::list<Context*> to_run;
  to_run.swap(m_callbacks);

  for (auto& tr : to_run) {
    tr->complete(0);
  }
}


void PgScrubber::maps_compare_n_cleanup()
{
  logger().debug("scrubber: {}: pg({})", __func__, m_pg_id);
  std::ignore = m_be->scrub_compare_maps(m_end.is_max()).
  	then([this]() mutable {
    m_start = m_end;
    run_callbacks();
    requeue_waiting();
    queue_local_trigger(&ScrubPgIF::send_maps_compared, get_osdmap_epoch(), 0ms,
			"MapsCompared");
    return seastar::make_ready_future<>();
  });
}

Scrub::preemption_t& PgScrubber::get_preemptor()
{
  return preemption_data;
}


void PgScrubber::handle_scrub_map_request(const MOSDRepScrub& msg, pg_shard_t from)
{
  auto request_ep = msg.get_map_epoch();
  logger().debug("{}: {} (ep: {}) from {}", __func__, msg.get_desc(), request_ep, from);

  // logger().debug("{}: pg:{} Msg: map_epoch: {} min_epoch: {} deep? {}", __func__,
  //	 m_pg->get_pgid(), msg.map_epoch, msg.min_epoch, msg.deep);

  // are we still processing a previous scrub-map request without noticing that
  // the interval changed? won't see it here, but rather at the reservation
  // stage.

#ifdef NOT_YET__NEED_SAME_INTERVAL
  if (req.map_epoch < m_pg->get_same_interval_since()) {
    logger().debug("{}: replica_scrub_op discarding old replica_scrub from {} < {}",
		   __func__, msg.map_epoch, m_pg->get_same_interval_since());

    // is there a general sync issue? are we holding a stale reservation?
    // not checking now - assuming we will actively react to interval change.

    return;
  }
#endif

  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos = ScrubMapBuilder{};

  m_replica_min_epoch = msg.min_epoch;
  m_start = msg.start;
  m_end = msg.end;
  m_max_end = msg.end;
  m_is_deep = msg.deep;
  m_interval_start = m_pg->get_same_interval_since();
  m_replica_request_priority = msg.high_priority ? Scrub::scrub_prio_t::high_priority
						 : Scrub::scrub_prio_t::low_priority;
  m_flags.priority =
    msg.priority ? msg.priority : m_pg->m_scrub_sched->get_scrub_priority();

  preemption_data.reset();
  preemption_data.force_preemptability(msg.allow_preemption);

  replica_scrubmap_pos.reset();

  // make sure the FSM is at NotActive
  m_fsm->assert_not_active();

  // RRR todo in the scheduler:  scrub_queued = false;

  queue_local_trigger(&ScrubPgIF::send_start_replica, m_replica_min_epoch, 10ms,
		      "StartReplicaNoWait");
}

void PgScrubber::set_op_parameters(requested_scrub_t& request)
{
  logger().debug("{}: input: {}", __func__, request);

  // write down the epoch of starting a new scrub. Will be used
  // to discard stale messages from previous aborted scrubs.
  m_epoch_start = m_pg->get_osdmap_epoch();

  m_flags.check_repair = request.check_repair;
  m_flags.auto_repair = request.auto_repair || request.need_auto;
  m_flags.required = request.req_scrub || request.must_scrub;

  m_flags.priority = (request.must_scrub || request.need_auto)
		       ? get_pg_cct()->_conf->osd_requested_scrub_priority
		       : m_pg->m_scrub_sched->get_scrub_priority();

  state_set(PG_STATE_SCRUBBING);

  // will we be deep-scrubbing?
  if (request.must_deep_scrub || request.need_auto || request.time_for_deep) {
    state_set(PG_STATE_DEEP_SCRUB);
  }

  if (request.must_repair || m_flags.auto_repair) {
    state_set(PG_STATE_REPAIR);
  }

  // the publishing here seems to be required for tests synchronization
  m_pg->publish_stats_to_osd();
  m_flags.deep_scrub_on_error = request.deep_scrub_on_error;
  request = requested_scrub_t{};
}

ScrubMachineListener::MsgAndEpoch PgScrubber::prep_replica_map_msg(
  PreemptionNoted was_preempted)
{
  logger().debug("{}: min epoch: {}", __func__, m_replica_min_epoch);

  auto reply =
    make_message<MOSDRepScrubMap>(spg_t(m_pg->get_info().pgid.pgid, m_pg->get_primary().shard),
				  m_replica_min_epoch, m_pg_whoami);

  reply->preempted = (was_preempted == PreemptionNoted::preempted);
  ::encode(replica_scrubmap, reply->get_data());

  return ScrubMachineListener::MsgAndEpoch{reply, m_replica_min_epoch};
}

void PgScrubber::send_replica_map(const MsgAndEpoch& preprepared)
{
  m_pg->send_cluster_message(m_pg->get_primary().osd, preprepared.m_msg,
			     preprepared.m_epoch, false);
}

void PgScrubber::send_preempted_replica()
{
  auto reply =
    make_message<MOSDRepScrubMap>(spg_t{m_pg->get_info().pgid.pgid, m_pg->get_primary().shard},
				  m_replica_min_epoch, m_pg_whoami);

  reply->preempted = true;
  ::encode(replica_scrubmap, reply->get_data());  // can we skip this? RRR
  m_pg->send_cluster_message(m_pg->get_primary().osd, reply, m_replica_min_epoch, false);
}


#if 0
/**
 * Send the requested map back to the primary (or - if we
 * were preempted - let the primary know).
 */
void PgScrubber::send_replica_map(::crimson::osd::Scrub::PreemptionNoted was_preempted)
{
  logger().debug("{}: min epoch: {}/{}", __func__, m_replica_min_epoch,
		 get_osdmap_epoch());

  auto reply = make_message<MOSDRepScrubMap>(m_pg_id, m_replica_min_epoch, m_pg_whoami);
  reply->preempted = (was_preempted == PreemptionNoted::preempted);
  ::encode(replica_scrubmap, reply->get_data());

  m_pg->send_cluster_message(m_pg->get_primary().osd, std::move(reply),
			     get_osdmap_epoch() /*m_replica_min_epoch*/);
}
#endif

/*
 *  - if the replica lets us know it was interrupted, we mark the chunk as
 * interrupted. The state-machine will react to that when all replica maps are
 * received.
 *  - when all maps are received, we signal the FSM with the GotReplicas event
 * (see scrub_send_replmaps_ready()). Note that due to the no-reentrancy
 * limitations of the FSM, we do not 'process' the event directly. Instead - it
 * is queued for the OSD to handle.
 */
void PgScrubber::map_from_replica(const MOSDRepScrubMap& m, pg_shard_t from)
{
  logger().debug("{}: {} from {}", __func__, m.get_desc(), from);

  if (m.get_map_epoch() < m_pg->get_same_interval_since()) {
    logger().debug("{}: discarding old from {} < {}", __func__, m.get_map_epoch(),
		   m_pg->get_same_interval_since());
    return;
  }

  // note: we check for active() before map_from_replica() is called. Thus, we
  // know m_be is initialized
  m_be->decode_received_map(from, m, m_pg->get_info().pgid.pool());

//  auto p = const_cast<bufferlist&>(m.get_data()).cbegin();
//  m_received_maps[m.from].decode(p, m_pg->get_info().pgid.pool());
//  logger().debug("{}: map version is {} ({})", __func__,
//		 m_received_maps[m.from].valid_through, m.get_map_epoch());

  auto [is_ok, err_txt] = m_maps_status.mark_arriving_map(m.from);
  if (!is_ok) {
    // previously an unexpected map was triggering an assert. Now, as scrubs can
    // be aborted at any time, the chances of this happening have increased, and
    // aborting is not justified
    logger().debug("{}: {} from OSD {}", __func__, err_txt, m.from);
    return;
  }

  if (m.preempted) {
    logger().debug("{}: replica was preempted, setting flag", __func__);
    ceph_assert(preemption_data.is_preemptable());  // otherwise - how dare the replica!
    preemption_data.do_preempt();
  }

  if (m_maps_status.are_all_maps_available()) {
    logger().debug("{}: all repl-maps available", __func__);
    logger().debug("{}: temp debug {}", __func__, m_pg->get_osdmap_epoch());
    queue_local_trigger(&ScrubPgIF::send_replica_maps_ready,
			/* temp fix RRR m.get_map_epoch() */ get_osdmap_epoch(), 0ms,
			"GotReplicas");
  }
}

// void PgScrubber::handle_scrub_reserve_request(Ref<MOSDScrubReserve> req, pg_shard_t
// from)
void PgScrubber::handle_scrub_reserve_request(const MOSDScrubReserve& req,
					      pg_shard_t from)
{
  logger().debug("{}: {} from {}", __func__, req.get_desc(), from);

  auto request_ep = req.get_map_epoch();

  /*
   *  if we are currently holding a reservation, then:
   *  either (1) we, the scrubber, did not yet notice an interval change. The
   * remembered reservation epoch is from before our interval, and we can
   * silently discard the reservation (no message is required). or: (2) the
   * interval hasn't changed, but the same Primary that (we think) holds the
   *  lock just sent us a new request. Note that we know it's the same Primary,
   * as otherwise the interval would have changed. Ostensibly we can discard &
   * redo the reservation. But then we will be temporarily releasing the OSD
   * resource - and might not be able to grab it again. Thus we simple treat
   * this as a successful new request.
   */

  if (m_remote_osd_resource.has_value() && m_remote_osd_resource->is_stale()) {
    // we are holding a stale reservation from a past epoch
    m_remote_osd_resource.reset();
  }

  // check if still relevant

  if (request_ep < m_pg->get_same_interval_since()) {
    // will not ack stale requests
    logger().debug("scrubber: {}: obsolete request (ep {} vs. {}) ignored", __func__,
		   m_pg->get_same_interval_since(), request_ep);
    return;  // seastar::make_ready_future<>();
  }

  bool granted{false};
  if (m_remote_osd_resource.has_value()) {

    logger().debug("{}: already reserved.", __func__);
    granted = true;

  } else if (get_pg_cct()->_conf->osd_scrub_during_recovery ||
	     !m_osds.is_recovery_active()) {
    m_remote_osd_resource.emplace(m_pg, m_osds.m_scrub_queue, request_ep);
    // OSD resources allocated?
    granted = m_remote_osd_resource->is_reserved();
    if (!granted) {
      // just forget it
      m_remote_osd_resource.reset();
      logger().debug("{}: failed to reserve remotely", __func__);
    }
  }

  logger().debug("{}: reserved? {}", __func__, (granted ? "yes" : "no"));

  auto reply = make_message<MOSDScrubReserve>(
    spg_t(m_pg_id.pgid, from.shard), request_ep,
    granted ? MOSDScrubReserve::GRANT : MOSDScrubReserve::REJECT, m_pg_whoami);

  (void)m_osds.send_to_osd(from.osd, reply, get_osdmap_epoch());
}


// void PgScrubber::handle_scrub_reserve_grant(RemoteScrubEvent op, pg_shard_t from)
void PgScrubber::handle_scrub_reserve_grant(const MOSDScrubReserve& msg, pg_shard_t from)
{
  logger().debug("{}: {} from {}", __func__, msg.get_desc(), from);

  if (m_reservations.has_value()) {
    m_reservations->handle_reserve_grant(msg, from);
  } else {
    // RRR derr << __func__ << ": received unsolicited reservation grant from osd " <<
    // from RRR 	 << " (" << op << ")" );
  }
}

void PgScrubber::handle_scrub_reserve_reject(const MOSDScrubReserve& msg, pg_shard_t from)
{
  logger().debug("{}: {} from {}", __func__, msg.get_desc(), from);

  if (m_reservations.has_value()) {
    // there is an active reservation process. No action is required otherwise.
    m_reservations->handle_reserve_reject(msg, from);
  }
}

// void PgScrubber::handle_scrub_reserve_release(RemoteScrubEvent op)
void PgScrubber::handle_scrub_reserve_release(const MOSDScrubReserve& msg,
					      pg_shard_t from)
{
  logger().debug("{}: {}", __func__, from /*msg.get_from()*/);
  // op->mark_started();
  m_remote_osd_resource.reset();
}

void PgScrubber::discard_replica_reservations()
{
  logger().debug("{}", __func__);
  if (m_reservations.has_value()) {
    m_reservations->discard_all();
  }
}

void PgScrubber::clear_scrub_reservations()
{
  logger().debug("{}", __func__);
  m_reservations.reset();	  // the remote reservations
  m_local_osd_resource.reset();	  // the local reservation
  m_remote_osd_resource.reset();  // we as replica reserved for a Primary
}

// void PgScrubber::handle_scrub_reserve_op(Ref<MOSDScrubReserve> req, pg_shard_t from)
void PgScrubber::handle_scrub_reserve_op(const MOSDScrubReserve& req, pg_shard_t from)
{
  logger().debug("{}: {} from {}", __func__, req.get_desc(), from);

  switch (req.type) {

    case MOSDScrubReserve::REQUEST:
      // a scrub-resource-reservation request has arrived from our primary
      handle_scrub_reserve_request(req, from);
      break;

    case MOSDScrubReserve::GRANT:
      // the replica has the resources for our scrub requests
      handle_scrub_reserve_grant(req, from);
      break;

    case MOSDScrubReserve::REJECT:
      // the replica has the resources for our scrub requests
      handle_scrub_reserve_reject(req, from);
      break;

    case MOSDScrubReserve::RELEASE:
      // the replica has the resources for our scrub requests
      handle_scrub_reserve_release(req, from);
      break;
  }
}

void PgScrubber::message_all_replicas(int32_t opcode, std::string_view op_text)
{
  ceph_assert(m_pg->peering_state.get_backfill_targets().empty());

  const epoch_t epch = get_osdmap_epoch();

  const auto not_me =
    boost::adaptors::filtered([this](pg_shard_t pgs) { return pgs != m_pg_whoami; });

  for (auto& p : m_pg->get_actingset() | not_me) {

    logger().debug("{}: scrub requesting {} from osd.{} Epoch:{}", __func__, op_text, p,
		   epch);

    auto m = make_message<MOSDScrubReserve>(spg_t{m_pg_id.pgid, p.shard}, epch, opcode,
					    m_pg_whoami);

    m_pg->send_cluster_message(p.osd, std::move(m), epch);
  }
}


void PgScrubber::unreserve_replicas()
{
  logger().debug("{}", __func__);
  m_reservations.reset();
}

// uses BE data members. Consider making part of the BE.
[[nodiscard]] bool PgScrubber::scrub_process_inconsistent()
{
  logger().debug("scrubber {}: pg({}) checking authoritative", __func__, m_pg_id);

  bool repair = state_test(PG_STATE_REPAIR);
  const bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));
  logger().debug("{}: deep_scrub: {} m_is_deep: {} repair: {}", __func__, deep_scrub,
		 m_is_deep, repair);

  // authoritative only store objects which are missing or inconsistent.
  if (!m_authoritative.empty()) {

    auto err_msg = fmt::format("scrubber: {}: pg({}) {} {} missing, {} inconsistent objects",
    	__func__, m_pg_id, mode, m_be->m_missing.size(), m_be->m_inconsistent.size());

    logger().debug("{}", err_msg);
#ifdef NO_CLUSTER_LOGGER_YET
    m_osds.clog->error(err_msg);
#endif

    if (repair) {
      state_clear(PG_STATE_CLEAN);

      for (const auto& [hobj, shrd_list] : m_authoritative) {

	auto missing_entry = m_be->m_missing.find(hobj);

	if (missing_entry != m_be->m_missing.end()) {
	  repair_object(hobj, shrd_list, missing_entry->second);
	  m_fixed_count += missing_entry->second.size();
	}

	if (m_be->m_inconsistent.count(hobj)) {
	  repair_object(hobj, shrd_list, m_be->m_inconsistent[hobj]);
	  m_fixed_count += m_be->m_inconsistent[hobj].size();
	}
      }
    }
  }
  return (!m_authoritative.empty() && repair);
}


// extracted from PrimaryLogScrub::_scrub_finish()

bool PgScrubber::cstat_differs(sum_item_t e, bool is_valid)
{
  return is_valid && (m_scrub_cstat.sum.*e !=
		      m_pg->get_peering_state().get_info().stats.stats.sum.*e);
}

std::string PgScrubber::cstat_diff_txt(sum_item_t e, std::string_view msg)
{
  return fmt::format("{}/{} {}", m_scrub_cstat.sum.*e,
		     m_pg->get_peering_state().get_info().stats.stats.sum.*e, msg);
}

bool PgScrubber::cstat_details_mismatch(std::string_view mode_txt)
{
  auto& stats = m_pg->get_peering_state().get_info().stats;
  const bool discrep =
    cstat_differs(&object_stat_sum_t::num_objects, true) ||
    cstat_differs(&object_stat_sum_t::num_object_clones, true) ||
    cstat_differs(&object_stat_sum_t::num_objects_dirty, !stats.dirty_stats_invalid) ||
    cstat_differs(&object_stat_sum_t::num_objects_omap, !stats.omap_stats_invalid) ||

    cstat_differs(&object_stat_sum_t::num_objects_pinned, !stats.pin_stats_invalid) ||
    cstat_differs(&object_stat_sum_t::num_objects_hit_set_archive,
		  !stats.hitset_stats_invalid) ||
    cstat_differs(&object_stat_sum_t::num_bytes_hit_set_archive,
		  !stats.hitset_bytes_stats_invalid) ||
    cstat_differs(&object_stat_sum_t::num_objects_manifest,
		  !stats.manifest_stats_invalid) ||

    cstat_differs(&object_stat_sum_t::num_whiteouts, true) ||
    cstat_differs(&object_stat_sum_t::num_bytes, true);

  if (discrep) {

    ++m_shallow_errors;

    // log an error
    std::string log_msg =
      fmt::format("{}: {} : stat mismatch, got ", m_pg_id, mode_txt) +
      cstat_diff_txt(&object_stat_sum_t::num_objects, "objects, ") +
      cstat_diff_txt(&object_stat_sum_t::num_object_clones, "clones, ") +
      cstat_diff_txt(&object_stat_sum_t::num_objects_dirty, "dirty, ") +
      cstat_diff_txt(&object_stat_sum_t::num_objects_omap, "omap, ") +
      cstat_diff_txt(&object_stat_sum_t::num_objects_pinned, "pinned, ") +
      cstat_diff_txt(&object_stat_sum_t::num_objects_hit_set_archive,
		     "hit_set_archive, ") +
      cstat_diff_txt(&object_stat_sum_t::num_whiteouts, " whiteouts, ") +
      cstat_diff_txt(&object_stat_sum_t::num_bytes, " bytes, ") +
      cstat_diff_txt(&object_stat_sum_t::num_objects_manifest, " manifest objects, ") +
      cstat_diff_txt(&object_stat_sum_t::num_bytes_hit_set_archive,
		     " hit_set_archive bytes.");
  }

  return discrep;
}

void PgScrubber::verify_cstat(bool repair,
			      scrub_level_t shallow_or_deep,
			      std::string_view mode_txt)
{
  auto& stats = m_pg->get_peering_state().get_info().stats;
  // const bool repair = state_test(PG_STATE_REPAIR);

  // if the whole info.stats object is marked invalid, recover it

  if (stats.stats_invalid) {
    m_pg->get_peering_state().update_stats(
      [=](auto& history, auto& stats) {
	/// the [] is called with PeeringState::info.history & PS::info.stats
	stats.stats = m_scrub_cstat;
	stats.stats_invalid = false;
	return false;
      },
      nullptr);

    // RRR if (m_pg->agent_state)
    // RRR   m_pg->agent_choose_mode();
  }

  // RRR seems to me there cannot be a difference between info-stats
  //  and scrub_stats in the 'stats_invalid' branch
  else {

    // const bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
    // const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));

    if (cstat_details_mismatch(mode_txt) && repair) {

      ++m_fixed_count;
      m_pg->get_peering_state().update_stats(
	[this](auto& history, auto& stats) {
	  stats.stats = m_scrub_cstat;
	  stats.dirty_stats_invalid = false;
	  stats.omap_stats_invalid = false;
	  stats.hitset_stats_invalid = false;
	  stats.hitset_bytes_stats_invalid = false;
	  stats.pin_stats_invalid = false;
	  stats.manifest_stats_invalid = false;
	  return false;	 // RRR ask why not just say' true' and have update_stats()
			 // publish the results
	},
	nullptr);

      m_pg->publish_stats_to_osd();
      m_pg->get_peering_state().share_pg_info();  // RRR understand
    }
  }


  // Clear object context cache to get repair information
  // if (repair)
  //  m_pg->object_contexts.clear();
  // ... RRR
}


void PgScrubber::final_cstat_update(scrub_level_t shallow_or_deep)
{
  logger().debug("scrubber {}(): {}/{} {}", __func__, m_shallow_errors, m_deep_errors, (shallow_or_deep == scrub_level_t::deep));

  // finish up
  ObjectStore::Transaction t;
  m_pg->get_peering_state().update_stats(
    [this, shallow_or_deep](auto& history, auto& stats) {
      logger().debug("m_pg->peering_state.update_stats()");
      utime_t now = ceph_clock_now();
      history.last_scrub = m_pg->get_peering_state().get_info().last_update;
      history.last_scrub_stamp = now;
      if (m_is_deep) {	// RRR why checking this and not the state flag?
	history.last_deep_scrub = m_pg->peering_state.get_info().last_update;
	history.last_deep_scrub_stamp = now;
      }

      if (shallow_or_deep == scrub_level_t::deep) {

	if ((m_shallow_errors == 0) && (m_deep_errors == 0))
	  history.last_clean_scrub_stamp = now;
	stats.stats.sum.num_shallow_scrub_errors = m_shallow_errors;
	stats.stats.sum.num_deep_scrub_errors = m_deep_errors;
	stats.stats.sum.num_large_omap_objects = m_be->m_omap_stats.large_omap_objects;
	stats.stats.sum.num_omap_bytes = m_be->m_omap_stats.omap_bytes;
	stats.stats.sum.num_omap_keys = m_be->m_omap_stats.omap_keys;
	logger().debug("scrub_finish shard {} num_omap_bytes = {} num_omap_keys = {}",
		       m_pg_whoami, stats.stats.sum.num_omap_bytes,
		       stats.stats.sum.num_omap_keys);
      } else {
	stats.stats.sum.num_shallow_scrub_errors = m_shallow_errors;
	// XXX: last_clean_scrub_stamp doesn't mean the pg is not
	// inconsistent because of deep-scrub errors
	if (m_shallow_errors == 0)
	  history.last_clean_scrub_stamp = now;
      }

      stats.stats.sum.num_scrub_errors =
	stats.stats.sum.num_shallow_scrub_errors + stats.stats.sum.num_deep_scrub_errors;
      if (m_flags.check_repair) {
	m_flags.check_repair = false;
	if (m_pg->get_peering_state().get_info().stats.stats.sum.num_scrub_errors) {
	  state_set(PG_STATE_FAILED_REPAIR);
	  logger().debug(
	    "scrub_finish {} error(s) still present after re-scrub",
	    m_pg->get_peering_state().get_info().stats.stats.sum.num_scrub_errors);
	}
      }
      return true;
    },
    &t);

  std::ignore =
    m_osds.get_store().do_transaction(m_pg->get_collection_ref(), std::move(t));

#ifdef NOT_YET_SNAPS
  if (!m_pg->snap_trimq.empty()) {
    logger().debug("scrub finished, requeuing snap_trimmer");
    m_pg->snap_trimmer_scrub_complete();
  }
#endif
}


void PgScrubber::log_results(bool repair,
			     scrub_level_t shallow_or_deep,
			     std::string_view mode_txt)
{
  auto& stats = m_pg->get_peering_state().get_info().stats;

  fmt::memory_buffer out;
  fmt::format_to(out, "{} {}", m_pg_id.pgid, mode_txt);

  int total_errors = m_shallow_errors + m_deep_errors;
  if (total_errors)
    fmt::format_to(out, " {} errors", total_errors);
  else
    fmt::format_to(out, " ok");

  if ((shallow_or_deep == scrub_level_t::shallow) &&
      stats.stats.sum.num_deep_scrub_errors) {
    fmt::format_to(out, " ({} remaining deep scrub error details lost)",
		   stats.stats.sum.num_deep_scrub_errors);
  }

  if (repair) {
    fmt::format_to(out, ", {} fixed", m_fixed_count);
  }

#ifdef NO_CLUSTER_LOGGER_YET
  if (total_errors)
    m_osds.clog->error(fmt::to_string(out));
  else
    m_osds.clog->debug(fmt::to_string(out));
#endif
  if (total_errors)
   logger().error("scrubber: {}: {}", __func__, fmt::to_string(out));
  else
    logger().debug("scrubber: {}: {}", __func__, fmt::to_string(out));
}


/*
 * note: only called for the Primary.
 */
void PgScrubber::scrub_finish()
{
  logger().debug("{}: before flags:{} deep_scrub_on_error:{}", __func__, m_flags,
		 m_flags.deep_scrub_on_error);

  // ceph_assert(m_pg->is_locked());

  // if the repair request comes from auto-repair and large number of errors,
  // we would like to cancel auto-repair

  bool repair = state_test(PG_STATE_REPAIR);
  if (repair && m_flags.auto_repair &&
      m_authoritative.size() > m_pg->get_cct()->_conf->osd_scrub_auto_repair_num_errors) {

    logger().debug("{}: undoing the repair", __func__);
    state_clear(PG_STATE_REPAIR);
    repair = false;
  }

  const bool deep_scrub_flag{state_test(PG_STATE_DEEP_SCRUB)};
  const auto deep_scrub{deep_scrub_flag ? scrub_level_t::deep : scrub_level_t::shallow};
  const char* mode = (repair ? "repair" : (deep_scrub_flag ? "deep-scrub" : "scrub"));

  bool do_auto_scrub{false};

  // if a regular scrub had errors within the limit, do a deep scrub to auto
  // repair
  if (m_flags.deep_scrub_on_error && !m_authoritative.empty() &&
      m_authoritative.size() <= get_pg_cct()->_conf->osd_scrub_auto_repair_num_errors) {

    ceph_assert(!deep_scrub_flag);
    do_auto_scrub = true;
    logger().debug("{}: Try to auto repair after scrub errors", __func__);
  }

  m_flags.deep_scrub_on_error = false;

  verify_cstat(repair, deep_scrub, mode);

  bool has_error = scrub_process_inconsistent();

  log_results(repair, deep_scrub, mode);

  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (repair) {
    if (m_fixed_count == m_shallow_errors + m_deep_errors) {

      ceph_assert(deep_scrub_flag);
      m_shallow_errors = 0;
      m_deep_errors = 0;
      logger().debug("{}: All may be fixed", __func__);

    } else if (has_error) {

      // Deep scrub in order to get corrected error counts
      m_pg->scrub_after_recovery = true;
      m_pg->m_planned_scrub.req_scrub =
	m_pg->m_planned_scrub.req_scrub || m_flags.required;

      logger().debug("{}: Current 'required': {} Planned 'req_scrub': {}", __func__,
		     m_flags.required, m_pg->m_planned_scrub.req_scrub);

    } else if (m_shallow_errors || m_deep_errors) {

      // We have errors but nothing can be fixed, so there is no repair
      // possible.
      state_set(PG_STATE_FAILED_REPAIR);
      logger().debug("{}: {} error(s) present with no repair possible", __func__,
		     (m_shallow_errors + m_deep_errors));
    }
  }

  //  finish up
  final_cstat_update(deep_scrub);

  if (has_error) {
    (void)m_osds.start_operation<LocalPeeringEvent>(
      m_pg, m_osds, m_pg->get_pg_whoami(), m_pg_id, get_osdmap_epoch(),
      get_osdmap_epoch(), PeeringState::DoRecovery{});
  } else {
    state_clear(PG_STATE_REPAIR);
  }

  cleanup_on_finish();
  if (do_auto_scrub) {
    request_rescrubbing(m_pg->m_planned_scrub);
  } else {
    // adjust the time of the next scrub
    m_scrub_reg_stamp = m_osds.m_scrub_queue.update_scrub_job(
      m_pg_id, m_scrub_reg_stamp, ceph_clock_now(), ScrubQueue::must_scrub_t::not_mandatory);
  }

  if (m_pg->get_peering_state().is_active() && m_pg->is_primary()) {
    m_pg->get_peering_state().share_pg_info();
  }
}

void PgScrubber::on_digest_updates()
{
  logger().debug("{}: #pending: {} {}", __func__, num_digest_updates_pending,
		 (m_end.is_max() ? " <last chunk> " : " <mid chunk> "));

  if (num_digest_updates_pending > 0) {
    // do nothing for now. We will be called again when new updates arrive
    return;
  }

  // got all updates, and finished with this chunk. Any more?
  if (m_end.is_max()) {
    scrub_finish();
    // consider moving the event to the finish itself
    queue_local_trigger(&ScrubPgIF::send_scrub_is_finished, m_pg->get_osdmap_epoch(), 1ms,
			"ScrubFinished");
  } else {
    // go get a new chunk (via "requeue")
    preemption_data.reset();
    queue_local_trigger(&ScrubPgIF::send_get_next_chunk, m_pg->get_osdmap_epoch(), 1ms,
			"NextChunk");
  }
}


/*
 * note that the flags-set fetched from the PG (m_pg->m_planned_scrub)
 * is cleared once scrubbing starts; Some of the values dumped here are
 * thus transitory.
 */
void PgScrubber::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrubber");
  f->dump_stream("epoch_start") << m_interval_start;
  f->dump_bool("active", m_active);
  if (m_active) {
    f->dump_stream("start") << m_start;
    f->dump_stream("end") << m_end;
    f->dump_stream("m_max_end") << m_max_end;
    f->dump_stream("subset_last_update") << m_subset_last_update;
    f->dump_bool("deep", m_is_deep);
    f->dump_bool("must_scrub", (m_pg->m_planned_scrub.must_scrub || m_flags.required));
    f->dump_bool("must_deep_scrub", m_pg->m_planned_scrub.must_deep_scrub);
    f->dump_bool("must_repair", m_pg->m_planned_scrub.must_repair);
    f->dump_bool("need_auto", m_pg->m_planned_scrub.need_auto);
    f->dump_bool("req_scrub", m_flags.required);
    f->dump_bool("time_for_deep", m_pg->m_planned_scrub.time_for_deep);
    f->dump_bool("auto_repair", m_flags.auto_repair);
    f->dump_bool("check_repair", m_flags.check_repair);
    f->dump_bool("deep_scrub_on_error", m_flags.deep_scrub_on_error);
    f->dump_stream("scrub_reg_stamp") << m_scrub_reg_stamp;  // utime_t
    f->dump_unsigned("priority", m_flags.priority);
    f->dump_int("shallow_errors", m_shallow_errors);
    f->dump_int("deep_errors", m_deep_errors);
    f->dump_int("fixed", m_fixed_count);
    {
      f->open_array_section("waiting_on_whom");
      for (const auto& p : m_maps_status.get_awaited()) {
	f->dump_stream("shard") << p;
      }
      f->close_section();
    }
  }
  f->close_section();
}

void PgScrubber::handle_query_state(ceph::Formatter* f)
{
  logger().debug("{}", __func__);

  f->open_object_section("scrub");
  f->dump_stream("scrubber.epoch_start") << m_interval_start;
  f->dump_bool("scrubber.active", m_active);
  f->dump_stream("scrubber.start") << m_start;
  f->dump_stream("scrubber.end") << m_end;
  f->dump_stream("scrubber.m_max_end") << m_max_end;
  f->dump_stream("scrubber.m_subset_last_update") << m_subset_last_update;
  f->dump_bool("scrubber.deep", m_is_deep);
  {
    f->open_array_section("scrubber.waiting_on_whom");
    for (const auto& p : m_maps_status.get_awaited()) {
      f->dump_stream("shard") << p;
    }
    f->close_section();
  }

  f->dump_string("comment", "DEPRECATED - may be removed in the next release");

  f->close_section();
}

PgScrubber::~PgScrubber()
{
  unregister_from_osd();
}

PgScrubber::PgScrubber(PG* pg)
    : m_pg{pg}
    , m_pg_id{pg->get_pgid()}
    , m_osds{m_pg->shard_services}
    , m_pg_whoami{pg->pg_whoami}
    , preemption_data{pg}
{

  logger().debug("{}: creating PgScrubber for {} / {}", __func__, pg->get_pgid(),
		 m_pg_whoami);
  m_fsm = std::make_unique<ScrubMachine>(m_pg, this);
  m_fsm->initiate();
}

void PgScrubber::reserve_replicas()
{
  logger().debug("{}", __func__);
  m_reservations.emplace(m_pg, m_pg_whoami);
}

void PgScrubber::cleanup_on_finish()
{
  logger().debug("{}", __func__);
  // ceph_assert(m_pg->is_locked());

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);
  m_pg->publish_stats_to_osd();

  clear_scrub_reservations();
  m_pg->publish_stats_to_osd();

  requeue_waiting();

  reset_internal_state();
  m_flags = scrub_flags_t{};

  m_scrub_cstat = object_stat_collection_t();
}

// uses process_event(), so must be invoked externally
void PgScrubber::scrub_clear_state()
{
  logger().debug("{}", __func__);

  clear_pgscrub_state();
  m_fsm->process_event(FullReset{});
}

/*
 * note: does not access the state-machine
 */
void PgScrubber::clear_pgscrub_state()
{
  logger().debug("{}", __func__);
  // ceph_assert(m_pg->is_locked());

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);

  state_clear(PG_STATE_REPAIR);

  clear_scrub_reservations();
  m_pg->publish_stats_to_osd();

  requeue_waiting();

  reset_internal_state();
  m_flags = scrub_flags_t{};

  m_scrub_cstat = object_stat_collection_t();
}

void PgScrubber::replica_handling_done()
{
  logger().debug("{}", __func__);

  state_clear(PG_STATE_SCRUBBING);
  state_clear(PG_STATE_DEEP_SCRUB);

  reset_internal_state();

  m_pg->publish_stats_to_osd();
}

/*
 * note: performs run_callbacks()
 * note: reservations-related variables are not reset here
 */
void PgScrubber::reset_internal_state()
{
  logger().debug("{}", __func__);

  preemption_data.reset();
  m_maps_status.reset();
  m_received_maps.clear();

  m_start = hobject_t{};
  m_end = hobject_t{};
  m_max_end = hobject_t{};
  m_subset_last_update = eversion_t{};
  m_shallow_errors = 0;
  m_deep_errors = 0;
  m_fixed_count = 0;
  // RRR m_omap_stats = (const struct omap_stat_t){0};

  run_callbacks();

  //m_inconsistent.clear();
  //m_missing.clear();
  m_authoritative.clear();
  num_digest_updates_pending = 0;
  m_primary_scrubmap = ScrubMap{};
  m_primary_scrubmap_pos.reset();
  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos.reset();
  //m_cleaned_meta_map = ScrubMap{};
  m_needs_sleep = true;
  m_sleep_started_at = utime_t{};

  m_active = false;
}

OSDMapService::cached_map_t PgScrubber::get_osdmap()
{
  return m_pg->get_osdmap();
}

OSDMapService::cached_map_t PgScrubber::get_osdmap() const
{
  return m_pg->get_osdmap();
}

ostream& operator<<(ostream& out, const PgScrubber& scrubber)
{
  return out << scrubber.m_flags;
}

ostream& PgScrubber::show(ostream& out) const
{
  return out << " [ " << m_pg_id << ": " << m_flags << " ] ";
}


void PgScrubber::repair_object(const hobject_t& soid,
			       const list<pair<ScrubMap::object, pg_shard_t>>& ok_peers,
			       const set<pg_shard_t>& bad_peers)
{
  set<pg_shard_t> ok_shards;
  for (auto&& peer : ok_peers)
    ok_shards.insert(peer.second);

  logger().info("{}: repair_object {} bad_peers osd.{{ {} }}, ok_peers osd.{{ {} }}",
		__func__, soid, bad_peers, ok_shards);

  // dout(10) << "repair_object " << soid << " bad_peers osd.{" << bad_peers << "},"
  //	   << " ok_peers osd.{" << ok_shards << "}" << dendl;

  const ScrubMap::object& po = ok_peers.back().first;
  eversion_t v;
  object_info_t oi;
  try {
    bufferlist bv;
    if (po.attrs.count(OI_ATTR)) {
      bv.push_back(po.attrs.find(OI_ATTR)->second);
    }
    auto bliter = bv.cbegin();
    decode(oi, bliter);
  } catch (...) {
    logger().info("{}: Need version of replica, bad object_info_t: {}", __func__, soid);
    ceph_abort();
  }

  if (bad_peers.count(m_pg->get_primary())) {
    // We should only be scrubbing if the PG is clean.
    // RRR ceph_assert(waiting_for_unreadable_object.empty());
    logger().info("{}: primary = {}", __func__, m_pg->get_primary());
  }

  /* No need to pass ok_peers, they must not be missing the object, so
   * force_object_missing will add them to missing_loc anyway */
  m_pg->get_peering_state().force_object_missing(bad_peers, soid, oi.version);
}


// ///////////////////// preemption_data_t //////////////////////////////////

PgScrubber::preemption_data_t::preemption_data_t(PG* pg) : m_pg{pg}
{
  m_left = static_cast<int>(
    m_pg->get_cct()->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
}

void crimson::osd::PgScrubber::preemption_data_t::reset()
{
  std::lock_guard<std::mutex> lk{m_preemption_lock};

  m_preemptable = false;
  m_preempted = false;
  m_left = static_cast<int>(
    m_pg->get_cct()->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
  m_size_divisor = 1;
}

// ///////////////////// ReplicaReservations //////////////////////////////////
namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
  logger().debug("{}: <ReplicaReservations> release-> {}", __func__, peer);

  auto m = make_message<MOSDScrubReserve>(spg_t(m_pg->get_pgid().pgid, peer.shard), epoch,
					  MOSDScrubReserve::RELEASE, m_pg->pg_whoami);
  (void)m_osds.send_to_osd(peer.osd, m, epoch);
}

ReplicaReservations::ReplicaReservations(PG* pg, pg_shard_t whoami)
    : m_pg{pg}
    , m_acting_set{pg->get_actingset()}
    , m_osds{m_pg->get_shard_services()}
    , m_pending{static_cast<int>(m_acting_set.size()) - 1}
{
  epoch_t epoch = m_pg->get_osdmap_epoch();

  // handle the special case of no replicas
  if (m_pending <= 0) {
    // just signal the scrub state-machine to continue
    send_all_done();

  } else {

    const auto not_me =
      boost::adaptors::filtered([whoami](pg_shard_t pgs) { return pgs != whoami; });

    for (auto& p : m_acting_set | not_me) {
      // if (p == whoami)
      // continue;
      auto m = new MOSDScrubReserve(spg_t(m_pg->get_pgid().pgid, p.shard), epoch,
				    MOSDScrubReserve::REQUEST, m_pg->pg_whoami);
      std::ignore = m_osds.send_to_osd(p.osd, m, epoch);
      m_waited_for_peers.push_back(p);
      logger().debug("{}: <ReplicaReservations> reserve<-> {}", __func__, p.osd);
    }
  }
}

void ReplicaReservations::send_all_done()
{
  epoch_t epoch = m_pg->get_osdmap_epoch();
  logger().debug("{} @ e({})", __func__, epoch);

  // we do not have access to the full PgScrubber API. That's why we will
  // construct the event manually (instead of using queue_local_trigger())

  std::ignore = m_osds.start_operation<ScrubEvent2>(
    m_pg, m_pg->get_shard_services(), m_pg->get_pg_whoami(), m_pg->get_pgid(), 1ms,
    &ScrubPgIF::send_remotes_reserved, epoch, "RemotesReserved");
}

void ReplicaReservations::send_reject()
{
  epoch_t epoch = m_pg->get_osdmap_epoch();
  logger().debug("{} @ e({})", __func__, epoch);

  // we do not have access to the full PgScrubber API. That's why we will
  // construct the event manually (instead of using queue_local_trigger())

  std::ignore = m_osds.start_operation<ScrubEvent2>(
    m_pg, m_pg->get_shard_services(), m_pg->get_pg_whoami(), m_pg->get_pgid(), 1ms,
    &ScrubPgIF::send_reservation_failure, epoch, "ReservationFailure");
}

void ReplicaReservations::discard_all()
{
  logger().debug("{}: {}", __func__, m_reserved_peers);

  m_had_rejections = true;  // preventing late-coming responses from triggering events
  m_reserved_peers.clear();
  m_waited_for_peers.clear();
}

ReplicaReservations::~ReplicaReservations()
{
  m_had_rejections = true;  // preventing late-coming responses from triggering events

  // send un-reserve messages to all reserved replicas. We do not wait for
  // answer (there wouldn't be one). Other incoming messages will be discarded
  // on the way, by our owner.
  logger().debug("{}: {}", __func__, m_reserved_peers);

  epoch_t epoch = m_pg->get_osdmap_epoch();

  for (auto& p : m_reserved_peers) {
    release_replica(p, epoch);
  }
  m_reserved_peers.clear();

  // note: the release will follow on the heels of the request. When tried
  // otherwise, grants that followed a reject arrived after the whole scrub
  // machine-state was reset, causing leaked reservations.
  for (auto& p : m_waited_for_peers) {
    release_replica(p, epoch);
  }
  m_waited_for_peers.clear();
}

/**
 *  @ATTN we would not reach here if the ReplicaReservation object managed by
 * the scrubber was reset.
 */
void ReplicaReservations::handle_reserve_grant(const MOSDScrubReserve& msg,
					       pg_shard_t from)
{
  logger().debug("{}: <ReplicaReservations> granted-> {}", __func__, from);
  // op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is
    // cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  // are we forced to reject the reservation?
  if (m_had_rejections) {

    logger().debug("{}: rejecting late-coming reservation from {}", __func__, from);
    release_replica(from, m_pg->get_osdmap_epoch());

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    logger().debug("{}: already had osd.{} reserved", __func__, from);

  } else {

    logger().debug("{}: osd.{} scrub reserve = success", __func__, from);
    m_reserved_peers.push_back(from);
    if (--m_pending == 0) {
      send_all_done();
    }
  }
}

void ReplicaReservations::handle_reserve_reject(const MOSDScrubReserve& msg,
						pg_shard_t from)
{
  logger().debug("{}: <ReplicaReservations> rejected-> {}", __func__, from);
  // logger().debug("{}: {}", __func__, *op->get_req());
  // op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is
    // cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  if (m_had_rejections) {

    // our failure was already handled when the first rejection arrived
    logger().debug("{}: ignoring late-coming rejection from {}", __func__, from);

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    logger().debug("{}: already had osd.{} reserved", __func__, from);

  } else {

    logger().debug("{}: osd.{} scrub reserve = fail", __func__, from);
    m_had_rejections = true;  // preventing any additional notifications
    send_reject();
  }
}

// ///////////////////// LocalReservation //////////////////////////////////////

LocalReservation::LocalReservation(crimson::osd::PG* pg, crimson::osd::ScrubQueue& osds)
    : m_pg{pg}, m_queuer{osds}
{
  if (!m_queuer.inc_scrubs_local()) {
    logger().debug("scrubber {}: pg({}) failed to reserve locally", __func__,
		   m_pg->get_pgid());
    // the failure is signalled by not having m_holding_local_reservation set
    return;
  }

  logger().debug("scrubber {}: pg({}) local OSD scrub resources reserved", __func__,
		 m_pg->get_pgid());
  m_holding_local_reservation = true;
}

LocalReservation::~LocalReservation()
{
  if (m_holding_local_reservation) {
    m_holding_local_reservation = false;
    m_queuer.dec_scrubs_local();
    logger().debug("{}: local OSD scrub resources freed", __func__);
  }
}

// ///////////////////// ReservedByRemotePrimary ///////////////////////////////

ReservedByRemotePrimary::ReservedByRemotePrimary(PG* pg,
						 crimson::osd::ScrubQueue& osds,
						 epoch_t epoch)
    : m_pg{pg}, m_queuer{osds}, m_reserved_at{epoch}
{
  if (!m_queuer.inc_scrubs_remote()) {
    logger().debug("{}: failed to reserve at Primary request", __func__);
    // the failure is signalled by not having m_reserved_by_remote_primary set
    return;
  }

  logger().debug("{}: scrub resources reserved at Primary request", __func__);
  m_reserved_by_remote_primary = true;
}

bool ReservedByRemotePrimary::is_stale() const
{
  return m_reserved_at < m_pg->get_same_interval_since();
}

ReservedByRemotePrimary::~ReservedByRemotePrimary()
{
  if (m_reserved_by_remote_primary) {
    m_reserved_by_remote_primary = false;
    m_queuer.dec_scrubs_remote();
    logger().debug("{}: scrub resources held for Primary were freed", __func__);
  }
}

// ///////////////////// MapsCollectionStatus ////////////////////////////////

auto MapsCollectionStatus::mark_arriving_map(pg_shard_t from)
  -> std::tuple<bool, std::string_view>
{
  auto fe = std::find(m_maps_awaited_for.begin(), m_maps_awaited_for.end(), from);
  if (fe != m_maps_awaited_for.end()) {
    // we are indeed waiting for a map from this replica
    m_maps_awaited_for.erase(fe);
    return std::tuple{true, ""sv};
  } else {
    return std::tuple{false, " unsolicited scrub-map"sv};
  }
}

void MapsCollectionStatus::reset()
{
  *this = MapsCollectionStatus{};
}

std::string MapsCollectionStatus::dump() const
{
  std::string all;
  for (const auto& rp : m_maps_awaited_for) {
    all.append(rp.get_osd() + " "s);
  }
  return all;
}

ostream& operator<<(ostream& out, const MapsCollectionStatus& sf)
{
  out << " [ ";
  for (const auto& rp : sf.m_maps_awaited_for) {
    out << rp.get_osd() << " ";
  }
  if (!sf.m_local_map_ready) {
    out << " local ";
  }
  return out << " ] ";
}

}  // namespace Scrub

//}  // namespace crimson::osd
