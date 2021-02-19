// -*- mode:C++; tab-width:2; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab

#include "scrubber.h"

#include <iostream>
#include <vector>

#include "debug.h"

#include "common/errno.h"
#include "crimson/common/log.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/osd_operations/pg_scrub_event.h"
#include "crimson/osd/osd_operations/scrub_event.h"
#include "crimson/osd/pg_scrub_sched.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDScrub.h"
#include "messages/MOSDScrubReserve.h"

#include "pg_backend.h"
#include "scrub_machine_cr.h"

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

using namespace ::crimson::osd::Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;

using ::crimson::osd::Scrub::PreemptionNoted;

#if 0
#define dout_context (m_pg->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this->m_pg)

template <class T> static ostream &_prefix(std::ostream *_dout, T *t) {
  return t->gen_prefix(*_dout) << " scrubber pg(" << t->get_pgid() << ") ";
}
#endif

ostream& operator<<(ostream& out, const ::crimson::osd::scrub_flags_t& sf)
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

ostream& operator<<(ostream& out, const ::crimson::osd::requested_scrub_t& sf)
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

namespace crimson::osd {

// moved from the .h (due to temp compilation issues)
eversion_t PgScrubber::get_last_update_applied() const
{
  // RRR return m_pg->peering_state.get_last_update_applied();
  return m_pg->get_peering_state().get_last_update_applied();
}


// RRR int PgScrubber::pending_active_pushes() const { return m_pg->active_pushes; }
int PgScrubber::pending_active_pushes() const
{
  return 0;
}

bool PgScrubber::state_test(uint64_t m) const
{
  return m_pg->state_test(m);
}
void PgScrubber::state_set(uint64_t m)
{
  m_pg->state_set(m);
}
void PgScrubber::state_clear(uint64_t m)
{
  m_pg->state_clear(m);
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


void PgScrubber::do_scrub_event(crimson::osd::PgScrubEvent evt, PeeringCtx &rctx)
{
  logger().warn("{}: event: ???", __func__);

  m_fsm->process_event(evt.get_event());
}


// void PgScrubber::queue_local_event(MessageRef msg, Scrub::scrub_prio_t prio){}
#if 0
void PgScrubber::queue_local_event(
  boost::intrusive_ptr<const boost::statechart::event_base> fsm_event,
  Scrub::scrub_prio_t prio)
{
  std::ignore = m_pg->get_shard_services().start_operation<LocalScrubEvent>(
    static_cast<crimson::osd::PG*>(m_pg), m_pg->get_shard_services(),
    m_pg->get_pg_whoami(), m_pg->get_pgid(), m_pg->get_osdmap_epoch(),
    m_pg->get_osdmap_epoch(), fsm_event);
}

void PgScrubber::queue_local_event2(const boost::statechart::event_base& fsm_event,
				    Scrub::scrub_prio_t prio)
{
  std::ignore = m_pg->get_shard_services().start_operation<LocalScrubEvent>(
    static_cast<crimson::osd::PG*>(m_pg), m_pg->get_shard_services(),
    m_pg->get_pg_whoami(), m_pg->get_pgid(), m_pg->get_osdmap_epoch(),
    m_pg->get_osdmap_epoch(), fsm_event);
}

void PgScrubber::queue_local_event(boost::statechart::event_base& fsm_event,
				   Scrub::scrub_prio_t prio)
{
  std::ignore = m_pg->get_shard_services().start_operation<LocalScrubEvent>(
    static_cast<crimson::osd::PG*>(m_pg), m_pg->get_shard_services(),
    m_pg->get_pg_whoami(), m_pg->get_pgid(), m_pg->get_osdmap_epoch(),
    m_pg->get_osdmap_epoch(), fsm_event);
}
#endif

#if 0
void PgScrubber::queue_local_event(boost::statechart::event_base* fsm_event,
				   Scrub::scrub_prio_t prio)
{
  std::unique_ptr<PgScrubEvent> evt((PgScrubEvent*)fsm_event);

  std::ignore = m_pg->get_shard_services().start_operation<LocalScrubEvent>(
    static_cast<crimson::osd::PG*>(m_pg), m_pg->get_shard_services(),
    m_pg->get_pg_whoami(), m_pg->get_pgid(), m_pg->get_osdmap_epoch(),
    m_pg->get_osdmap_epoch(), std::move(*evt));
}
#endif

void PgScrubber::queue_local_event(boost::statechart::event_base* fsm_event,
				   Scrub::scrub_prio_t prio)
{
  std::unique_ptr<PgScrubEvent> evt((PgScrubEvent*)fsm_event);

  std::ignore = m_pg->get_shard_services().start_operation<LocalScrubEvent>(
    //static_cast<crimson::osd::PG*>(m_pg),
    m_pg,
    m_pg->get_shard_services(),
    m_pg->get_pg_whoami(),
    m_pg->get_pgid(),
    m_pg->get_osdmap_epoch(),
    m_pg->get_osdmap_epoch(),
    Scrub::InternalError{});
    //std::move(*evt));
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
    return false;
  }

  // is this a message from before we started this scrub?
  if (epoch_to_verify < m_epoch_start) {
    return false;
  }

  // has a new interval started?
  if (!check_interval(epoch_to_verify)) {
    // if this is a new interval, on_change() has already terminated that
    // old scrub.
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

  //const PGPool& p1 = m_pg->get_pool();
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
  logger().debug("{}: epoch: {}", __func__, epoch_queued);
  // we may have lost our Primary status while the message languished in the
  // queue
  if (check_interval(epoch_queued)) {
    logger().debug("{}: scrubber event -->> StartScrub epoch: {}", __func__,
		   epoch_queued);
    reset_epoch(epoch_queued);
    m_fsm->my_states();
    m_fsm->process_event(Scrub::StartScrub{});
    logger().debug("{}: scrubber event --<< StartScrub", __func__);
  }
}

void PgScrubber::initiate_scrub_after_repair(epoch_t epoch_queued)
{
  logger().debug("epoch: {}", epoch_queued);
  // we may have lost our Primary status while the message languished in the
  // queue
  if (check_interval(epoch_queued)) {
    logger().debug("{}: scrubber event -->> AfterRepairScrub epoch: {}", __func__,
		   epoch_queued);
    reset_epoch(epoch_queued);
    m_fsm->my_states();
    m_fsm->process_event(Scrub::AfterRepairScrub{});
    logger().debug("{}: scrubber event --<< AfterRepairScrub", __func__);
  }
}

void PgScrubber::send_scrub_unblock(epoch_t epoch_queued)
{
  logger().debug("scrubber event -->> {} epoch: {}", __func__, epoch_queued);
  if (is_message_relevant(epoch_queued)) {
    m_fsm->my_states();
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
  }
  logger().debug("scrubber event --<< {}", __func__);
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

void PgScrubber::reg_next_scrub(const requested_scrub_t& request_flags)
{
  if (!is_primary()) {
    // normal. No warning is required.
    return;
  }

  logger().debug(" planned: must? {}. need-auto? {}. stamp: {}", request_flags.must_scrub,
		 request_flags.need_auto,
		 m_pg->get_peering_state().get_info().history.last_scrub_stamp);

  ceph_assert(!is_scrub_registered());

  utime_t reg_stamp;
  ScrubQueue::must_scrub_t must{ScrubQueue::must_scrub_t::not_mandatory};

  if (request_flags.must_scrub || request_flags.need_auto) {
    // Set the smallest time that isn't utime_t()
    reg_stamp = PgScrubber::scrub_must_stamp();
    must = ScrubQueue::must_scrub_t::mandatory;
  } else if (/* RRR m_pg->info.stats.stats_invalid && */
	     m_pg->get_cct()->_conf->osd_scrub_invalid_stats) {
    reg_stamp = ceph_clock_now();
    must = ScrubQueue::must_scrub_t::mandatory;
  } else {
    reg_stamp = m_pg->get_peering_state().get_info().history.last_scrub_stamp;
  }

  logger().debug(" pg({}) must: {} required:{} flags: {} stamp: {}", m_pg_id, must,
		 m_flags.required, request_flags, reg_stamp);

  const double scrub_min_interval =
    m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
  const double scrub_max_interval =
    m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);

  // note the sched_time, so we can locate this scrub, and remove it later
  m_scrub_reg_stamp = m_osds.m_scrub_queue.reg_pg_scrub(
    m_pg_id, reg_stamp, scrub_min_interval, scrub_max_interval, must);
  logger().debug(" pg({}) register next scrub, scrub time {}, must = {}", m_pg_id,
		 m_scrub_reg_stamp);
}

void PgScrubber::unreg_next_scrub()
{
  logger().debug(" existing: {}. was registered? {}", m_scrub_reg_stamp,
		 is_scrub_registered());
  if (is_scrub_registered()) {
    m_osds.m_scrub_queue.unreg_pg_scrub(m_pg_id, m_scrub_reg_stamp);
    m_scrub_reg_stamp = utime_t{};
  }
}

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

void PgScrubber::request_rescrubbing(requested_scrub_t& req_flags)
{
  logger().debug("{}: existing: {}. was registered? {}", __func__, m_scrub_reg_stamp,
		 is_scrub_registered());

  unreg_next_scrub();
  req_flags.need_auto = true;
  reg_next_scrub(req_flags);
}

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

seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> list_objects_partial(
    os::CollectionRef c,
    const ghobject_t& start,
    //const ghobject_t& end,
    int min,
    int max/*,
    uint64_t limit*/)
{
  std::vector<ghobject_t> v;
  return seastar::make_ready_future<std::tuple<std::vector<ghobject_t>, ghobject_t>>(
    v, ghobject_t{});
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
  m_received_maps.clear();

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

  logger().debug("{}: Min: {} Max: {} Div: {}", __func__, min_idx, max_idx,
		 preemption_data.chunk_divisor());

  hobject_t start = m_start;
  hobject_t candidate_end;

  return m_pg->get_backend().list_objects(start, max_idx).then([this](auto&& obj_n_next) {
    auto& [objects, candidate_end] = obj_n_next;
    if (objects.empty()) {
      // do something
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
      logger().debug("PgScrubber::select_range(): scrub blocked somewhere in range [{} , {})", m_start,
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
      //queue_local_event(
	//boost::intrusive_ptr<Scrub::SelectedChunkFree>(new Scrub::SelectedChunkFree{}),
	//Scrub::scrub_prio_t::low_priority);
      queue_local_event(
	new Scrub::SelectedChunkFree{},
	Scrub::scrub_prio_t::low_priority);

    } else {

      logger().debug("select_range_n_notify(): selected chunk is busy");
      queue_local_event(
	//boost::intrusive_ptr<Scrub::ChunkIsBusy>(new Scrub::ChunkIsBusy{}),
	new Scrub::ChunkIsBusy{},
	Scrub::scrub_prio_t::low_priority);
    }
  });
}

#if 0
bool PgScrubber::select_range()
{
  m_primary_scrubmap = ScrubMap{};
  m_received_maps.clear();

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

  logger().debug("{}: Min: {} Max: {} Div: {}", __func__, min_idx, max_idx, preemption_data.chunk_divisor());

  hobject_t start = m_start;
  hobject_t candidate_end;
  std::vector<hobject_t> objects;
  int ret = m_pg->get_pgbackend()->objects_list_partial(start, min_idx, max_idx, &objects,
							&candidate_end);
  ceph_assert(ret >= 0);

  if (!objects.empty()) {

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

  } else {
    ceph_assert(candidate_end.is_max());
  }

  // is that range free for us? if not - we will be rescheduled later by whoever
  // triggered us this time

  if (!m_pg->_range_available_for_scrub(m_start, candidate_end)) {
    // we'll be requeued by whatever made us unavailable for scrub
    logger().debug("{}: scrub blocked somewhere in range [{},{})", __func__, m_start, candidate_end);
    return false;
  }

  m_end = candidate_end;
  if (m_end > m_max_end)
    m_max_end = m_end;

  logger().debug("{}: range selected: {} //// {} //// {}", __func__, m_start, m_end, m_max_end );
  return true;
}
#endif

bool PgScrubber::write_blocked_by_scrub(const hobject_t& soid)
{
  if (soid < m_start || soid >= m_end) {
    return false;
  }

  logger().debug("{}: {} can preempt? {} already preempted? {}", __func__, soid,
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
 *  Otherwise: perform a requeue (i.e. - rescheduling thru the OSD queue)
 *    anyway.
 */

/*
class PGScrubItem : public PGOpQueueable {
protected:
 epoch_t epoch_queued;
 std::string_view message_name;
 PGScrubItem(spg_t pg, epoch_t epoch_queued, std::string_view derivative_name)
     : PGOpQueueable{pg}, epoch_queued{epoch_queued}, message_name{derivative_name}
 {}
 op_type_t get_op_type() const final { return op_type_t::bg_scrub; }
 std::ostream& print(std::ostream& rhs) const final
 {
   return rhs << message_name << "(pgid=" << get_pgid()
	      << "epoch_queued=" << epoch_queued << ")";
 }
 void run(OSD* osd,
	  OSDShard* sdata,
	  PGRef& pg,
	  ThreadPool::TPHandle& handle) override = 0;
 op_scheduler_class get_scheduler_class() const final
 {
   return op_scheduler_class::background_best_effort;
 }
};

class PGScrubResched : public PGScrubItem {
public:
 PGScrubResched(spg_t pg, epoch_t epoch_queued)
     : PGScrubItem{pg, epoch_queued, "PGScrubResched"}
 {}
 void run(OSD* osd, OSDShard* sdata, PGRef& pg, ThreadPool::TPHandle& handle) final;
};

 */


seastar::future<bool>
PgScrubber::add_delayed_scheduling()  // replace with an errorator, if returning anything
{
  milliseconds sleep_time{0ms};
  if (m_needs_sleep) {
    double scrub_sleep =
      1000.0 * m_osds.get_scrub_services().scrub_sleep_time(m_flags.required);
    sleep_time = milliseconds{long(scrub_sleep)};
  }
  logger().debug(" sleep: {} ms. needed? {}", sleep_time.count(), m_needs_sleep);

  // MessageRef resched_event_msg = make_message<PGScrubResched>(m_pg,
  // m_pg->get_osdmap_epoch());

  if (sleep_time.count()) {

    // schedule a transition for some 'sleep_time' ms in the future

    m_needs_sleep = false;
    m_sleep_started_at = ceph_clock_now();

    // create a ref to this PG
    auto this_pg = m_pg;  // RRR to be replaced with a counted ref


    // the following log line is used by osd-scrub-test.sh
    logger().debug(" scrub state is PendingTimer, sleeping");

    // spg_t pgid = m_osds.get_pg();

    return seastar::sleep(sleep_time).then([scrbr = this, this_pg]() {
      // was there an interval change in the meantime?

      /* RRR just for now */ std::ignore = this_pg;

      if (false /* interval changed (and maybe other PG status checks?) */) {
	// RRR lgeneric_subdout(g_ceph_context, osd, 10)
	// RRR   << "scrub_requeue_callback: Could not find "
	// RRR  << "PG " << pgid << " can't complete scrub requeue after sleep" );
	return seastar::make_ready_future<bool>(false);
      }

      scrbr->m_needs_sleep = true;
      scrbr->m_sleep_started_at = utime_t{};
      // std::ignore = scrbr->m_osds.get_scrub_services().queue_scrub_event(this_pg,
      // resched_event_msg, Scrub::scrub_prio_t::low_priority);
      auto x = new Scrub::InternalSchedScrub{};
      scrbr->queue_local_event(x, Scrub::scrub_prio_t::low_priority);
      return seastar::make_ready_future<bool>(true);
    });

  } else {
    // just a requeue
    //queue_local_event(Scrub::InternalSchedScrub{}.intrusive_from_this(),
    queue_local_event(new Scrub::InternalSchedScrub{},
		      Scrub::scrub_prio_t::high_priority);
    return seastar::make_ready_future<bool>(true);
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
  logger().debug(" started in epoch/interval:  {}/ {} pg same_interval_since: {}",
		 m_epoch_start, m_interval_start, m_pg->get_same_interval_since());

  bool do_have_replicas = false;

  m_primary_scrubmap_pos.reset();

  // ask replicas to scan and send maps
  std::ignore = seastar::parallel_for_each(m_pg->get_acting_recovery_backfill(), [this,
								    replica_can_preempt,
								    &do_have_replicas](
								     pg_shard_t shard) {
    if (shard == m_pg->get_pg_whoami())
      return seastar::make_ready_future<>();

    do_have_replicas = true;
    m_maps_status.mark_replica_map_request(shard);
    return request_scrub_map(shard, m_subset_last_update, m_start, m_end, m_is_deep,
			     replica_can_preempt);
  }).finally([this, do_have_replicas]() -> void {
    logger().info("PgScrubber::get_replicas_maps(): replica requests sent");

    /* RRR */ std::ignore = do_have_replicas;

    queue_local_event(new Scrub::ReplicaRequestsSent{}, //.intrusive_from_this(),
		      Scrub::scrub_prio_t::high_priority);
  });
}

/*
bool PgScrubber::get_replicas_maps(bool replica_can_preempt)
{
  logger().debug(" started in epoch/interval:  {}/ {} pg same_interval_since: {}",
		 m_epoch_start, m_interval_start, m_pg->get_same_interval_since());

  bool do_have_replicas = false;

  m_primary_scrubmap_pos.reset();

  // ask replicas to scan and send maps
  for (const auto& i : m_pg->get_acting_recovery_backfill()) {

    if (i == m_pg_whoami)
      continue;

    do_have_replicas = true;
    m_maps_status.mark_replica_map_request(i);
    _request_scrub_map(i, m_subset_last_update, m_start, m_end, m_is_deep,
		       replica_can_preempt);
  }

  logger().debug("{}: awaiting {}", __func__, m_maps_status);
  return do_have_replicas;
}
*/

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
  logger().debug(" scrubmap from osd.{} {}", replica, (deep ? " deep" : " shallow"));

  auto rep_scrub_op = make_message<MOSDRepScrub>(
    spg_t(m_pg_id.pgid, replica.shard), version, get_osdmap_epoch(),
    m_pg->get_last_peering_reset(), start, end, deep, allow_preemption, m_flags.priority,
    false /* RRR todo m_pg->ops_blocked_by_scrub()*/);

  return m_osds.send_to_osd(replica.shard, rep_scrub_op, get_osdmap_epoch());

  /*
    auto repscrubop =
      new MOSDRepScrub(spg_t(m_pg->info.pgid.pgid, replica.shard), version,
		       get_osdmap_epoch(), m_pg->get_last_peering_reset(), start, end,
    deep, allow_preemption, m_flags.priority, m_pg->ops_blocked_by_scrub());

    // default priority. We want the replica-scrub processed prior to any recovery
    // or client io messages (we are holding a lock!)
    m_osds.send_message_osd_cluster(replica.osd, repscrubop, get_osdmap_epoch());
  */
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

  logger().debug("{}: start same_interval: {}", __func__, m_interval_start);

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
      queue_local_event(
        new Scrub::OnInitDone{},
	//boost::intrusive_ptr<Scrub::OnInitDone>(new Scrub::OnInitDone{}),
	Scrub::scrub_prio_t::low_priority);
    });
}


//void PgScrubber::on_init_immediate()
//{
//  // going upwards from 'inactive'
//  ceph_assert(!is_scrub_active());
//
//  preemption_data.reset();
//  m_pg->publish_stats_to_osd();
//  m_interval_start = m_pg->get_same_interval_since();
//
//  logger().debug("{}: start same_interval: {}", __func__, m_interval_start);
//
//  //  create a new store
//  ObjectStore::Transaction t;
//  cleanup_store(&t);
//  m_store.reset(Scrub::Store::create(
//    &m_osds.get_store(), &t, m_pg_id,
//    m_pg->get_collection_ref()->get_cid()));  // RRR find which type of store is needed
//  // RRR  Scrub::Store::create(m_pg->osd->store, &t, m_pg->info.pgid, m_pg->coll));
//
//  on_init_results = m_osds.get_store()
//		      .do_transaction(m_pg->get_collection_ref(), std::move(t))
//		      .then([this]() {
//			m_start = m_pg->get_pgid().pgid.get_hobj_start();
//			m_active = true;
//		      });
//}


void PgScrubber::on_replica_init()
{
  ceph_assert(!m_active);
  m_active = true;
}

void PgScrubber::_scan_snaps(ScrubMap& smap)
{
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

  // m_osds.queue_for_scrub_resched(m_pg, Scrub::scrub_prio_t::low_priority);
}

void PgScrubber::initiate_primary_map_build()
{
  logger().debug("{}}()", __func__);

  // RRR handle errors

  std::ignore = build_primary_map_chunk().then([this](){
    logger().debug("initiate_primary_map_build(): map built");
    queue_local_event(
      new Scrub::IntLocalMapDone{},
      //boost::intrusive_ptr<Scrub::IntLocalMapDone>(new Scrub::IntLocalMapDone{}),
      Scrub::scrub_prio_t::low_priority);
  });
}


struct object_list_ret {
  int res;
  int p;
  vector<ghobject_t> m_objs;
};

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

void PgScrubber::build_replica_map_chunk()
{
  logger().debug(" interval start: {}  epoch: {} deep: {}", m_interval_start,
		 m_epoch_start, m_is_deep);

  std::ignore =
    build_scrub_map_chunk(replica_scrubmap, replica_scrubmap_pos, m_start, m_end,
			  m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow)
      .then([this]() {
	m_cleaned_meta_map.clear_from(m_start);
	m_cleaned_meta_map.insert(replica_scrubmap);
	auto for_meta_scrub = clean_meta_map();
	return _scan_snaps(for_meta_scrub);
      })
      .then([this]() {
	// the local map was created. Send it to the primary.
	send_replica_map(PreemptionNoted::no_preemption);
	replica_handling_done();
	logger().debug("build_replica_map_chunk(): map built");
	//queue_local_event(boost::intrusive_ptr<Scrub::FullReset>(new Scrub::FullReset{}),
	queue_local_event(new Scrub::FullReset{},
			  Scrub::scrub_prio_t::high_priority);

	return seastar::make_ready_future<>();
      });

  //     requeue_replica(m_replica_request_priority);
}


#ifdef SEE_EXAMPLE_RRR


seastar::future<std::tuple<std::vector<ghobject_t>, ghobject_t>> AlienStore::list_objects(
  CollectionRef ch, const ghobject_t& start, const ghobject_t& end, uint64_t limit) const
{
  logger().debug("{}", __func__);
  return seastar::do_with(
    std::vector<ghobject_t>(), ghobject_t(), [=](auto& objects, auto& next) {
      objects.reserve(limit);
      return tp
	->submit([=, &objects, &next] {
	  auto c = static_cast<AlienCollection*>(ch.get());
	  return store->collection_list(c->collection, start, end,
					store->get_ideal_list_max(), &objects, &next);
	})
	.then([&objects, &next](int r) {
	  assert(r == 0);
	  return seastar::make_ready_future<
	    std::tuple<std::vector<ghobject_t>, ghobject_t>>(
	    std::make_tuple(std::move(objects), std::move(next)));
	});
    });
}

#endif

#if 0


int BlueStore::collection_list(
  CollectionHandle &c_, const ghobject_t& start, const ghobject_t& end, int max,
  vector<ghobject_t> *ls, ghobject_t *pnext)
{
  Collection *c = static_cast<Collection *>(c_.get());
  c->flush();
  dout(15) << __func__ << " " << c->cid
           << " start " << start << " end " << end << " max " << max << dendl;
  int r;
  {
    std::shared_lock l(c->lock);
    r = _collection_list(c, start, end, max, false, ls, pnext);
  }

  dout(10) << __func__ << " " << c->cid
    << " start " << start << " end " << end << " max " << max
    << " = " << r << ", ls.size() = " << ls->size()
    << ", next = " << (pnext ? *pnext : ghobject_t())  << dendl;
  return r;
}
#endif

seastar::future<> PgScrubber::scan_rollback_obs(const vector<ghobject_t>& rollback_obs)
{
#ifdef RRR_NOT_YET
  ObjectStore::Transaction t;
  eversion_t trimmed_to =
    m_pg->get_recovery_state().get_last_rollback_info_trimmed_to_applied();

  for (const auto& o : rollback_obs) {

    if (o.generation < trimmed_to.version) {

      logger().info(
	"{}: osd.{} pg {} found obsolete rollback obj {} generation < trimmed_to {} "
	"...repaired",
	__func__, /* osd->whoami*/ 777, m_pg_id, o, trimmed_to);
      t.remove(m_pg->coll, o);
    }
  }

  if (!t.empty()) {
    logger().error("{}: queueing trans to clean up obsolete rollback objs", __func__);
    m_osds.get_store()->do_transaction(ch, std::move(t));
  }
#endif
  return seastar::make_ready_future();
}


seastar::future<> PgScrubber::repair_oinfo_oid(ScrubMap& smap)
{
  // RRR transposing the loop into a 'do_for_each' is probably a bad idea,
  //  as I am creating a continuation chain for all objects, even though most
  //  or all are clean

  return seastar::do_for_each(
    smap.objects.rbegin(), smap.objects.rend(),
    [smap, this](auto& i) mutable -> seastar::future<> {
      const hobject_t& hoid = i.first;
      ScrubMap::object& o = i.second;

      if (o.attrs.find(OI_ATTR) == o.attrs.end()) {
	return seastar::make_ready_future<>();
      }

      bufferlist bl;
      bl.push_back(o.attrs[OI_ATTR]);
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

#ifdef NOT_YET_RRR
      ObjectStore::Transaction t;
      OSDriver::OSTransaction _t(osdriver.get_transaction(&t));
      t.setattr(coll, ghobject_t(hoid), OI_ATTR, bl);

      return m_pg->get_store()->queue_transaction(ch, std::move(t));
#endif

      // return crimson::ct_error::enoent::make();
      return seastar::make_ready_future<>();
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

seastar::future<> PgScrubber::build_scrub_map_chunk(ScrubMap& map,
						    ScrubMapBuilder& pos,
						    hobject_t start,
						    hobject_t end,
						    scrub_level_t depth)
{
  logger().debug("{}: [{},{}) pos: {} Deep: {}", __func__, start, end, pos,
		 (depth == scrub_level_t::deep) ? "deep" : "shallow");

  // start
  pos.reset();

  pos.deep = depth == scrub_level_t::deep;
  // RRR map.valid_through = m_pg->info.last_update;

  // objects

  // assume objects_list_range returns <new ls, vector<ghobject_t> rollback_obs>

  return m_pg->get_backend()
    .list_range(start, end, pos.ls)
    .then(  // should list_range return an errorator?

      [this, map, pos, start, end,
       depth](vector<ghobject_t>&& rollback_obs) mutable -> seastar::future<> {
	if (pos.empty()) {
	  return seastar::make_ready_future<>();
	}

	// check for & remove obsolete gen objects

	// not sure I understand the logic here. Why do we need to loop on the
	// list_range() if ls wasn't empty?

	// to ask: why no need to scrub rollbacks if 'ls' is empty?

	return scan_rollback_obs(rollback_obs);
	/*.then([pos](){
		pos.pos = 0;
		return seastar::make_ready_future<>();
	}*/
      })
    .then([this, map, pos]() mutable -> seastar::future<> {
      // scan the objects

      return m_pg->get_backend().scan_list(map, pos).safe_then(

	[this, map, pos]() mutable -> seastar::future<> {
	  logger().debug("PgScrubber::build_scrub_map_chunk(): before repair_oinfo");
	  return repair_oinfo_oid(map);
	},
	PGBackend::ll_read_errorator::all_same_way([]() {
	  // RRR complete the error handling here

	  // return PGBackend::stat_errorator::make_ready_future(); })
	  return seastar::make_ready_future<>();
	}));
    })
    .then([map]() {
      logger().debug("PgScrubber::build_scrub_map_chunk(): done. Got {} items",
		     map.objects.size());
      return seastar::make_ready_future<>();
    });
}

#if 0

::crimson::os::FuturizedStore::read_errorato

int PgScrubber::build_scrub_map_chunk(
  ScrubMap& map, ScrubMapBuilder& pos, hobject_t start, hobject_t end, bool deep)
{
  logger().debug("{}: [{},{}) pos: {} Deep: {}", __func__, start, end, pos, deep);

  // start
  while (pos.empty()) {

    pos.deep = deep;
    map.valid_through = m_pg->info.last_update;

    // objects
    vector<ghobject_t> rollback_obs;
    pos.ret = m_pg->get_backend()->objects_list_range(start, end, &pos.ls, &rollback_obs);
    logger().debug("{}: while pos empty {}", __func__, pos.ret);
    if (pos.ret < 0) {
      logger().debug("{}: objects_list_range error: {}", __func__, pos.ret);
      return pos.ret;
    }
    logger().debug("{}: pos.ls.empty()? {}", __func__, (pos.ls.empty() ? "+" : "-"));
    if (pos.ls.empty()) {
      break;
    }
    m_pg->_scan_rollback_obs(rollback_obs);
    pos.pos = 0;
    return -EINPROGRESS;
  }

  // scan objects
  while (!pos.done()) {
    int r = m_pg->get_backend()->be_scan_list(map, pos);
    logger().debug("{}: backend ret {}", __func__, r);
    if (r == -EINPROGRESS) {
      logger().debug("{}: in progress", __func__);
      return r;
    }
  }

  // finish
  logger().debug("{} finishing", __func__);
  ceph_assert(pos.done());
  m_pg->_repair_oinfo_oid(map);

  logger().debug("{}: done, got {} items", __func__, map.objects.size());
  return 0;
}
#endif

/*
 * Process:
 * Building a map of objects suitable for snapshot validation.
 * The data in m_cleaned_meta_map is the leftover partial items that need to
 * be completed before they can be processed.
 *
 * Snapshots in maps precede the head object, which is why we are scanning
 * backwards.
 */
ScrubMap PgScrubber::clean_meta_map()
{
  ScrubMap for_meta_scrub;

  if (m_end.is_max() || m_cleaned_meta_map.objects.empty()) {
    m_cleaned_meta_map.swap(for_meta_scrub);
  } else {
    auto iter = m_cleaned_meta_map.objects.end();
    --iter;  // not empty, see 'if' clause
    auto begin = m_cleaned_meta_map.objects.begin();
    if (iter->first.has_snapset()) {
      ++iter;
    } else {
      while (iter != begin) {
	auto next = iter--;
	if (next->first.get_head() != iter->first.get_head()) {
	  ++iter;
	  break;
	}
      }
    }
    for_meta_scrub.objects.insert(begin, iter);
    m_cleaned_meta_map.objects.erase(begin, iter);
  }

  return for_meta_scrub;
}

void PgScrubber::run_callbacks()
{
  std::list<Context*> to_run;
  to_run.swap(m_callbacks);

  for (auto& tr : to_run) {
    tr->complete(0);
  }
}

void PgScrubber::maps_compare_n_cleanup()
{
  scrub_compare_maps();
  m_start = m_end;
  run_callbacks();
  requeue_waiting();
}

Scrub::preemption_t& PgScrubber::get_preemptor()
{
  return preemption_data;
}

#if 0
void PgScrubber::requeue_replica(Scrub::scrub_prio_t is_high_priority)
{
  logger().debug("{}", __func__);
  m_osds.queue_for_rep_scrub_resched(m_pg, is_high_priority, m_flags.priority);
}
#endif

/*
 * Process note: called for the arriving "give me your map, replica!" request.
 * Unlike the original implementation, we do not requeue the Op waiting for
 * updates. Instead - we trigger the FSM.
 */
void PgScrubber::replica_scrub_op(RemoteScrubEvent op)
{
#ifdef NOT_YET
  auto msg = op.get_req<MOSDRepScrub>();
  logger().debug("{}: pg:{} Msg: map_epoch: {} min_epoch: {} deep? {}", __func__,
		 m_pg->get_pgid(), msg->map_epoch, msg->min_epoch, msg->deep);

  // are we still processing a previous scrub-map request without noticing that
  // the interval changed? won't see it here, but rather at the reservation
  // stage.

  if (msg->map_epoch < m_pg->get_same_interval_since()) {
    logger().debug("{}: replica_scrub_op discarding old replica_scrub from {} < {}",
		   __func__, msg->map_epoch, m_pg->get_same_interval_since());

    // is there a general sync issue? are we holding a stale reservation?
    // not checking now - assuming we will actively react to interval change.

    return;
  }

  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos = ScrubMapBuilder{};

  m_replica_min_epoch = msg->min_epoch;
  m_start = msg->start;
  m_end = msg->end;
  m_max_end = msg->end;
  m_is_deep = msg->deep;
  m_interval_start = m_pg->get_same_interval_since();
  m_replica_request_priority = msg->high_priority ? Scrub::scrub_prio_t::high_priority
						  : Scrub::scrub_prio_t::low_priority;
  m_flags.priority =
    msg->priority ? msg->priority : m_pg->m_scrub_sched->get_scrub_priority();

  preemption_data.reset();
  preemption_data.force_preemptability(msg->allow_preemption);

  replica_scrubmap_pos.reset();

  // make sure the FSM is at NotActive
  m_fsm->assert_not_active();

  m_osds.queue_for_rep_scrub(m_pg, m_replica_request_priority, m_flags.priority);
#endif
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

seastar::future<> PgScrubber::compare_smaps(const map<pg_shard_t, ScrubMap*>& maps,
					    const set<hobject_t>& master_set)
{
  utime_t now = ceph_clock_now();

  std::for_each(master_set.cbegin(), master_set.cend(), [this, maps](const auto& m) {


  });


  return seastar::make_ready_future();
}

void PgScrubber::scrub_compare_maps()
{
#ifdef NOT_YET

  logger().debug("{}: has maps, analyzing", __func__);

  // construct authoritative scrub map for type-specific scrubbing

  m_cleaned_meta_map.insert(m_primary_scrubmap);

  map<hobject_t, pair<std::optional<uint32_t>, std::optional<uint32_t>>> missing_digest;
  map<pg_shard_t, ScrubMap*> maps;

  maps[m_pg_whoami] = &m_primary_scrubmap;

  for (const auto& i : m_pg->get_acting_recovery_backfill()) {
    if (i == m_pg_whoami)
      continue;
    logger().debug("{} replica {} has {} items", __func__, i,
		   m_received_maps[i].objects.size());
    maps[i] = &m_received_maps[i];
  }

  set<hobject_t> master_set;

  // Construct master set
  for (const auto& map : maps) {
    for (const auto& i : map.second->objects) {
      master_set.insert(i.first);
    }
  }

  stringstream ss;
  m_pg->get_backend().omap_checks(maps, master_set, m_omap_stats, ss);

  if (!ss.str().empty()) {
    m_osds.clog->warn(ss);
  }

  if (m_pg->peering_state.get_acting_recovery_backfill().size() > 1) {

    logger().debug("{}: comparing replica scrub maps", __func__);

    // Map from object with errors to good peer
    map<hobject_t, list<pg_shard_t>> authoritative;

    logger().debug("{}: {} has {} items", __func__, m_pg->get_primary(),
		   m_primary_scrubmap.objects.size());

    ss.str("");
    ss.clear();

    m_pg->get_backend()->be_compare_scrubmaps(
      maps, master_set, state_test(PG_STATE_REPAIR), m_missing, m_inconsistent,
      authoritative, missing_digest, m_shallow_errors, m_deep_errors, m_store.get(),
      m_pg->info.pgid, m_pg->peering_state.get_acting(), ss);
    logger().debug(ss.str());

    if (!ss.str().empty()) {
      m_osds.clog->error(ss);
    }

    for (auto& i : authoritative) {
      list<pair<ScrubMap::object, pg_shard_t>> good_peers;
      for (auto j = i.second.cbegin(); j != i.second.cend(); ++j) {
	good_peers.emplace_back(maps[*j]->objects[i.first], *j);
      }
      m_authoritative.emplace(i.first, good_peers);
    }

    for (auto i = authoritative.begin(); i != authoritative.end(); ++i) {
      m_cleaned_meta_map.objects.erase(i->first);
      m_cleaned_meta_map.objects.insert(
	*(maps[i->second.back()]->objects.find(i->first)));
    }
  }

  auto for_meta_scrub = clean_meta_map();

  // ok, do the pg-type specific scrubbing

  // (Validates consistency of the object info and snap sets)
  scrub_snapshot_metadata(for_meta_scrub, missing_digest);

  // Called here on the primary can use an authoritative map if it isn't the
  // primary
  _scan_snaps(for_meta_scrub);

  if (!m_store->empty()) {

    if (state_test(PG_STATE_REPAIR)) {
      logger().debug("{}: discarding scrub results", __func__);
      m_store->flush(nullptr);
    } else {
      logger().debug("{}: updating scrub object", __func__);
      ObjectStore::Transaction t;
      m_store->flush(&t);
      m_pg->osd->store->queue_transaction(m_pg->ch, std::move(t), nullptr);
    }
  }
#endif
}

/**
 * Send the requested map back to the primary (or - if we
 * were preempted - let the primary know).
 */
void PgScrubber::send_replica_map(::crimson::osd::Scrub::PreemptionNoted was_preempted)
{
  logger().debug("{}: min epoch: {}", __func__, m_replica_min_epoch);

  auto reply = make_message<MOSDRepScrubMap>(m_pg_id, m_replica_min_epoch, m_pg_whoami);
  reply->preempted = (was_preempted == PreemptionNoted::preempted);
  ::encode(replica_scrubmap, reply->get_data());

  m_pg->send_cluster_message(m_pg->get_primary().osd, std::move(reply),
			     m_replica_min_epoch);
  // m_osds.send_to_osd(m_pg->get_primary().osd, std::move(reply), m_replica_min_epoch);
}

/*
 *  - if the replica lets us know it was interrupted, we mark the chunk as
 * interrupted. The state-machine will react to that when all replica maps are
 * received.
 *  - when all maps are received, we signal the FSM with the GotReplicas event
 * (see scrub_send_replmaps_ready()). Note that due to the no-reentrancy
 * limitations of the FSM, we do not 'process' the event directly. Instead - it
 * is queued for the OSD to handle.
 */
void PgScrubber::map_from_replica(RemoteScrubEvent op)
{
#ifdef NOT_YET
  auto m = op->get_req<MOSDRepScrubMap>();
  logger().debug("{}: {}", __func__, *m);

  if (m->map_epoch < m_pg->get_same_interval_since()) {
    logger().debug("{}: discarding old from {} < {}", __func__, m->map_epoch,
		   m_pg->get_same_interval_since());
    return;
  }

  auto p = const_cast<bufferlist&>(m->get_data()).cbegin();

  m_received_maps[m->from].decode(p, m_pg->info.pgid.pool());
  logger().debug("{}: map version is {}", __func__,
		 m_received_maps[m->from].valid_through);

  auto [is_ok, err_txt] = m_maps_status.mark_arriving_map(m->from);
  if (!is_ok) {
    // previously an unexpected map was triggering an assert. Now, as scrubs can
    // be aborted at any time, the chances of this happening have increased, and
    // aborting is not justified
    logger().debug("{}: {} from OSD {}", __func__, err_txt, m->from);
    return;
  }

  if (m->preempted) {
    logger().debug("{}: replica was preempted, setting flag", __func__);
    ceph_assert(preemption_data.is_preemptable());  // otherwise - how dare the replica!
    preemption_data.do_preempt();
  }

  if (m_maps_status.are_all_maps_available()) {
    logger().debug("{}: all repl-maps available", __func__);
    m_osds.queue_scrub_got_repl_maps(m_pg, m_pg->is_scrub_blocking_ops());
  }
#endif
}

void PgScrubber::handle_scrub_reserve_request(RemoteScrubEvent op)
{
#ifdef NOT_YET

  logger().debug("{}: {}", __func__, *op->get_req());
  op->mark_started();
  auto request_ep = op->get_req<MOSDScrubReserve>()->get_map_epoch();

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

  if (request_ep < m_pg->get_same_interval_since()) {
    // will not ack stale requests
    return;
  }

  bool granted{false};
  if (m_remote_osd_resource.has_value()) {

    logger().debug("{}: already reserved.", __func__);
    granted = true;

  } else if (m_pg->cct->_conf->osd_scrub_during_recovery ||
	     !m_osds.is_recovery_active()) {
    m_remote_osd_resource.emplace(m_pg, m_osds, request_ep);
    // OSD resources allocated?
    granted = m_remote_osd_resource->is_reserved();
    if (!granted) {
      // just forget it
      m_remote_osd_resource.reset();
      logger().debug("{}: failed to reserve remotely", __func__);
    }
  }

  logger().debug("{}: reserved? {}", __func__, (granted ? "yes" : "no"));

  Message* reply = new MOSDScrubReserve(
    spg_t(m_pg->info.pgid.pgid, m_pg->get_primary().shard), request_ep,
    granted ? MOSDScrubReserve::GRANT : MOSDScrubReserve::REJECT, m_pg_whoami);

  m_osds.send_message_osd_cluster(reply, op->get_req()->get_connection());
#endif

}

void PgScrubber::handle_scrub_reserve_grant(RemoteScrubEvent op, pg_shard_t from)
{
#ifdef NOT_YET
  logger().debug("{}: {}", __func__, *op->get_req());
  op->mark_started();

  if (m_reservations.has_value()) {
    m_reservations->handle_reserve_grant(op, from);
  } else {
    // RRR derr << __func__ << ": received unsolicited reservation grant from osd " <<
    // from RRR 	 << " (" << op << ")" );
  }
#endif
}

void PgScrubber::handle_scrub_reserve_reject(RemoteScrubEvent op, pg_shard_t from)
{
#ifdef NOT_YET

  logger().debug("{}: {}", __func__, *op->get_req());
  op->mark_started();

  if (m_reservations.has_value()) {
    // there is an active reservation process. No action is required otherwise.
    m_reservations->handle_reserve_reject(op, from);
  }
#endif
}

void PgScrubber::handle_scrub_reserve_release(RemoteScrubEvent op)
{
  logger().debug("{}: {}", __func__, op.get_from());
  //op->mark_started();
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

void PgScrubber::message_all_replicas(int32_t opcode, std::string_view op_text)
{
  ceph_assert(m_pg->peering_state.get_backfill_targets().empty());

  std::vector<std::pair<int, Message*>> messages;
  messages.reserve(m_pg->get_actingset().size());

  epoch_t epch = get_osdmap_epoch();

  const auto not_me = boost::adaptors::filtered([this]( pg_shard_t pgs ){return pgs != m_pg_whoami;});

  for (auto& p : m_pg->get_actingset()|not_me) {

    //if (p == m_pg_whoami)
    //  continue;

    logger().debug("{}: scrub requesting {} from osd.{} Epoch:{}", __func__, op_text, p,
		   epch);

    auto m = make_message<MOSDScrubReserve>(spg_t(m_pg_id.pgid, p.shard),
       epch, opcode, m_pg_whoami);

    m_pg->send_cluster_message(p.osd, std::move(m), epch);
  }
}

/*
logger().debug("{}: min epoch: {}", __func__, m_replica_min_epoch);

auto reply = make_message<MOSDRepScrubMap>(m_pg_id, m_replica_min_epoch, m_pg_whoami);
reply->preempted = (was_preempted == PreemptionNoted::preempted);
::encode(replica_scrubmap, reply->get_data());

m_pg->send_cluster_message(m_pg->get_primary().osd, std::move(reply),
  m_replica_min_epoch);
*/

void PgScrubber::unreserve_replicas()
{
  logger().debug("{}", __func__);
  m_reservations.reset();
}

[[nodiscard]] bool PgScrubber::scrub_process_inconsistent()
{
  logger().debug("{}: checking authoritative", __func__);

  bool repair = state_test(PG_STATE_REPAIR);
  const bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));
  logger().debug("{}: deep_scrub: {} m_is_deep: {} repair: {}", __func__, deep_scrub,
		 m_is_deep, repair);

  // authoritative only store objects which are missing or inconsistent.
  if (!m_authoritative.empty()) {

    stringstream ss;
    ss << m_pg_id << " " << mode << " " << m_missing.size() << " missing, "
       << m_inconsistent.size() << " inconsistent objects";
    logger().debug("{}", ss.str());
    // RRR m_osds.clog->error(ss);

    if (repair) {
      state_clear(PG_STATE_CLEAN);

      for (const auto& [hobj, shrd_list] : m_authoritative) {

	auto missing_entry = m_missing.find(hobj);

	if (missing_entry != m_missing.end()) {
	  repair_object(hobj, shrd_list, missing_entry->second);
	  m_fixed_count += missing_entry->second.size();
	}

	if (m_inconsistent.count(hobj)) {
	  repair_object(hobj, shrd_list, m_inconsistent[hobj]);
	  m_fixed_count += m_inconsistent[hobj].size();
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
  //bool repair = state_test(PG_STATE_REPAIR);
  //bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  //const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));


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
  // finish up
  ObjectStore::Transaction t;
  m_pg->get_peering_state().update_stats(
    [this, shallow_or_deep](auto& history, auto& stats) {
      logger().debug("m_pg->peering_state.update_stats()");
      utime_t now = ceph_clock_now();
      history.last_scrub = m_pg->get_peering_state().get_info().last_update;
      history.last_scrub_stamp = now;
      if (m_is_deep) { // RRR why checking this and not the state flag?
	history.last_deep_scrub = m_pg->peering_state.get_info().last_update;
	history.last_deep_scrub_stamp = now;
      }

      if (shallow_or_deep == scrub_level_t::deep) {

	if ((m_shallow_errors == 0) && (m_deep_errors == 0))
	  history.last_clean_scrub_stamp = now;
	stats.stats.sum.num_shallow_scrub_errors = m_shallow_errors;
	stats.stats.sum.num_deep_scrub_errors = m_deep_errors;
	stats.stats.sum.num_large_omap_objects = m_omap_stats.large_omap_objects;
	stats.stats.sum.num_omap_bytes = m_omap_stats.omap_bytes;
	stats.stats.sum.num_omap_keys = m_omap_stats.omap_keys;
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
	  logger().debug("scrub_finish {} error(s) still present after re-scrub",
			 m_pg->get_peering_state().get_info().stats.stats.sum.num_scrub_errors);
	}
      }
      return true;
    },
    &t);

  std::ignore = m_osds.get_store().do_transaction(m_pg->get_collection_ref(), std::move(t));

  /* RRR if (!m_pg->snap_trimq.empty()) {
    logger().debug("scrub finished, requeuing snap_trimmer");
    m_pg->snap_trimmer_scrub_complete();
  }*/
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

  if (total_errors)
    ;  // m_osds.clog->error(out.data());
  else
    ;  // m_osds.clog->debug(out.data());
}


/*
 * note: only called for the Primary.
 */
void PgScrubber::scrub_finish()
{
  logger().debug("{}: before flags:{} deep_scrub_on_error:{}", __func__, m_flags,
		 m_flags.deep_scrub_on_error);

  //ceph_assert(m_pg->is_locked());

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
  const auto deep_scrub{ deep_scrub_flag ? scrub_level_t::deep : scrub_level_t::shallow };
  const char* mode = (repair ? "repair" : (deep_scrub_flag ? "deep-scrub" : "scrub"));

  bool do_auto_scrub{false};

  // if a regular scrub had errors within the limit, do a deep scrub to auto
  // repair
  if (m_flags.deep_scrub_on_error && m_authoritative.size() &&
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
    (void) m_osds.start_operation<LocalPeeringEvent>(
      m_pg,
      m_osds,
      m_pg->get_pg_whoami(),
      m_pg_id,
      get_osdmap_epoch(),
      get_osdmap_epoch(),
      PeeringState::DoRecovery{});
    //m_pg->queue_peering_event(PGPeeringEventRef(std::make_shared<PGPeeringEvent>(
    //  get_osdmap_epoch(), get_osdmap_epoch(), PeeringState::DoRecovery())));
  } else {
    state_clear(PG_STATE_REPAIR);
  }

  cleanup_on_finish();
  if (do_auto_scrub) {
    request_rescrubbing(m_pg->m_planned_scrub);
  }

  if (m_pg->get_peering_state().is_active() && m_pg->is_primary()) {
    m_pg->get_peering_state().share_pg_info();
  }
}

Scrub::FsmNext PgScrubber::on_digest_updates()
{
  logger().debug("{}: #pending: {} pending? {} {}", __func__, num_digest_updates_pending,
		 (m_end.is_max() ? " <last chunk> " : " <mid chunk> "));

  if (num_digest_updates_pending == 0) {

    // got all updates, and finished with this chunk. Any more?
    if (m_end.is_max()) {
      scrub_finish();
      return Scrub::FsmNext::goto_notactive;
    } else {
      // go get a new chunk (via "requeue")
      preemption_data.reset();
      return Scrub::FsmNext::next_chunk;
    }
  } else {
    return Scrub::FsmNext::do_discard;
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

PgScrubber::~PgScrubber() = default;

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
  //ceph_assert(m_pg->is_locked());

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
  //ceph_assert(m_pg->is_locked());

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
  m_omap_stats = (const struct omap_stat_t){0};

  run_callbacks();

  m_inconsistent.clear();
  m_missing.clear();
  m_authoritative.clear();
  num_digest_updates_pending = 0;
  m_primary_scrubmap = ScrubMap{};
  m_primary_scrubmap_pos.reset();
  replica_scrubmap = ScrubMap{};
  replica_scrubmap_pos.reset();
  m_cleaned_meta_map = ScrubMap{};
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
  m_left =
    static_cast<int>(m_pg->get_cct()->_conf.get_val<uint64_t>("osd_scrub_max_preemptions"));
  m_size_divisor = 1;
}

// ///////////////////// ReplicaReservations //////////////////////////////////
namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
  logger().debug("{}: <ReplicaReservations> release-> {}", __func__, peer);

  auto m = new MOSDScrubReserve(spg_t(m_pg->get_pgid().pgid, peer.shard), epoch,
				MOSDScrubReserve::RELEASE, m_pg->pg_whoami);
  std::ignore = m_osds.send_to_osd(peer.osd, m, epoch);
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

    for (auto p : m_acting_set) {
      if (p == whoami)
	continue;
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
#ifdef NOT_YET
  m_osds.queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);
#endif
}

void ReplicaReservations::send_reject()
{
#ifdef NOT_YET
  m_osds.queue_for_scrub_denied(m_pg, scrub_prio_t::low_priority);
#endif
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
void ReplicaReservations::handle_reserve_grant(RemoteScrubEvent op, pg_shard_t from)
{
  logger().debug("{}: <ReplicaReservations> granted-> {}", __func__, from);
  //op->mark_started();

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

void ReplicaReservations::handle_reserve_reject(RemoteScrubEvent op, pg_shard_t from)
{
  logger().debug("{}: <ReplicaReservations> rejected-> {}", __func__, from);
  //logger().debug("{}: {}", __func__, *op->get_req());
  //op->mark_started();

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

LocalReservation::LocalReservation(crimson::osd::PG* pg, crimson::osd::ScrubQueue& osds) : m_pg{pg}, m_queuer{osds}
{
  if (!m_queuer.inc_scrubs_local()) {
    logger().debug("{}: failed to reserve locally", __func__);
    // the failure is signalled by not having m_holding_local_reservation set
    return;
  }

  logger().debug("{}: local OSD scrub resources reserved", __func__);
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

ReservedByRemotePrimary::ReservedByRemotePrimary(PG* pg, crimson::osd::ScrubQueue& osds, epoch_t epoch)
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

// ///////////////////// MapsCollectionStatus //////////////////////////////////

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

}  // namespace crimson::osd
