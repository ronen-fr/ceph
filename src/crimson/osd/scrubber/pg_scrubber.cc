// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab

#include "./pg_scrubber.h"  // the '.' notation used to affect clang-format order

#include <cmath>
#include <iostream>
#include <vector>

#include "debug.h"

// #include "common/errno.h"
// #include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
// #include "messages/MOSDRepScrubMap.h"
// #include "messages/MOSDScrub.h"
// #include "messages/MOSDScrubReserve.h"
// 
// #include "osd/OSD.h"
// #include "osd/osd_types_fmt.h"
// #include "ScrubStore.h"
// #include "scrub_machine.h"

#include "crimson/common/log.h"
#include "osd/osd_types_fmt.h"


#include "crimson/osd/osd_operations/scrub_event.h"
#include "crimson/osd/scrubber/scrub_machine_cr.h"


// using std::list;
// using std::map;
// using std::pair;
// using std::set;
// using std::stringstream;
// using std::vector;
using std::ostream;

using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;


using crimson::common::local_conf;
using crimson::osd::ScrubEvent;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}



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
    out << " must_repair";
  if (sf.auto_repair)
    out << " auto_repair";
  if (sf.check_repair)
    out << " check_repair";
  if (sf.deep_scrub_on_error)
    out << " deep_scrub_on_error";
  if (sf.must_deep_scrub)
    out << " must_deep_scrub";
  if (sf.must_scrub)
    out << " must_scrub";
  if (sf.time_for_deep)
    out << " time_for_deep";
  if (sf.need_auto)
    out << " need_auto";
  if (sf.req_scrub)
    out << " req_scrub";

  return out;
}

PgScrubber::~PgScrubber()
{
  if (m_scrub_job) {
    // make sure the OSD won't try to scrub this one just now
    rm_from_osd_scrubbing();
    m_scrub_job.reset();
  }
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
  m_fsm = std::make_unique<Scrub::ScrubMachine>(m_pg, this);
  m_fsm->initiate();
  m_scrub_job = ceph::make_ref<ScrubQueue::ScrubJob>(m_pg->get_cct(), m_pg_id, m_pg_whoami.shard);
}

// src/crimson/osd/CMakeFiles/crimson-osd.dir/scrubber/pg_scrubber.cc.o: In function `PgScrubber::PgScrubber(crimson::osd::PG*)':
// /opt/rh/gcc-toolset-9/root/usr/include/c++/9/bits/unique_ptr.h:849: undefined reference to `Scrub::ScrubMachine::ScrubMachine(crimson::osd::PG*, ScrubMachineListener*)'


ostream& PgScrubber::show(ostream& out) const
{
  return out << " [ " << m_pg_id << ": " << m_flags << " ] ";
}



void PgScrubber::scrub_fake_scrub_session(epoch_t epoch_queued)
{
  // mark us as scrubbing,
  // wait for 10 seconds,
  // mark us as done scrubbing

  logger().warn("{}: pg: {} - faking scrub session", __func__, m_pg_id);
  set_scrub_begin_time();
  m_active = true;

 (void)m_pg->get_shard_services().start_operation<ScrubEvent>(
        m_pg, m_pg->get_shard_services(), m_pg_id,
        (ScrubEvent::ScrubEventFwdImm)(&PgScrubber::scrub_fake_scrub_done), m_pg->get_osdmap_epoch(),
        0, 10s);
}


void PgScrubber::scrub_fake_scrub_done(epoch_t epoch_queued)
{
  logger().warn("{}: pg: {} - fake scrub session done", __func__, m_pg_id);
  set_scrub_duration();
  clear_queued_or_active();
  m_active = false;
  clear_scrub_reservations();
  m_pg->set_last_scrub_stamp(ceph_clock_now());
  m_pg->set_last_deep_scrub_stamp(ceph_clock_now());

  // this is the wrong function to call, but for now:
  m_pg->scrub_requested(scrub_level_t::shallow, scrub_type_t::not_repair); // juts sends updates

  m_pg->reschedule_scrub();
}


crimson::osd::ScrubEvent::interruptible_future<>
PgScrubber::scrub_echo(epoch_t epoch_queued)
{
  logger().warn("{}: pg: {} epoch: {} echo block starts", __func__, m_pg_id, epoch_queued);
  return seastar::sleep(1s).then([pg=m_pg_id, epoch_queued]() mutable -> crimson::osd::ScrubEvent::interruptible_future<> {
    logger().warn("scrub_echo: pg: {} epoch: {} echo block done", pg, epoch_queued);
    return seastar::make_ready_future();
  });
  //logger().warn("{}: pg: {} epoch: {} echo block sent", __func__, m_pg_id, epoch_queued);
}

// trying to debug a crash: this version works!
void PgScrubber::scrub_echo_v(epoch_t epoch_queued)
{
  logger().warn("{}: pg: {} epoch: {} echo block starts", __func__, m_pg_id, epoch_queued);
  (void) seastar::sleep(1s).then([pg=m_pg_id, epoch_queued]() /*mutable -> crimson::osd::ScrubEvent::interruptible_future<> */ {
    logger().warn("scrub_echo: pg: {} epoch: {} echo block done", pg, epoch_queued);
  });
}


// -------------------------------------------------------------------------------------------
// the I/F used by the state-machine (i.e. the implementation of ScrubMachineListener)

// -----------------

bool PgScrubber::is_reserving() const
{
  return false; // RRR m_fsm->is_reserving();
}

bool PgScrubber::is_primary() const
{
  return m_pg->is_primary();
  //return m_pg->recovery_state.is_primary(); 
}


  bool PgScrubber::state_test(uint64_t m) const { return m_pg->state_test(m); };
  void PgScrubber::state_set(uint64_t m) { m_pg->state_set(m); }
  void PgScrubber::state_clear(uint64_t m) { m_pg->state_clear(m); }


// ///////////////////////////////////////////////////////////////////// //
// scrub-op registration handling

void PgScrubber::unregister_from_osd()
{
  if (m_scrub_job) {
    logger().debug("{}: prev. state: {}", __func__, registration_state());
    m_osds.get_scrub_services().remove_from_osd_queue(m_scrub_job);
  }
}

bool PgScrubber::is_scrub_registered() const
{
  return m_scrub_job && m_scrub_job->in_queues;
}

std::string_view PgScrubber::registration_state() const
{
  if (m_scrub_job) {
    return m_scrub_job->registration_state();
  }
  return "(no sched job)"sv;
}

void PgScrubber::rm_from_osd_scrubbing()
{
  // make sure the OSD won't try to scrub this one just now
  unregister_from_osd();
}

void PgScrubber::on_primary_change(const requested_scrub_t& request_flags)
{
  logger().info("{}: {} flags:{}", __func__, (is_primary() ? " Primary " : " Replica "), request_flags);

  if (!m_scrub_job) {
    return;
  }

  logger().debug("{}: scrub-job state: {}", __func__, m_scrub_job->state_desc());

  if (is_primary()) {
    auto suggested =  determine_scrub_time(request_flags);
    m_osds.get_scrub_services().register_with_osd(m_scrub_job, suggested);
  } else {
    m_osds.get_scrub_services().remove_from_osd_queue(m_scrub_job);
  }

  logger().debug("{}: done (registration state: {})", __func__, registration_state());
}


void PgScrubber::on_maybe_registration_change(const requested_scrub_t& request_flags)
{
  logger().info("{}: {} flags:{}", __func__, (is_primary() ? " Primary " : " Replica "), request_flags);

  on_primary_change(request_flags);
  logger().debug("{}: done (registration state: {})", __func__, registration_state());
}

void PgScrubber::update_scrub_job(const requested_scrub_t& request_flags)
{
  logger().info("{}: flags:{}", __func__, request_flags);

  {
    // verify that the 'in_q' status matches our "Primariority"
    if (m_scrub_job && is_primary() &&  !m_scrub_job->in_queues) {
      //dout(1) << __func__ << " !!! primary but not scheduled! " << dendl;
      m_pg->get_clog_error() << m_pg->get_pgid() << " primary but not scheduled"
                             << " flags:" << request_flags;
    }
  }

  if (is_primary() && m_scrub_job) {
    auto suggested = determine_scrub_time(request_flags);
    m_osds.get_scrub_services().update_job(m_scrub_job, suggested);
  }

  logger().debug("{}: done (registration state: {})", __func__, registration_state());
}

ScrubQueue::sched_params_t
PgScrubber::determine_scrub_time(const requested_scrub_t& request_flags) const
{
  ScrubQueue::sched_params_t res;

  if (!is_primary()) {
    return res; // with ok_to_scrub set to 'false'
  }

  if (request_flags.must_scrub || request_flags.need_auto) {

    // Set the smallest time that isn't utime_t()
    res.proposed_time = PgScrubber::scrub_must_stamp();
    res.is_must = ScrubQueue::must_scrub_t::mandatory;
    // we do not need the interval data in this case

  } else if (m_pg->get_pg_info(ScrubberPasskey{}).stats.stats_invalid &&
           local_conf()->osd_scrub_invalid_stats) {
    res.proposed_time = ceph_clock_now();
    res.is_must = ScrubQueue::must_scrub_t::mandatory;

  } else {
    res.proposed_time = m_pg->get_pg_info(ScrubberPasskey{}).history.last_scrub_stamp;
    res.min_interval = /* RRR for now */ 100 +
      m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
    res.max_interval = /* RRR for now */ 400 +
      m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  }

  logger().debug("{}: suggested: {} hist: {} v:{} must:{} pool min: {} max: {}",
    __func__, res.proposed_time, m_pg->get_pg_info(ScrubberPasskey{}).history.last_scrub_stamp,
    m_pg->get_pg_info(ScrubberPasskey{}).stats.stats_invalid,
    res.is_must==ScrubQueue::must_scrub_t::mandatory ? "y" : "n",
    res.min_interval, res.max_interval);

  return res;
}



// ///////////////////// preemption_data_t //////////////////////////////////

PgScrubber::preemption_data_t::preemption_data_t(PG* pg) : m_pg{pg}
{
  m_left = static_cast<int>(
   local_conf().get_val<uint64_t>("osd_scrub_max_preemptions"));
}

void PgScrubber::preemption_data_t::reset()
{
  //std::lock_guard<std::mutex> lk{m_preemption_lock};

  m_preemptable = false;
  m_preempted = false;
  m_left =
    static_cast<int>(local_conf().get_val<uint64_t>("osd_scrub_max_preemptions"));
  m_size_divisor = 1;
}


// ///////////////////// ReplicaReservations //////////////////////////////////
namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
  auto reply = crimson::make_message<MOSDScrubReserve>(
    spg_t(m_pg_info.pgid.pgid, m_pg->get_primary().shard), epoch,
    MOSDScrubReserve::RELEASE, m_pg->get_pg_whoami());

  std::ignore = m_osds.send_to_osd(peer.osd, std::move(reply), epoch);

//  RRR get this to work:
//   ceph_assert(!gate.is_closed());
// 
//   gate.dispatch_in_background("ReplicaReservations", *this, [reply=std::move(reply), epoch, peer, this] {
//     return m_osds.send_to_osd(peer.osd, std::move(reply), epoch);
//     //return this->get_connection()->send(std::move(reply));
//   });
}

#if 0

void PGRecovery::request_replica_scan(
  const pg_shard_t& target,
  const hobject_t& begin,
  const hobject_t& end)
{
  logger().debug("{}: target.osd={}", __func__, target.osd);
  auto msg = crimson::make_message<MOSDPGScan>(
    MOSDPGScan::OP_SCAN_GET_DIGEST,
    pg->get_pg_whoami(),
    pg->get_osdmap_epoch(),
    pg->get_last_peering_reset(),
    spg_t(pg->get_pgid().pgid, target.shard),
    begin,
    end);
  std::ignore = pg->get_shard_services().send_to_osd(
    target.osd,
    std::move(msg),
    pg->get_osdmap_epoch());
}


void RecoveryBackend::handle_backfill_finish(
  MOSDPGBackfill& m)
{
  logger().debug("{}", __func__);
  ceph_assert(!pg.is_primary());
  ceph_assert(crimson::common::local_conf()->osd_kill_backfill_at != 1);
  auto reply = crimson::make_message<MOSDPGBackfill>(
    MOSDPGBackfill::OP_BACKFILL_FINISH_ACK,
    pg.get_osdmap_epoch(),
    m.query_epoch,
    spg_t(pg.get_pgid().pgid, pg.get_primary().shard));
  reply->set_priority(pg.get_recovery_op_priority());
  std::ignore = m.get_connection()->send(std::move(reply));
  shard_services.start_operation<crimson::osd::LocalPeeringEvent>(
    static_cast<crimson::osd::PG*>(&pg),
    shard_services,
    pg.get_pg_whoami(),
    pg.get_pgid(),
    pg.get_osdmap_epoch(),
    pg.get_osdmap_epoch(),
    RecoveryDone{});
}

#endif

ReplicaReservations::ReplicaReservations(PG* pg,
                                         pg_shard_t whoami,
                                         ScrubQueue::ScrubJobRef scrubjob)
    : m_pg{pg}
    , m_acting_set{pg->get_actingset(ScrubberPasskey{})}
    , m_osds{m_pg->get_shard_services()}
    , m_pending{static_cast<int>(m_acting_set.size()) - 1}
    , m_pg_info{m_pg->get_pg_info(ScrubberPasskey())}
    , m_scrub_job{scrubjob}
{
  epoch_t epoch = m_pg->get_osdmap_epoch();

  {
    std::stringstream prefix;
    prefix << "osd." << m_osds.whoami << " ep: " << epoch
           << " scrubber::ReplicaReservations pg[" << m_pg->get_pgid() << "]: ";
    m_log_msg_prefix = prefix.str();
  }

  // handle the special case of no replicas
  if (m_pending <= 0) {
    // just signal the scrub state-machine to continue
    send_all_done();

  } else {

    for (auto p : m_acting_set) {
      if (p == whoami) {
        continue;
      }

      auto reply = crimson::make_message<MOSDScrubReserve>(
        spg_t(m_pg_info.pgid.pgid, m_pg->get_primary().shard), epoch,
        MOSDScrubReserve::RELEASE, m_pg->get_pg_whoami());

      std::ignore = m_osds.send_to_osd(p.osd, std::move(reply), epoch);
      m_waited_for_peers.push_back(p);
      logger().info("{}reserve {}", m_log_msg_prefix, p.osd);
    }
  }
}

void ReplicaReservations::send_internal_event(ScrubEvent::ScrubEventFwdImm evt)
{
  ceph_assert(!gate.is_closed());

  /*gate.dispatch_in_background("ReplicaReservations", *this, [this, evt](){
   return*/ m_pg->get_shard_services().start_operation<ScrubEvent>(
    m_pg,
    m_pg->get_shard_services(),
    m_pg->get_pgid(),
    (ScrubEvent::ScrubEventFwdImm)(evt),
    m_pg->get_osdmap_epoch(),
    0);
//});
}

void ReplicaReservations::send_all_done()
{
  //m_osds->queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);
  // -> queue_scrub_event_msg<PGScrubResourcesOK>(..)
  // -> pg->scrub_send_resources_granted(epoch_queued, handle);
  // -> forward_scrub_event(&ScrubPgIF::send_remotes_reserved, queued, "RemotesReserved");

//   m_pg->get_shard_services().start_operation<ScrubEvent>(
//     m_pg,
//     m_pg->get_shard_services(),
//     m_pg->get_pgid(),
//     (ScrubEvent::ScrubEventFwdImm)(&PgScrubber::send_remotes_reserved),
//     m_pg->get_osdmap_epoch());

  send_internal_event(static_cast<ScrubEvent::ScrubEventFwdImm>(&PgScrubber::send_remotes_reserved));
}


void ReplicaReservations::send_reject()
{
  m_scrub_job->resources_failure = true;
  //m_osds->queue_for_scrub_denied(m_pg, scrub_prio_t::low_priority);
  send_internal_event(static_cast<ScrubEvent::ScrubEventFwdImm>(&PgScrubber::send_reservation_failure));
}


void ReplicaReservations::discard_all()
{
  logger().info("{}/{}: reserved peers: {}", m_log_msg_prefix, __func__, m_reserved_peers);

  m_had_rejections = true;  // preventing late-coming responses from triggering events
  m_reserved_peers.clear();
  m_waited_for_peers.clear();
}


ReplicaReservations::~ReplicaReservations()
{
  m_had_rejections = true;  // preventing late-coming responses from triggering events

  // send un-reserve messages to all reserved replicas. We do not wait for answer (there
  // wouldn't be one). Other incoming messages will be discarded on the way, by our
  // owner.
  epoch_t epoch = m_pg->get_osdmap_epoch();

  for (auto& p : m_reserved_peers) {
    release_replica(p, epoch);
  }
  m_reserved_peers.clear();

  // note: the release will follow on the heels of the request. When tried otherwise,
  // grants that followed a reject arrived after the whole scrub machine-state was
  // reset, causing leaked reservations.
  for (auto& p : m_waited_for_peers) {
    release_replica(p, epoch);
  }
  m_waited_for_peers.clear();
}


seastar::future<> ReplicaReservations::stop()
{
  logger().info("{}", __func__);
  return gate.close();
}



/**
 *  @ATTN we would not reach here if the ReplicaReservation object managed by the
 * scrubber was reset.
 */
void ReplicaReservations::handle_reserve_grant(Ref<crimson::osd::RemoteScrubEvent> op, pg_shard_t from)
{
  logger().info("{}: pg[{}]: granted by {}", m_log_msg_prefix, m_pg->get_pgid(), from);
  //op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  // are we forced to reject the reservation?
  if (m_had_rejections) {

    logger().info("{}: pg[{}]: rejecting late-coming reservation from {}", m_log_msg_prefix, m_pg->get_pgid(), from);
    release_replica(from, m_pg->get_osdmap_epoch());

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    logger().info("{}: pg[{}]: already had osd.{} reserved", m_log_msg_prefix, m_pg->get_pgid(), from);

  } else {

    logger().info("{}: pg[{}]: osd.{} scrub reserve = success",
                  m_log_msg_prefix, m_pg->get_pgid(), from);
    m_reserved_peers.push_back(from);
    if (--m_pending == 0) {
      send_all_done();
    }
  }
}

void ReplicaReservations::handle_reserve_reject(Ref<crimson::osd::RemoteScrubEvent> op, pg_shard_t from)
{
  logger().info("{}: pg[{}]: rejected by {}", m_log_msg_prefix, m_pg->get_pgid(), from);
  //dout(15) << __func__ << ": " << *op->get_req() << dendl;

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  if (m_had_rejections) {

    // our failure was already handled when the first rejection arrived
    logger().debug("{}: pg[{}]: ignoring late-coming rejection from {}",
                   m_log_msg_prefix, m_pg->get_pgid(), from);

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    logger().info("{}: pg[{}]: already had osd.{} reserved",
                  m_log_msg_prefix, m_pg->get_pgid(), from);

  } else {

    logger().info("{}: pg[{}]: osd.{} scrub reserve = fail",
                  m_log_msg_prefix, m_pg->get_pgid(), from);
    m_had_rejections = true;  // preventing any additional notifications
    send_reject();
  }
}


std::ostream& ReplicaReservations::gen_prefix(std::ostream& out) const
{
  return out << m_log_msg_prefix;
}


// ///////////////////// LocalReservation //////////////////////////////////

// note: no dout()s in LocalReservation functions. Client logs interactions.
LocalReservation::LocalReservation(crimson::osd::ShardServices* osds)
    : m_osds{osds}
{
  if (m_osds->get_scrub_services().inc_scrubs_local()) {
    // the failure is signalled by not having m_holding_local_reservation set
    m_holding_local_reservation = true;
  }
}

LocalReservation::~LocalReservation()
{
  if (m_holding_local_reservation) {
    m_holding_local_reservation = false;
    m_osds->get_scrub_services().dec_scrubs_local();
  }
}

// ///////////////////// ReservedByRemotePrimary ///////////////////////////////

ReservedByRemotePrimary::ReservedByRemotePrimary(const PgScrubber* scrubber,
						 PG* pg,
						 crimson::osd::ShardServices* osds,
						 epoch_t epoch)
    : m_scrubber{scrubber}
    , m_pg{pg}
    , m_osds{osds}
    , m_reserved_at{epoch}
{
  if (!m_osds->get_scrub_services().inc_scrubs_remote()) {
    logger().info("{}: failed to reserve at Primary request", __func__);
    // the failure is signalled by not having m_reserved_by_remote_primary set
    return;
  }

  logger().debug("{}: reserved scrub resources at Primary request", __func__);
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
    m_osds->get_scrub_services().dec_scrubs_remote();
  }
}

std::ostream& ReservedByRemotePrimary::gen_prefix(std::ostream& out) const
{
  return out; // RRR m_scrubber->gen_prefix(out);
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

ostream& operator<<(ostream& out, const ::Scrub::MapsCollectionStatus& sf)
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


// ///////////////////// blocked_range_t ///////////////////////////////


blocked_range_t::blocked_range_t(crimson::osd::ShardServices* osds, ceph::timespan waittime, spg_t pg_id)
    : m_osds{osds}
{
#ifdef NOT_YET
  auto now_is = std::chrono::system_clock::now();
  m_callbk = new LambdaContext([now_is, pg_id, osds]([[maybe_unused]] int r) {
    std::time_t now_c = std::chrono::system_clock::to_time_t(now_is);
    char buf[50];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", std::localtime(&now_c));
    lgeneric_subdout(g_ceph_context, osd, 10)
      << "PgScrubber: " << pg_id << " blocked on an object for too long (since " << buf
      << ")" << dendl;
    osds->clog->warn() << "osd." << osds->whoami << " PgScrubber: " << pg_id << " blocked on an object for too long (since " << buf << ")";
    return;
  });

  //std::lock_guard l(m_osds->sleep_lock);
  m_osds->sleep_timer.add_event_after(waittime, m_callbk);
#endif

}

blocked_range_t::~blocked_range_t()
{
  // not yet : m_osds->sleep_timer.cancel_event(m_callbk);
}

}

void PgScrubber::initiate_regular_scrub(epoch_t epoch_queued)
{
  logger().debug("{}: epoch: {}", __func__, epoch_queued);

  // not sure we really need to check for interval changes, as this is part of the
  // interruptible_future work.

  // we may have lost our Primary status while the message languished in the queue
  if (check_interval(epoch_queued)) {
    logger().info("scrubber event -->> StartScrub epoch: {}", epoch_queued);
    reset_epoch(epoch_queued);
    m_fsm->process_event(StartScrub{});
    logger().info("scrubber event --<< StartScrub");
  } else {
    clear_queued_or_active(); // RRR make sure we do this if the interruptable future is interrupted
  }
}




// fakes

ScrubEIF PgScrubber::initiate_regular_scrub_v2(epoch_t epoch_queued)
{
  logger().debug("{}: epoch: {}", __func__, epoch_queued);
  // we may have lost our Primary status while the message languished in the queue
  if (check_interval(epoch_queued)) {
    logger().info("scrubber event -->> StartScrub epoch: {}", epoch_queued);
    reset_epoch(epoch_queued);
    //m_fsm->process_event(StartScrub{});
    logger().info("scrubber event --<< StartScrub");
  } else {
    clear_queued_or_active();
  }
  return seastar::now();
}

void PgScrubber::initiate_scrub_after_repair(epoch_t epoch_queued) {}

void PgScrubber::send_scrub_resched(epoch_t epoch_queued) {}

void PgScrubber::active_pushes_notification(epoch_t epoch_queued) {}

void PgScrubber::update_applied_notification(epoch_t epoch_queued) {}

void PgScrubber::send_scrub_unblock(epoch_t epoch_queued) {}

void PgScrubber::digest_update_notification(epoch_t epoch_queued) {}

void PgScrubber::send_replica_maps_ready(epoch_t epoch_queued) {}

void PgScrubber::send_start_replica(epoch_t epoch_queued,
                                    Scrub::act_token_t token)
{}

void PgScrubber::send_sched_replica(epoch_t epoch_queued,
                                    Scrub::act_token_t token)
{}

void PgScrubber::send_replica_pushes_upd(epoch_t epoch_queued) {}

void PgScrubber::on_applied_when_primary(const eversion_t& applied_version) {}

void PgScrubber::send_full_reset(epoch_t epoch_queued) {}

void PgScrubber::send_chunk_free(epoch_t epoch_queued) {}

void PgScrubber::send_chunk_busy(epoch_t epoch_queued) {}

void PgScrubber::send_local_map_done(epoch_t epoch_queued) {}

void PgScrubber::send_maps_compared(epoch_t epoch_queued) {}

void PgScrubber::send_get_next_chunk(epoch_t epoch_queued) {}

void PgScrubber::send_scrub_is_finished(epoch_t epoch_queued) {}

bool PgScrubber::write_blocked_by_scrub(const hobject_t& soid)
{
  return false;
}

bool PgScrubber::range_intersects_scrub(const hobject_t& start,
                                        const hobject_t& end)
{
  return false;
}

void PgScrubber::dispatch_reserve_message(Ref<crimson::osd::RemoteScrubEvent> op)
{
  MOSDScrubReserve* m = static_cast<MOSDScrubReserve*>(op->get_payload_msg());

  switch (m->type) {
   case MOSDScrubReserve::REQUEST:
    return handle_scrub_reserve_request(op, m);
   case MOSDScrubReserve::RELEASE:
    return handle_scrub_reserve_release(op, m);

   case MOSDScrubReserve::GRANT:
    return handle_scrub_reserve_grant(op, m->from);
   case MOSDScrubReserve::REJECT:
    return handle_scrub_reserve_reject(op, m->from);
  }
}



void PgScrubber::handle_scrub_reserve_request(Ref<crimson::osd::RemoteScrubEvent> op, MOSDScrubReserve* m)
{
  logger().info("{}: pg[{}] got scrub reserve request", __func__, m_pg->get_pgid());

  auto request_ep = m->map_epoch;

  /*
   *  if we are currently holding a reservation, then:
   *  either (1) we, the scrubber, did not yet notice an interval change. The remembered
   *  reservation epoch is from before our interval, and we can silently discard the
   *  reservation (no message is required).
   *  or:
   *  (2) the interval hasn't changed, but the same Primary that (we think) holds the
   *  lock just sent us a new request. Note that we know it's the same Primary, as
   *  otherwise the interval would have changed.
   *  Ostensibly we can discard & redo the reservation. But then we
   *  will be temporarily releasing the OSD resource - and might not be able to grab it
   *  again. Thus, we simply treat this as a successful new request
   *  (but mark the fact that if there is a previous request from the primary to
   *  scrub a specific chunk - that request is now defunct).
   */

  if (m_remote_osd_resource.has_value() && m_remote_osd_resource->is_stale()) {
    // we are holding a stale reservation from a past epoch
    m_remote_osd_resource.reset();
    logger().info("{}: pg[{}] cleared existing stale reservation", __func__, m_pg->get_pgid());
  }

  if (request_ep < m_pg->get_same_interval_since()) {
    // will not ack stale requests
    return;
  }

  bool is_granted{false};
  if (m_remote_osd_resource.has_value()) {

    logger().info("{}: pg[{}] already reserved.", __func__, m_pg->get_pgid());

    /*
     * it might well be that we did not yet finish handling the latest scrub-op from
     * our primary. This happens, for example, if 'noscrub' was set via a command, then
     * reset. The primary in this scenario will remain in the same interval, but we do need
     * to reset our internal state (otherwise - the first renewed 'give me your scrub map'
     * from the primary will see us in active state, crashing the OSD).
     */
    advance_token();
    is_granted = true;

  } else if (local_conf()->osd_scrub_during_recovery ||
	     !m_osds.is_recovery_active()) {

    m_remote_osd_resource.emplace(this, m_pg, &m_osds, request_ep);

    // was the OSD resources allocated?
    is_granted = m_remote_osd_resource->is_reserved();
    if (!is_granted) {
      // just forget it
      m_remote_osd_resource.reset();
      logger().info("{}: pg[{}] failed to reserve remotely", __func__, m_pg->get_pgid());
    }
  }

  logger().info("{}: pg[{}] reserved? {}", __func__, m_pg->get_pgid(), (is_granted ? "yes" : "no"));

  auto reply = crimson::make_message<MOSDScrubReserve>(
     spg_t(m_pg_id.pgid, m_pg->get_primary().shard), request_ep,
    is_granted ? MOSDScrubReserve::GRANT : MOSDScrubReserve::REJECT, m_pg_whoami);


  /*return*/(void) m->get_connection()->send(std::move(reply));
}

void PgScrubber::handle_scrub_reserve_grant(Ref<crimson::osd::RemoteScrubEvent> op, pg_shard_t from)
{
  logger().info("{}: pg[{}] got scrub granted by {}", __func__, m_pg->get_pgid(), from);

  if (m_reservations.has_value()) {
    m_reservations->handle_reserve_grant(op, from);
  } else {
    derr << __func__ << ": received unsolicited reservation grant from osd " << from
	 << " (" << op << ")" << dendl;
  }
}

void PgScrubber::handle_scrub_reserve_reject(Ref<crimson::osd::RemoteScrubEvent> op, pg_shard_t from)
{
  logger().info("{}: pg[{}] got scrub rejected by {}", __func__, m_pg->get_pgid(), from);

  if (m_reservations.has_value()) {
    // there is an active reservation process. No action is required otherwise.
    m_reservations->handle_reserve_reject(op, from);
  }
}

void PgScrubber::handle_scrub_reserve_release(Ref<crimson::osd::RemoteScrubEvent> op, MOSDScrubReserve* m)
{
  logger().info("{}: pg[{}] got scrub release", __func__, m_pg->get_pgid());

  /*
   * this specific scrub session has terminated. All incoming events carrying the old
   * tag will be discarded.
   */
  advance_token();
  // RRR handle the gate m_remote_osd_resource->close();
  m_remote_osd_resource.reset();
}


// a future<> returning function?
void PgScrubber::discard_replica_reservations() {}

void PgScrubber::clear_scrub_reservations()
{
  logger().info("scrubber: clear_scrub_reservations");
  m_reservations.reset();	  // the remote reservations
  m_local_osd_resource.reset();	  // the local reservation
  m_remote_osd_resource.reset();  // we as replica reserved for a Primary
}

void PgScrubber::unreserve_replicas() {}

void PgScrubber::scrub_requested(scrub_level_t scrub_level,
                                 scrub_type_t scrub_type,
                                 requested_scrub_t& req_flags)
{}

bool PgScrubber::reserve_local()
{
  // try to create the reservation object (which translates into asking the
  // OSD for the local scrub resource). If failing - undo it immediately

  m_local_osd_resource.emplace(&m_osds);
  if (m_local_osd_resource->is_reserved()) {
    logger().info("{}: pg[{}]: local resources reserved", __func__, m_pg_id);
    return true;
  }

  logger().warn("{}: pg[{}]: failed to reserve local scrub resources", __func__, m_pg_id);
  m_local_osd_resource.reset();
  return false;
}


/*
 * note that the flags-set fetched from the PG (m_pg->m_planned_scrub)
 * is cleared once scrubbing starts; Some of the values dumped here are
 * thus transitory.
 */
void PgScrubber::dump_scrubber(ceph::Formatter* f,
			       const requested_scrub_t& request_flags) const
{
  f->open_object_section("scrubber");

  if (m_active) {  // TBD replace with PR#42780's test
    f->dump_bool("active", true);
    dump_active_scrubber(f, state_test(PG_STATE_DEEP_SCRUB));
  } else {
    f->dump_bool("active", false);
    f->dump_bool("must_scrub",
		 (m_pg->m_planned_scrub.must_scrub || m_flags.required));
    f->dump_bool("must_deep_scrub", request_flags.must_deep_scrub);
    f->dump_bool("must_repair", request_flags.must_repair);
    f->dump_bool("need_auto", request_flags.need_auto);

    f->dump_stream("scrub_reg_stamp") << m_scrub_job->get_sched_time();

    // note that we are repeating logic that is coded elsewhere (currently PG.cc).
    // This is not optimal.
    bool deep_expected = (ceph_clock_now() >= m_pg->next_deepscrub_interval()) ||
			 request_flags.must_deep_scrub || request_flags.need_auto;
    auto sched_state =
      m_scrub_job->scheduling_state(ceph_clock_now(), deep_expected);
    f->dump_string("schedule", sched_state);
  }

  if (m_publish_sessions) {
    f->dump_int("test_sequence",
                m_sessions_counter);  // an ever-increasing number used by tests
  }

  f->close_section();
}

void PgScrubber::dump_active_scrubber(ceph::Formatter* f, bool is_deep) const
{
  f->dump_stream("epoch_start") << m_interval_start;
  f->dump_stream("start") << m_start;
  f->dump_stream("end") << m_end;
  f->dump_stream("max_end") << m_max_end;
  f->dump_stream("subset_last_update") << m_subset_last_update;
  // note that m_is_deep will be set some time after PG_STATE_DEEP_SCRUB is
  // asserted. Thus, using the latter.
  f->dump_bool("deep", is_deep);

  // dump the scrub-type flags
  f->dump_bool("req_scrub", m_flags.required);
  f->dump_bool("auto_repair", m_flags.auto_repair);
  f->dump_bool("check_repair", m_flags.check_repair);
  f->dump_bool("deep_scrub_on_error", m_flags.deep_scrub_on_error);
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
  f->dump_string("schedule", "scrubbing");
}

pg_scrubbing_status_t PgScrubber::get_schedule() const
{
  logger().debug("{}: pg[{}]", __func__, m_pg_id);

  if (!m_scrub_job) {
    return pg_scrubbing_status_t{};
  }

  auto now_is = ceph_clock_now();

  if (m_active) {
    // report current scrub info, including updated duration
    int32_t duration = (utime_t{now_is} - scrub_begin_stamp).sec();

    return pg_scrubbing_status_t{
      utime_t{},
      duration,
      pg_scrub_sched_status_t::active,
      true,  // active
      (m_is_deep ? scrub_level_t::deep : scrub_level_t::shallow),
      false /* is periodic? unknown, actually */};
  }
  if (m_scrub_job->state != ScrubQueue::qu_state_t::registered) {
    return pg_scrubbing_status_t{utime_t{},
                                 0,
                                 pg_scrub_sched_status_t::not_queued,
                                 false,
                                 scrub_level_t::shallow,
                                 false};
  }

  // Will next scrub surely be a deep one? note that deep-scrub might be
  // selected even if we report a regular scrub here.
  bool deep_expected = (now_is >= m_pg->next_deepscrub_interval()) ||
                       m_pg->m_planned_scrub.must_deep_scrub ||
                       m_pg->m_planned_scrub.need_auto;
  scrub_level_t expected_level =
    deep_expected ? scrub_level_t::deep : scrub_level_t::shallow;
  bool periodic = !m_pg->m_planned_scrub.must_scrub &&
                  !m_pg->m_planned_scrub.need_auto &&
                  !m_pg->m_planned_scrub.must_deep_scrub;

  // are we ripe for scrubbing?
  if (now_is > m_scrub_job->schedule.scheduled_at) {
    // we are waiting for our turn at the OSD.
    return pg_scrubbing_status_t{m_scrub_job->schedule.scheduled_at,
                                 0,
                                 pg_scrub_sched_status_t::queued,
                                 false,
                                 expected_level,
                                 periodic};
  }

  return pg_scrubbing_status_t{m_scrub_job->schedule.scheduled_at,
                               0,
                               pg_scrub_sched_status_t::scheduled,
                               false,
                               expected_level,
                               periodic};
}

void PgScrubber::handle_query_state(ceph::Formatter* f)
{
  logger().debug("{}: pg[{}]", __func__, m_pg_id);

  f->open_object_section("scrub");
  f->dump_stream("scrubber.epoch_start") << m_interval_start;
  f->dump_bool("scrubber.active", m_active);
  f->dump_stream("scrubber.start") << m_start;
  f->dump_stream("scrubber.end") << m_end;
  f->dump_stream("scrubber.max_end") << m_max_end;
  f->dump_stream("scrubber.subset_last_update") << m_subset_last_update;
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

unsigned int PgScrubber::scrub_requeue_priority(
  Scrub::scrub_prio_t with_priority, unsigned int suggested_priority) const
{
  return 100;
}

unsigned int PgScrubber::scrub_requeue_priority(
  Scrub::scrub_prio_t with_priority) const
{
  return 100;
}

void PgScrubber::scrub_clear_state() {}

void PgScrubber::stats_of_handled_objects(const object_stat_sum_t& delta_stats,
                                          const hobject_t& soid)
{}
void PgScrubber::set_op_parameters(requested_scrub_t& request) {}

void PgScrubber::cleanup_store(ObjectStore::Transaction* t) {}

bool PgScrubber::get_store_errors(const scrub_ls_arg_t& arg,
                                  scrub_ls_result_t& res_inout) const
{
  return false;
}


int PgScrubber::asok_debug(std::string_view cmd,
                           std::string param,
                           Formatter* f,
                           std::stringstream& ss)
{
  logger().info("{}: cmd: {}, param: {}", __func__, cmd, param);

  if (cmd == "block") {
    // set a flag that will cause the next 'select_range' to report a blocked object
    m_debug_blockrange = 1;

  } else if (cmd == "unblock") {
    // send an 'unblock' event, as if a blocked range was freed
    m_debug_blockrange = 0;
    //m_fsm->process_event(Unblocked{});

  } else if ((cmd == "set") || (cmd == "unset")) {

    if (param == "sessions") {
      // set/reset the inclusion of the scrub sessions counter in 'query' output
      m_publish_sessions = (cmd == "set");

    } else if (param == "block") {
      if (cmd == "set") {
        // set a flag that will cause the next 'select_range' to report a blocked object
        m_debug_blockrange = 1;
      } else {
      // send an 'unblock' event, as if a blocked range was freed
        m_debug_blockrange = 0;
        //m_fsm->process_event(Unblocked{});
      }
    }
  }
  return 0;
}

void PgScrubber::select_range_n_notify() {}

Scrub::BlockedRangeWarning PgScrubber::acquire_blocked_alarm()
{
  return Scrub::BlockedRangeWarning();
}

eversion_t PgScrubber::search_log_for_updates() const
{
  return eversion_t();
}

eversion_t PgScrubber::get_last_update_applied() const
{
  return eversion_t();
}

int PgScrubber::pending_active_pushes() const
{
  return 0;
}

void PgScrubber::on_init() {}

void PgScrubber::on_replica_init() {}

void PgScrubber::replica_handling_done() {}

void PgScrubber::clear_pgscrub_state() {}

void PgScrubber::add_delayed_scheduling() {}

void PgScrubber::get_replicas_maps(bool replica_can_preempt) {}

void PgScrubber::on_digest_updates() {}





ScrubMachineListener::MsgAndEpoch PgScrubber::prep_replica_map_msg(
  Scrub::PreemptionNoted was_preempted)
{
  return ScrubMachineListener::MsgAndEpoch();
}

void PgScrubber::send_replica_map(
  const ScrubMachineListener::MsgAndEpoch& preprepared)
{}

void PgScrubber::send_preempted_replica() {}

void PgScrubber::send_remotes_reserved(epoch_t epoch_queued) {}

void PgScrubber::send_reservation_failure(epoch_t epoch_queued) {}

[[nodiscard]] bool PgScrubber::has_pg_marked_new_updates() const
{
  return false;
}

void PgScrubber::set_subset_last_update(eversion_t e) {}

void PgScrubber::maps_compare_n_cleanup() {}

Scrub::preemption_t& PgScrubber::get_preemptor()
{
  return preemption_data;
}

seastar::future<> PgScrubber::build_primary_map_chunk()
{
  return seastar::now();
}

void PgScrubber::initiate_primary_map_build() {}

seastar::future<> PgScrubber::build_replica_map_chunk()
{
  return seastar::now();
}

void PgScrubber::reserve_replicas() {}

void PgScrubber::set_reserving_now() {}
void PgScrubber::clear_reserving_now() {}

[[nodiscard]] bool PgScrubber::was_epoch_changed() const
{
  return false;
}

void PgScrubber::set_queued_or_active()
{
  m_queued_or_active = true;
}

void PgScrubber::clear_queued_or_active()
{
  m_queued_or_active = false;
}

bool PgScrubber::is_queued_or_active() const
{
  return m_queued_or_active;
}

void PgScrubber::mark_local_map_ready() {}

[[nodiscard]] bool PgScrubber::are_all_maps_available() const
{
  return true;
}

std::string PgScrubber::dump_awaited_maps() const
{
  return "";
}

void PgScrubber::set_scrub_begin_time() {}

void PgScrubber::set_scrub_duration() {}

// [[nodiscard]] bool PgScrubber::is_scrub_registered() const;

//  std::string_view PgScrubber::registration_state() const;

void PgScrubber::reset_internal_state() {}

void PgScrubber::advance_token() {}

// bool PgScrubber::is_token_current(Scrub::act_token_t received_token) const {
// return true; }

void PgScrubber::_scan_snaps(ScrubMap& smap) {}

ScrubMap PgScrubber::clean_meta_map()
{
  return ScrubMap();
}

void PgScrubber::reset_epoch(epoch_t epoch_queued) {}

void PgScrubber::run_callbacks() {}

bool PgScrubber::is_message_relevant(epoch_t epoch_to_verify)
{
  return true;
}

[[nodiscard]] bool PgScrubber::should_abort() const
{
  return false;
}


[[nodiscard]] bool PgScrubber::verify_against_abort(epoch_t epoch_to_verify)
{
  return true;
}

[[nodiscard]] bool PgScrubber::check_interval(epoch_t epoch_to_verify)
{
  return true;
}

void PgScrubber::final_cstat_update(scrub_level_t shallow_or_deep)
{
  logger().debug("scrubber {}(): {}/{} {}", __func__, m_shallow_errors, m_deep_errors, (shallow_or_deep == scrub_level_t::deep));

#ifdef NOT_YET___NEED_BE
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

void PgScrubber::scrub_finish()
{
//  dout(10) << __func__ << " before flags: " << m_flags
//	   << ". repair state: " << (state_test(PG_STATE_REPAIR) ? "repair" : "no-repair")
//	   << ". deep_scrub_on_error: " << m_flags.deep_scrub_on_error << dendl;
  logger().debug("{}: before flags:{} deep_scrub_on_error:{}", __func__, m_flags,
		 m_flags.deep_scrub_on_error);

  ceph_assert(is_queued_or_active());

  m_pg->m_planned_scrub = requested_scrub_t{};

  // if the repair request comes from auto-repair and large number of errors,
  // we would like to cancel auto-repair
  if (m_is_repair && m_flags.auto_repair &&
      m_authoritative.size() > local_conf()->osd_scrub_auto_repair_num_errors) {

    logger().debug("{}: undoing the repair", __func__);
    state_clear(PG_STATE_REPAIR); // not expected to be set, anyway
    m_is_repair = false;
    update_op_mode_text();
  }


  // RRR fix the copied lines below to match new handling of mode:
  const bool deep_scrub_flag{state_test(PG_STATE_DEEP_SCRUB)};
  const auto deep_scrub{deep_scrub_flag ? scrub_level_t::deep : scrub_level_t::shallow};
  const char* mode = (m_is_repair ? "repair" : (deep_scrub_flag ? "deep-scrub" : "scrub"));

  bool do_auto_scrub{false};

  // if a regular scrub had errors within the limit, do a deep scrub to auto repair
  if (m_flags.deep_scrub_on_error && !m_authoritative.empty() &&
      m_authoritative.size() <= local_conf()->osd_scrub_auto_repair_num_errors) {

    ceph_assert(!m_is_deep);
    do_auto_scrub = true;
    logger().debug("{}: Try to auto repair after scrub errors", __func__);
  }

  m_flags.deep_scrub_on_error = false;

  verify_cstat(m_is_repair, deep_scrub, mode);

  bool has_error = scrub_process_inconsistent();

  log_results(m_is_repair, deep_scrub, mode);

  // Since we don't know which errors were fixed, we can only clear them
  // when every one has been fixed.
  if (m_is_repair) {
    if (m_fixed_count == m_shallow_errors + m_deep_errors) {

      ceph_assert(m_is_deep);
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
    (void)m_osds.start_operation<crimson::osd::LocalPeeringEvent>(
      m_pg, m_osds, m_pg->get_pg_whoami(), m_pg_id, get_osdmap_epoch(),
      get_osdmap_epoch(), PeeringState::DoRecovery{});
  } else {
    m_is_repair = false;
    state_clear(PG_STATE_REPAIR);
    update_op_mode_text();
  }

  cleanup_on_finish();
  if (do_auto_scrub) {
    request_rescrubbing(m_pg->m_planned_scrub);
  }/* else {
    // adjust the time of the next scrub
    m_scrub_reg_stamp = m_osds.m_scrub_queue.update_scrub_job(
      m_pg_id, m_scrub_reg_stamp, ceph_clock_now(), ScrubQueue::must_scrub_t::not_mandatory);
  }*/

  if (m_pg->get_peering_state().is_active() && m_pg->is_primary()) {
    m_pg->get_peering_state().share_pg_info();
  }
}
