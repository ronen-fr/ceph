// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab

#include "./pg_scrubber.h"  // the '.' notation used to affect clang-format order

#include <cmath>
#include <iostream>
#include <vector>

#include "debug.h"

// #include "common/errno.h"
// #include "messages/MOSDOp.h"
// #include "messages/MOSDRepScrub.h"
// #include "messages/MOSDRepScrubMap.h"
// #include "messages/MOSDScrub.h"
// #include "messages/MOSDScrubReserve.h"
// 
// #include "osd/OSD.h"
// #include "osd/osd_types_fmt.h"
// #include "ScrubStore.h"
// #include "scrub_machine.h"

#include "crimson/common/log.h"

#include "crimson/osd/scrubber/scrub_machine_cr.h"


// using std::list;
// using std::map;
// using std::pair;
// using std::set;
// using std::stringstream;
// using std::vector;
using std::ostream;

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

PgScrubber::PgScrubber(PG* pg)
    : m_pg{pg}
    , m_pg_id{pg->get_pgid()}
    , m_osds{m_pg->shard_services}
    , m_pg_whoami{pg->pg_whoami}
    , preemption_data{/*pg*/}
{

  logger().debug("{}: creating PgScrubber for {} / {}", __func__, pg->get_pgid(),
		 m_pg_whoami);
  m_fsm = std::make_unique<Scrub::ScrubMachine>(m_pg, this);
  m_fsm->initiate();
}


// ///////////////////// preemption_data_t //////////////////////////////////

PgScrubber::preemption_data_t::preemption_data_t(PG* pg) : m_pg{pg}
{
  m_left = static_cast<int>(
   m_pg->local_conf()->get_val<uint64_t>("osd_scrub_max_preemptions"));
}

void PgScrubber::preemption_data_t::reset()
{
  //std::lock_guard<std::mutex> lk{m_preemption_lock};

  m_preemptable = false;
  m_preempted = false;
  m_left =
    static_cast<int>(local_conf()->get_val<uint64_t>("osd_scrub_max_preemptions"));
  m_size_divisor = 1;
}


// ///////////////////// ReplicaReservations //////////////////////////////////
namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
//   auto m = new MOSDScrubReserve(spg_t(m_pg_info.pgid.pgid, peer.shard), epoch,
// 				MOSDScrubReserve::RELEASE, m_pg->pg_whoami);
//   m_osds->send_message_osd_cluster(peer.osd, m, epoch);
}

ReplicaReservations::ReplicaReservations(PG* pg, pg_shard_t whoami, ScrubQueue::ScrubJobRef scrubjob)
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
      if (p == whoami)
	continue;
//       auto m = new MOSDScrubReserve(spg_t(m_pg_info.pgid.pgid, p.shard), epoch,
// 				    MOSDScrubReserve::REQUEST, m_pg->pg_whoami);
//       m_osds->send_message_osd_cluster(p.osd, m, epoch);
      m_waited_for_peers.push_back(p);
      logger().info("{}reserve {}", m_log_msg_prefix, p.osd);
    }
  }
}

void ReplicaReservations::send_all_done()
{
  //m_osds->queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::send_reject()
{
  m_scrub_job->resources_failure = true;
  //m_osds->queue_for_scrub_denied(m_pg, scrub_prio_t::low_priority);
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

#ifdef NOT_YET

/**
 *  @ATTN we would not reach here if the ReplicaReservation object managed by the
 * scrubber was reset.
 */
void ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << ": granted by " << from << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  // are we forced to reject the reservation?
  if (m_had_rejections) {

    dout(10) << __func__ << ": rejecting late-coming reservation from "
	     << from << dendl;
    release_replica(from, m_pg->get_osdmap_epoch());

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    dout(10) << __func__ << ": already had osd." << from << " reserved" << dendl;

  } else {

    dout(10) << __func__ << ": osd." << from << " scrub reserve = success"
	     << dendl;
    m_reserved_peers.push_back(from);
    if (--m_pending == 0) {
      send_all_done();
    }
  }
}

void ReplicaReservations::handle_reserve_reject(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << ": rejected by " << from << dendl;
  dout(15) << __func__ << ": " << *op->get_req() << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  if (m_had_rejections) {

    // our failure was already handled when the first rejection arrived
    dout(15) << __func__ << ": ignoring late-coming rejection from "
	     << from << dendl;

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    dout(10) << __func__ << ": already had osd." << from << " reserved" << dendl;

  } else {

    dout(10) << __func__ << ": osd." << from << " scrub reserve = fail" << dendl;
    m_had_rejections = true;  // preventing any additional notifications
    send_reject();
  }
}

#endif

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
    dout(10) << __func__ << ": failed to reserve at Primary request" << dendl;
    // the failure is signalled by not having m_reserved_by_remote_primary set
    return;
  }

  dout(20) << __func__ << ": scrub resources reserved at Primary request" << dendl;
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

// ///////////////////// blocked_range_t ///////////////////////////////

blocked_range_t::blocked_range_t(crimson::osd::ShardServices* osds, ceph::timespan waittime, spg_t pg_id)
    : m_osds{osds}
{
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
}

blocked_range_t::~blocked_range_t()
{
  //std::lock_guard l(m_osds->sleep_lock);
  m_osds->sleep_timer.cancel_event(m_callbk);
}

