// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include "./scrub_reservations.h"

#include <fmt/ranges.h>

#include <cmath>
#include <iostream>
#include <vector>

#include "debug.h"

#include "common/ceph_time.h"
#include "common/errno.h"
#include "include/utime_fmt.h"
#include "messages/MOSDOp.h"
#include "messages/MOSDRepScrub.h"
#include "messages/MOSDRepScrubMap.h"
#include "messages/MOSDScrubReserve.h"
#include "osd/OSD.h"
#include "osd/PG.h"
#include "osd/osd_types_fmt.h"

using std::list;
using std::vector;
using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;

#define dout_context (m_osds->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this)

template <class T>
static ostream& _prefix(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout);
}

namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
  auto m = new MOSDScrubReserve(
      spg_t(m_pg_info.pgid.pgid, peer.shard), epoch, MOSDScrubReserve::RELEASE,
      m_pg->pg_whoami);
  m_osds->send_message_osd_cluster(peer.osd, m, epoch);
}

void ReplicaReservations::send_a_request(pg_shard_t peer, epoch_t epoch)
{
  auto m = make_message<MOSDScrubReserve>(
      spg_t(m_pg_info.pgid.pgid, peer.shard), epoch, MOSDScrubReserve::REQUEST,
      m_pg->pg_whoami);
  m_pg->send_cluster_message(peer.osd, m, epoch, false);
  m_requests_sent++;
  m_request_sent_at = clock::now();
  dout(10) << fmt::format(
		  "{}: reserving {} ({} of {})", __func__, *m_next_to_request,
		  m_requests_sent, m_total_needeed)
	   << dendl;
}

ReplicaReservations::ReplicaReservations(
    PG* pg,
    pg_shard_t whoami,
    ScrubQueue::ScrubJobRef scrubjob,
    const ConfigProxy& conf)
    : m_pg{pg}
    , m_osds{m_pg->get_pg_osd(ScrubberPasskey())}
    , m_pg_info{m_pg->get_pg_info(ScrubberPasskey())}
    , m_scrub_job{scrubjob}
    , m_conf{conf}
{
  epoch_t epoch = m_pg->get_osdmap_epoch();

  // sort the acting set, so that we send the requests in a consistent order
  // (reducing the chance of deadlocking with another PG) // RRR rephrase
  auto acting = pg->get_actingset();
  m_sorted_secondaries.reserve(acting.size());
  std::copy_if(
      acting.begin(), acting.end(), std::back_inserter(m_sorted_secondaries),
      [whoami](const pg_shard_t& shard) { return shard != whoami; });

  // sorted by OSD number
  std::sort(
      m_sorted_secondaries.begin(), m_sorted_secondaries.end(),
      [](const pg_shard_t& a, const pg_shard_t& b) { return a.osd < b.osd; });
  m_total_needeed = m_sorted_secondaries.size();

  m_log_msg_prefix = fmt::format(
      "osd.{} ep: {} scrubber::ReplicaReservations pg[{}]: ", m_osds->whoami,
      epoch, pg->pg_id);
  dout(10) << fmt::format("{}: acting: {}", __func__, m_sorted_secondaries)
	   << dendl;

  m_next_to_request = m_sorted_secondaries.begin();
  if (m_next_to_request == m_sorted_secondaries.end()) {
    // A special case of no replicas.
    // just signal the scrub state-machine to continue
    send_all_done();

  } else {
    // send the first reservation requests
    send_a_request(*m_next_to_request, epoch);
  }
}

void ReplicaReservations::send_all_done()
{
  // stop any pending timeout timer
  m_osds->queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::send_reject()
{
  // stop any pending timeout timer
  m_scrub_job->resources_failure = true;
  m_osds->queue_for_scrub_denied(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::release_all(replica_subset_t replicas)
{
  epoch_t epoch = m_pg->get_osdmap_epoch();
  // send 'release' messages to all replicas we have managed to reserve
  for (const auto& p : replicas) {
    release_replica(p, epoch);
  }
}

void ReplicaReservations::discard_all()
{
  dout(10) << fmt::format("{}: reset w/o issuing messages", __func__) << dendl;
  m_requests_sent = 0;
  m_sorted_secondaries.clear();
  m_next_to_request = m_sorted_secondaries.begin();
}

ReplicaReservations::~ReplicaReservations()
{
  auto requested =
      replica_subset_t{m_sorted_secondaries.begin(), m_requests_sent};
  release_all(requested);
}

/**
 * Once the secondary we have messaged has granted the reservation, we send
 * the next request in ascending shard number order.
 *
 *  @ATTN we would not reach here if the ReplicaReservation object managed by
 * the scrubber was reset.
 */
void ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  op->mark_started();

  // verify that the grant is from the peer we expected. If not?
  // for now - abort the OSD. \todo reconsider the reaction.
  if (from != *m_next_to_request) {
    dout(1) << fmt::format(
		   "{}: unexpected grant from {} (expected {})", __func__, from,
		   *m_next_to_request)
	    << dendl;
    ceph_assert(from == *m_next_to_request);
    return;
  }

  auto now_is = clock::now();
  auto elapsed = now_is - m_request_sent_at;
  // \todo: was this response late?
  dout(10) << fmt::format(
		  "{}: granted by {} ({} of {}) in {}ms", __func__,
		  *m_next_to_request, m_requests_sent, m_total_needeed,
		  duration_cast<milliseconds>(elapsed).count())
	   << dendl;

  if (++m_next_to_request == m_sorted_secondaries.end()) {
    // we have received all the reservations we asked for.
    dout(10) << fmt::format(
		    "{}: osd.{} scrub reserve = success", __func__, from)
	     << dendl;
    send_all_done();
  } else {
    // send the next reservation request
    send_a_request(*m_next_to_request, m_pg->get_osdmap_epoch());
  }
}

void ReplicaReservations::handle_reserve_reject(
    OpRequestRef op,
    pg_shard_t from)
{
  op->mark_started();
  dout(10) << fmt::format(
		  "{}: rejected by {} ({})", __func__, from, *op->get_req())
	   << dendl;

  // a convenient log message for the reservation process conclusion
  dout(10) << fmt::format("{}: osd.{} scrub reserve = fail", __func__, from)
	   << dendl;

  // verify that the denial is from the peer we expected. If not?
  // for now - abort the OSD. \todo reconsider the reaction.
  if (from != *m_next_to_request) {
    dout(1) << fmt::format(
		   "{}: unexpected rejection from {} (expected {})", __func__,
		   from, *m_next_to_request)
	    << dendl;
    ceph_assert(from == *m_next_to_request);
    return;
  }

  auto requested =
      replica_subset_t{m_sorted_secondaries.begin(), m_requests_sent - 1};
  release_all(requested);
  send_reject();
}

void ReplicaReservations::handle_no_reply_timeout()
{
  dout(1) << fmt::format(
		 "{}: timeout! no reply from osd.{} (shard {})", __func__,
		 m_next_to_request->osd, m_next_to_request->shard)
	  << dendl;

  auto requested =
      replica_subset_t{m_sorted_secondaries.begin(), m_requests_sent - 1};
  release_all(requested);
  send_reject();
}

std::ostream& ReplicaReservations::gen_prefix(std::ostream& out) const
{
  return out << m_log_msg_prefix;
}
}  // namespace Scrub
