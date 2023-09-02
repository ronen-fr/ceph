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
  auto m = new MOSDScrubReserve(spg_t(m_pg_info.pgid.pgid, peer.shard),
				epoch,
				MOSDScrubReserve::RELEASE,
				m_pg->pg_whoami);
  m_osds->send_message_osd_cluster(peer.osd, m, epoch);
}

ReplicaReservations::ReplicaReservations(
  PG* pg,
  pg_shard_t whoami,
  ScrubQueue::ScrubJobRef scrubjob,
  const ConfigProxy& conf)
    : m_pg{pg}
    , m_acting_set{pg->get_actingset()}
    , m_osds{m_pg->get_pg_osd(ScrubberPasskey())}
    , m_pending{static_cast<int>(m_acting_set.size()) - 1}
    , m_pg_info{m_pg->get_pg_info(ScrubberPasskey())}
    , m_scrub_job{scrubjob}
    , m_conf{conf}
{
  epoch_t epoch = m_pg->get_osdmap_epoch();
  m_log_msg_prefix = fmt::format(
      "osd.{} ep: {} scrubber::ReplicaReservations pg[{}]: ", m_osds->whoami,
      epoch, pg->pg_id);

  m_timeout = conf.get_val<std::chrono::milliseconds>(
      "osd_scrub_slow_reservation_response");

  if (m_pending <= 0) {
    // A special case of no replicas.
    // just signal the scrub state-machine to continue
    send_all_done();

  } else {
    // send the reservation requests
    for (auto p : m_acting_set) {
      if (p == whoami)
	continue;
      auto m = new MOSDScrubReserve(
	spg_t(m_pg_info.pgid.pgid, p.shard), epoch, MOSDScrubReserve::REQUEST,
	m_pg->pg_whoami);
      m_osds->send_message_osd_cluster(p.osd, m, epoch);
      m_waited_for_peers.push_back(p);
      dout(10) << __func__ << ": reserve " << p.osd << dendl;
    }
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

void ReplicaReservations::discard_all()
{
  dout(10) << __func__ << ": " << m_reserved_peers << dendl;

  m_had_rejections = true;  // preventing late-coming responses from triggering
			    // events
  m_reserved_peers.clear();
  m_waited_for_peers.clear();
}

/*
 * The following holds when update_latecomers() is called:
 * - we are still waiting for replies from some of the replicas;
 * - we might have already set a timer. If so, we should restart it.
 * - we might have received responses from 50% of the replicas.
 */
std::optional<ReplicaReservations::tpoint_t>
ReplicaReservations::update_latecomers(tpoint_t now_is)
{
  if (m_reserved_peers.size() > m_waited_for_peers.size()) {
    // at least half of the replicas have already responded. Time we flag
    // latecomers.
    return now_is + m_timeout;
  } else {
    return std::nullopt;
  }
}

ReplicaReservations::~ReplicaReservations()
{
  m_had_rejections = true;  // preventing late-coming responses from triggering
			    // events

  // send un-reserve messages to all reserved replicas. We do not wait for
  // answer (there wouldn't be one). Other incoming messages will be discarded
  // on the way, by our owner.
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
void ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << ": granted by " << from << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is
    // cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  // are we forced to reject the reservation?
  if (m_had_rejections) {

    dout(10) << __func__ << ": rejecting late-coming reservation from " << from
	     << dendl;
    release_replica(from, m_pg->get_osdmap_epoch());

  } else if (std::find(m_reserved_peers.begin(),
		       m_reserved_peers.end(),
		       from) != m_reserved_peers.end()) {

    dout(10) << __func__ << ": already had osd." << from << " reserved"
	     << dendl;

  } else {

    dout(10) << __func__ << ": osd." << from << " scrub reserve = success"
	     << dendl;
    m_reserved_peers.push_back(from);

    // was this response late?
    auto now_is = clock::now();
    if (m_timeout_point && (now_is > *m_timeout_point)) {
      m_osds->clog->warn() << fmt::format(
	"osd.{} scrubber pg[{}]: late reservation from osd.{}",
	m_osds->whoami,
	m_pg->pg_id,
	from);
      m_timeout_point.reset();
    } else {
      // possibly set a timer to warn about late-coming reservations
      m_timeout_point = update_latecomers(now_is);
    }

    if (--m_pending == 0) {
      send_all_done();
    }
  }
}

void ReplicaReservations::handle_reserve_reject(OpRequestRef op,
						pg_shard_t from)
{
  dout(10) << __func__ << ": rejected by " << from << dendl;
  dout(15) << __func__ << ": " << *op->get_req() << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is
    // cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  if (m_had_rejections) {

    // our failure was already handled when the first rejection arrived
    dout(15) << __func__ << ": ignoring late-coming rejection from " << from
	     << dendl;

  } else if (std::find(m_reserved_peers.begin(),
		       m_reserved_peers.end(),
		       from) != m_reserved_peers.end()) {

    dout(10) << __func__ << ": already had osd." << from << " reserved"
	     << dendl;

  } else {

    dout(10) << __func__ << ": osd." << from << " scrub reserve = fail"
	     << dendl;
    m_had_rejections = true;  // preventing any additional notifications
    send_reject();
  }
}

void ReplicaReservations::handle_no_reply_timeout()
{
  dout(1) << fmt::format(
	       "{}: timeout! no reply from {}", __func__, m_waited_for_peers)
	  << dendl;

  // treat reply timeout as if a REJECT was received
  m_had_rejections = true;  // preventing any additional notifications
  send_reject();
}

std::ostream& ReplicaReservations::gen_prefix(std::ostream& out) const
{
  return out << m_log_msg_prefix;
}

} // namespace Scrub
