// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/osd/internal_osd_client.h"

#include <tuple>

#include "crimson/common/log.h"
#include "crimson/mon/MonClient.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "include/random.h"
#include "messages/MOSDOp.h"
#include "osd/OSDMap.h"
#include "osd/osd_types_fmt.h"

SET_SUBSYS(osd);

namespace crimson::osd {

// lossy connections are re-established on demand; ops on a connection
// that was reset are resent after this delay, unless a new map triggered
// a resend earlier.
static constexpr auto RESEND_ON_RESET_DELAY = std::chrono::seconds(1);

InternalOSDClient::InternalOSDClient(int whoami, crimson::mon::Client &monc)
  : whoami(whoami), monc(monc)
{
  resend_timer.set_callback([this] { resend_disconnected(); });
}

InternalOSDClient::~InternalOSDClient() = default;

seastar::future<> InternalOSDClient::start()
{
  LOG_PREFIX(InternalOSDClient::start);
  // not bound: needs a unique nonce to be told apart from the OSD's
  // own messengers by the peers (including this very OSD).
  msgr = crimson::net::Messenger::create(
    entity_name_t::OSD(whoami),
    "internal_client",
    ceph::util::generate_random_number<uint64_t>(),
    true);
  msgr->set_default_policy(crimson::net::SocketPolicy::lossy_client(0));
  msgr->set_auth_client(&monc);
  msgr->set_auth_server(&monc);
  INFO("osd.{}", whoami);
  return msgr->start({this});
}

seastar::future<> InternalOSDClient::stop()
{
  LOG_PREFIX(InternalOSDClient::stop);
  INFO("osd.{}: cancelling {} ops", whoami, inflight_ops.size());
  stopping = true;
  resend_timer.cancel();
  while (!inflight_ops.empty()) {
    complete(inflight_ops.begin()->first, tl::unexpected(-ECANCELED));
  }
  if (msgr) {
    msgr->stop();
    co_await msgr->shutdown();
  }
}

bool InternalOSDClient::calc_target(const OSDMap &map, op_t &op) const
{
  pg_t raw_pgid;
  if (map.object_locator_to_pg(op.oid, op.oloc, raw_pgid) < 0) {
    return false;
  }
  auto &t = op.target;
  t.hash = raw_pgid.ps();
  t.pgid = map.raw_pg_to_pg(raw_pgid);
  map.pg_to_up_acting_osds(
    t.pgid, &t.up, &t.up_primary, &t.acting, &t.acting_primary);
  if (spg_t spgid; map.get_primary_shard(t.pgid, &spgid)) {
    t.spgid = spgid;
  } else {
    t.spgid = spg_t(t.pgid);
  }
  return true;
}

void InternalOSDClient::send(op_t &op)
{
  LOG_PREFIX(InternalOSDClient::send);
  const int osd = op.target.acting_primary;
  op.conn = {};
  if (osd < 0 || !osdmap->is_up(osd)) {
    DEBUG("tid {}: no primary for {} in e{}, waiting for a new map",
	  op.tid, op.target.pgid, osdmap->get_epoch());
    return;
  }
  const hobject_t hobj(
    op.oid, op.oloc.key, op.snap, op.target.hash,
    op.oloc.pool, op.oloc.nspace);
  auto m = crimson::make_message<MOSDOp>(
    0, op.tid, hobj, op.target.spgid, osdmap->get_epoch(),
    op.flags | CEPH_OSD_FLAG_READ | CEPH_OSD_FLAG_KNOWN_REDIR |
    CEPH_OSD_FLAG_SUPPORTSPOOLEIO,
    CEPH_FEATURES_SUPPORTED_DEFAULT);
  m->set_snapid(op.snap);
  // keep our copy of the ops: we may need to resend them
  m->ops = op.ops;
  m->set_reqid(osd_reqid_t(entity_name_t::OSD(whoami), 0, op.tid));
  m->set_retry_attempt(op.attempts++);
  DEBUG("tid {}: sending {} to osd.{}, attempt {}",
	op.tid, *m, osd, op.attempts - 1);
  op.conn = msgr->connect(
    osdmap->get_addrs(osd).front(), entity_name_t::OSD(osd));
  std::ignore = op.conn->send(std::move(m)).handle_exception(
    [FNAME, tid = op.tid](auto eptr) {
      ERROR("tid {}: failed to send: {}", tid, eptr);
    });
}

void InternalOSDClient::complete(ceph_tid_t tid, read_result_t &&result)
{
  auto it = inflight_ops.find(tid);
  if (it == inflight_ops.end()) {
    return;
  }
  auto promise = std::move(it->second.promise);
  inflight_ops.erase(it);
  promise.set_value(std::move(result));
}

seastar::future<InternalOSDClient::read_result_t> InternalOSDClient::read(
  ceph_tid_t tid,
  object_t oid,
  object_locator_t oloc,
  snapid_t snap,
  std::vector<OSDOp> ops,
  int flags)
{
  LOG_PREFIX(InternalOSDClient::read);
  if (stopping) {
    return seastar::make_ready_future<read_result_t>(
      tl::unexpected(-ECANCELED));
  }
  ceph_assert(osdmap);
  auto [it, inserted] = inflight_ops.try_emplace(tid);
  ceph_assert(inserted);
  auto &op = it->second;
  op.tid = tid;
  op.oid = std::move(oid);
  op.oloc = std::move(oloc);
  op.snap = snap;
  op.flags = flags;
  op.ops = std::move(ops);
  auto fut = op.promise.get_future();
  if (!calc_target(*osdmap, op)) {
    DEBUG("tid {}: pool {} does not exist", tid, op.oloc.pool);
    complete(tid, tl::unexpected(-ENOENT));
  } else {
    send(op);
  }
  return fut;
}

void InternalOSDClient::cancel(ceph_tid_t tid)
{
  LOG_PREFIX(InternalOSDClient::cancel);
  DEBUG("tid {}", tid);
  complete(tid, tl::unexpected(-ECANCELED));
}

void InternalOSDClient::handle_osd_map(cached_map_t new_osdmap)
{
  LOG_PREFIX(InternalOSDClient::handle_osd_map);
  auto last_map = std::exchange(osdmap, std::move(new_osdmap));
  if (!last_map || inflight_ops.empty()) {
    return;
  }
  for (auto it = inflight_ops.begin(); it != inflight_ops.end(); ) {
    auto &op = (it++)->second;
    const op_target_t prev = op.target;
    if (!calc_target(*osdmap, op)) {
      DEBUG("tid {}: pool {} was deleted", op.tid, op.oloc.pool);
      complete(op.tid, tl::unexpected(-ENOENT));
      continue;
    }
    if (!op.conn ||
        prev.pgid != op.target.pgid ||
        PastIntervals::is_new_interval(
	  prev.acting_primary, op.target.acting_primary,
	  prev.acting, op.target.acting,
	  prev.up_primary, op.target.up_primary,
	  prev.up, op.target.up,
	  osdmap.get(), last_map.get(), prev.pgid)) {
      DEBUG("tid {}: resending, target changed in e{}",
	    op.tid, osdmap->get_epoch());
      send(op);
    }
  }
}

void InternalOSDClient::resend_disconnected()
{
  for (auto &[tid, op] : inflight_ops) {
    if (!op.conn) {
      send(op);
    }
  }
}

void InternalOSDClient::handle_reply(Ref<MOSDOpReply> reply)
{
  LOG_PREFIX(InternalOSDClient::handle_reply);
  const auto tid = reply->get_tid();
  auto it = inflight_ops.find(tid);
  if (it == inflight_ops.end()) {
    DEBUG("tid {}: no such op (cancelled?), dropping {}", tid, *reply);
    return;
  }
  auto &op = it->second;
  if (reply->get_retry_attempt() >= 0 &&
      reply->get_retry_attempt() != op.attempts - 1) {
    DEBUG("tid {}: dropping {}, a reply to attempt {} != {}", tid, *reply,
	  reply->get_retry_attempt(), op.attempts - 1);
    return;
  }
  DEBUG("tid {}: {}", tid, *reply);
  complete(tid, std::move(reply));
}

std::optional<seastar::future<>> InternalOSDClient::ms_dispatch(
  crimson::net::ConnectionRef conn,
  MessageRef m)
{
  LOG_PREFIX(InternalOSDClient::ms_dispatch);
  switch (m->get_type()) {
  case CEPH_MSG_OSD_OPREPLY:
    handle_reply(boost::static_pointer_cast<MOSDOpReply>(std::move(m)));
    return seastar::now();
  case CEPH_MSG_OSD_BACKOFF:
    // crimson OSDs never send backoffs. When a classic one does, the
    // blocked op is resent once the target PG's interval changes.
    INFO("ignoring {} from {}", *m, conn->get_peer_name());
    return seastar::now();
  default:
    return std::nullopt;
  }
}

void InternalOSDClient::ms_handle_reset(
  crimson::net::ConnectionRef conn,
  bool /* is_replace */)
{
  LOG_PREFIX(InternalOSDClient::ms_handle_reset);
  bool any = false;
  for (auto &[tid, op] : inflight_ops) {
    if (op.conn && op.conn.get() == conn.get()) {
      DEBUG("tid {}: connection to {} reset", tid, conn->get_peer_name());
      op.conn = {};
      any = true;
    }
  }
  if (any && !stopping && !resend_timer.armed()) {
    resend_timer.arm(RESEND_ON_RESET_DELAY);
  }
}

} // namespace crimson::osd
