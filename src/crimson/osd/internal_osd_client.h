// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#pragma once

#include <map>
#include <vector>

#include <seastar/core/future.hh>
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/timer.hh>

#include "crimson/common/type_helpers.h"
#include "crimson/net/Dispatcher.h"
#include "crimson/net/Fwd.h"
#include "crimson/osd/osdmap_service.h"
#include "include/expected.hpp"
#include "messages/MOSDOpReply.h"
#include "osd/osd_types.h"

namespace crimson::mon {
class Client;
}

namespace crimson::osd {

/**
 * InternalOSDClient
 *
 * A minimal RADOS client, used by the OSD itself to send read ops to the
 * primary of an arbitrary object - in any pool, any PG, possibly hosted
 * by this very OSD. Currently used for fetching the source object of
 * COPY_FROM (see OpsExecuter::do_copy_from()).
 *
 * The client lives on PRIMARY_CORE only: its messenger authenticates
 * through the OSD's MonClient. Other shards reach it via ShardServices.
 *
 * Compared to the classic Objecter, it only handles reads, with no linger
 * ops, no pool ops, no throttling and no tiering redirects. Ops are
 * resent whenever the target PG's interval changes or the connection to
 * the target resets, as a crimson OSD silently drops ops that were sent
 * to a stale primary.
 */
class InternalOSDClient final : public crimson::net::Dispatcher {
public:
  using cached_map_t = OSDMapService::cached_map_t;

  /// the reply, or:
  /// -ENOENT: the pool does not exist;
  /// -ECANCELED: the op was cancelled, or the OSD is stopping.
  using read_result_t = tl::expected<Ref<MOSDOpReply>, int>;

  InternalOSDClient(int whoami, crimson::mon::Client &monc);
  ~InternalOSDClient() final;

  seastar::future<> start();
  /// fails all pending ops with -ECANCELED
  seastar::future<> stop();

  /// to be called for every new OSDMap epoch, in order
  void handle_osd_map(cached_map_t new_osdmap);

  /**
   * read
   *
   * Sends 'ops' (read ops only) to the primary of 'oid'.
   *
   * @param tid must be unique across all shards (see
   *            ShardServices::get_tid()); it identifies the op for cancel().
   */
  seastar::future<read_result_t> read(
    ceph_tid_t tid,
    object_t oid,
    object_locator_t oloc,
    snapid_t snap,
    std::vector<OSDOp> ops,
    int flags);

  /// completes the op with -ECANCELED, if still pending
  void cancel(ceph_tid_t tid);

  // Dispatcher
  std::optional<seastar::future<>> ms_dispatch(
    crimson::net::ConnectionRef conn,
    MessageRef m) final;
  void ms_handle_reset(
    crimson::net::ConnectionRef conn,
    bool is_replace) final;

private:
  struct op_target_t {
    pg_t pgid;            ///< the actual PG (raw pgid folded by pg_num)
    spg_t spgid;
    uint32_t hash = 0;    ///< raw placement seed of the object
    int up_primary = -1;
    int acting_primary = -1;
    std::vector<int> up;
    std::vector<int> acting;
  };

  struct op_t {
    ceph_tid_t tid;
    object_t oid;
    object_locator_t oloc;
    snapid_t snap;
    int flags;
    std::vector<OSDOp> ops;
    op_target_t target;
    crimson::net::ConnectionRef conn;
    int attempts = 0;
    seastar::promise<read_result_t> promise;
  };

  /// (re)computes op.target. Returns false if the pool does not exist.
  bool calc_target(const OSDMap &osdmap, op_t &op) const;
  /// sends the op to its target primary, if there is one
  void send(op_t &op);
  void complete(ceph_tid_t tid, read_result_t &&result);
  void handle_reply(Ref<MOSDOpReply> reply);
  /// resends the ops whose connection was reset
  void resend_disconnected();

  const int whoami;
  crimson::mon::Client &monc;
  crimson::net::MessengerRef msgr;
  cached_map_t osdmap;
  std::map<ceph_tid_t, op_t> inflight_ops;
  seastar::timer<seastar::lowres_clock> resend_timer;
  bool stopping = false;
};

} // namespace crimson::osd
