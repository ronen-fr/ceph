// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <boost/intrusive_ptr.hpp>
#include <seastar/core/future.hh>

#include "msg/MessageRef.h"
#include "crimson/os/cyan_collection.h"

namespace ceph::net {
  class Messenger;
}

namespace ceph::mgr {
  class Client;
}

namespace ceph::mon {
  class Client;
}

namespace ceph::os {
  class FuturizedStore;
}

class PerfCounters;
class OSDMap;
class PeeringCtx;

namespace ceph::osd {

/**
 * Represents services available to each PG
 */
class ShardServices {
  using cached_map_t = boost::local_shared_ptr<const OSDMap>;
  ceph::net::Messenger &cluster_msgr;
  ceph::net::Messenger &public_msgr;
  ceph::mon::Client &monc;
  ceph::mgr::Client &mgrc;
  ceph::os::FuturizedStore &store;

  CephContext cct;

  PerfCounters *perf = nullptr;
  PerfCounters *recoverystate_perf = nullptr;

public:
  ShardServices(
    ceph::net::Messenger &cluster_msgr,
    ceph::net::Messenger &public_msgr,
    ceph::mon::Client &monc,
    ceph::mgr::Client &mgrc,
    ceph::os::FuturizedStore &store);

  seastar::future<> send_to_osd(
    int peer,
    MessageRef m,
    epoch_t from_epoch);

  ceph::os::FuturizedStore &get_store() {
    return store;
  }

  CephContext *get_cct() {
    return &cct;
  }

  // Loggers
  PerfCounters &get_recoverystate_perf_logger() {
    return *recoverystate_perf;
  }
  PerfCounters &get_perf_logger() {
    return *perf;
  }

  /// Dispatch and reset ctx transaction
  seastar::future<> dispatch_context_transaction(
    ceph::os::CollectionRef col, PeeringCtx &ctx);

  /// Dispatch and reset ctx messages
  seastar::future<> dispatch_context_messages(
    PeeringCtx &ctx);

  /// Dispatch ctx and dispose of context
  seastar::future<> dispatch_context(
    ceph::os::CollectionRef col,
    PeeringCtx &&ctx);

  /// Dispatch ctx and dispose of ctx, transaction must be empty
  seastar::future<> dispatch_context(
    PeeringCtx &&ctx) {
    return dispatch_context({}, std::move(ctx));
  }

  // PG Temp State
private:
  // TODO: hook into map processing and some kind of heartbeat/peering
  // message processing
  struct pg_temp_t {
    std::vector<int> acting;
    bool forced = false;
  };
  map<pg_t, pg_temp_t> pg_temp_wanted;
  map<pg_t, pg_temp_t> pg_temp_pending;
  void _sent_pg_temp();
  friend std::ostream& operator<<(std::ostream&, const pg_temp_t&);
public:
  void queue_want_pg_temp(pg_t pgid, const vector<int>& want,
			  bool forced = false);
  void remove_want_pg_temp(pg_t pgid);
  void requeue_pg_temp();
  void send_pg_temp();

  // Shard-local OSDMap
private:
  cached_map_t osdmap;
public:
  void update_map(cached_map_t new_osdmap) {
    osdmap = std::move(new_osdmap);
  }
  cached_map_t &get_osdmap() {
    return osdmap;
  }

  // PG Created State
private:
  set<pg_t> pg_created;
public:
  seastar::future<> send_pg_created(pg_t pgid);
  seastar::future<> send_pg_created();
  void prune_pg_created();
};


}
