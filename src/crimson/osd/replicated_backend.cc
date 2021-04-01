// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "replicated_backend.h"

#include "messages/MOSDRepOpReply.h"

#include "crimson/common/exception.h"
#include "crimson/common/log.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/shard_services.h"
#include "osd/PeeringState.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

ReplicatedBackend::ReplicatedBackend(pg_t pgid,
                                     pg_shard_t whoami,
                                     ReplicatedBackend::CollectionRef coll,
                                     crimson::osd::ShardServices& shard_services)
  : PGBackend{whoami, coll, &shard_services.get_store(), shard_services},
    pgid{pgid},
    whoami{whoami}/*,
    shard_services{shard_services} */
{
  logger().warn("{}: pgid {} whoami {} {:p}", __func__, pgid, whoami, (void*)(&shard_services) );
}

ReplicatedBackend::ll_read_errorator::future<ceph::bufferlist>
ReplicatedBackend::_read(const hobject_t& hoid,
                         const uint64_t off,
                         const uint64_t len,
                         const uint32_t flags)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  return store->read(coll, ghobject_t{hoid}, off, len, flags);
}

seastar::future<crimson::osd::acked_peers_t>
ReplicatedBackend::_submit_transaction(std::set<pg_shard_t>&& pg_shards,
                                       const hobject_t& hoid,
                                       ceph::os::Transaction&& txn,
                                       osd_op_params_t&& osd_op_p,
                                       epoch_t min_epoch, epoch_t map_epoch,
				       std::vector<pg_log_entry_t>&& log_entries)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  if (__builtin_expect((bool)peering, false)) {
    throw crimson::common::actingset_changed(peering->is_primary);
  }

  const ceph_tid_t tid = next_txn_id++;
  auto req_id = osd_op_p.req->get_reqid();
  auto pending_txn =
    pending_trans.try_emplace(tid, pg_shards.size(), osd_op_p.at_version).first;
  bufferlist encoded_txn;
  encode(txn, encoded_txn);

  return seastar::parallel_for_each(std::move(pg_shards),
    [=, encoded_txn=std::move(encoded_txn), txn=std::move(txn)]
    (auto pg_shard) mutable {
      if (pg_shard == whoami) {
        return shard_services.get_store().do_transaction(coll,std::move(txn));
      } else {
        auto m = make_message<MOSDRepOp>(req_id, whoami,
                                         spg_t{pgid, pg_shard.shard}, hoid,
                                         CEPH_OSD_FLAG_ACK | CEPH_OSD_FLAG_ONDISK,
                                         map_epoch, min_epoch,
                                         tid, osd_op_p.at_version);
        m->set_data(encoded_txn);
        pending_txn->second.acked_peers.push_back({pg_shard, eversion_t{}});
	encode(log_entries, m->logbl);
	m->pg_trim_to = osd_op_p.pg_trim_to;
	m->min_last_complete_ondisk = osd_op_p.min_last_complete_ondisk;
	m->set_rollback_to(osd_op_p.at_version);
        // TODO: set more stuff. e.g., pg_states
        return shard_services.send_to_osd(pg_shard.osd, std::move(m), map_epoch);
      }
    }).then([this, peers=pending_txn->second.weak_from_this()] {
      if (!peers) {
	// for now, only actingset_changed can cause peers
	// to be nullptr
	assert(peering);
	throw crimson::common::actingset_changed(peering->is_primary);
      }
      if (--peers->pending == 0) {
        peers->all_committed.set_value();
	peers->all_committed = {};
	return seastar::now();
      }
      return peers->all_committed.get_shared_future();
    }).then([pending_txn, this] {
      auto acked_peers = std::move(pending_txn->second.acked_peers);
      pending_trans.erase(pending_txn);
      return seastar::make_ready_future<crimson::osd::acked_peers_t>(std::move(acked_peers));
    });
}

// for now - assume that called once
// RRR check whether we really need to return the hash
PGBackend::ll_read_errorator::future<ceph::buffer::hash>
ReplicatedBackend::deep_info_on_data(const hobject_t& soid, ScrubMap::object& o) const
{
  constexpr const uint32_t fadvise_flags = CEPH_OSD_OP_FLAG_FADVISE_SEQUENTIAL |
					   CEPH_OSD_OP_FLAG_FADVISE_DONTNEED |
					   CEPH_OSD_OP_FLAG_BYPASS_CLEAN_CACHE;

  // RRR fix to init with -1 only on the error path

  return seastar::do_with(ceph::buffer::hash{bufferhash(-1)}, [this,
							       &soid, &o](auto&& whole_obj) {
    return store
      ->read(coll, ghobject_t{soid, ghobject_t::NO_GEN, whoami.shard}, 0, 0,
	     fadvise_flags)
      .safe_then(
	[/*this,*/ whole_obj, &o, &soid](auto&& bl) mutable
	-> PGBackend::ll_read_errorator::future<ceph::buffer::hash> {
	  whole_obj << bl;
	  o.digest = whole_obj.digest();
	  o.digest_present = true;
	  logger().debug("deep_info_on_data(): {} {}", soid, o.digest);
	  return seastar::make_ready_future<ceph::buffer::hash>(whole_obj);
	},
	PGBackend::ll_read_errorator::all_same_way([&whole_obj, &o, soid]() mutable {
	  logger().error("ReplicatedBackend::deep_info_on_data(): read error on {}", soid);
	  o.read_error = true;
	  return seastar::make_ready_future<ceph::buffer::hash>(whole_obj);
	}));
  });
}

// RRR what is the equivalent of 'allow_eio' here?

// RRR no need to return errorator here?

PGBackend::ll_read_errorator::future<ceph::buffer::hash>
ReplicatedBackend::deep_info_omap_header(const hobject_t& soid, ScrubMap::object& o) const
{
  return seastar::do_with(ceph::buffer::hash{bufferhash(-1)}, [this,
    &soid, &o](auto&& hdr_hash) mutable {
    return store
      ->omap_get_header(coll, ghobject_t{soid, ghobject_t::NO_GEN, whoami.shard})
      .safe_then(
	[/*this,*/ hdr_hash, /*&o,*/ &soid](auto&& bl) mutable
	  -> PGBackend::ll_read_errorator::future<ceph::buffer::hash> {
	  hdr_hash << bl;
	  logger().debug("deep_info_omap_header(): {} ", soid);
	  return seastar::make_ready_future<ceph::buffer::hash>(hdr_hash);
	},
	PGBackend::ll_read_errorator::all_same_way([hdr_hash, o, soid]() mutable {
	  logger().error("ReplicatedBackend::deep_info_omap_header(): read error on {}", soid);
	  o.read_error = true;
	  //return seastar::make_ready_future<ceph::buffer::hash>(hdr_hash);
	  return seastar::make_ready_future<ceph::buffer::hash>(hdr_hash);
	}));
  });
}


PGBackend::ll_read_errorator::future<ceph::buffer::hash>
ReplicatedBackend::deep_info_omap(const hobject_t& soid, ScrubMap::object& o, ceph::buffer::hash accum_hash) const
{
  return seastar::make_ready_future<ceph::buffer::hash>(accum_hash);
#ifdef IN_A_MINUTE
  //return seastar::do_with(ceph::buffer::hash{bufferhash(-1)}, [this,
  //  soid, o](auto&& hdr_hash) mutable {
    return store
      ->omap_get_header(coll, ghobject_t{soid, ghobject_t::NO_GEN, whoami.shard})
      .safe_then(
	[this, hdr_hash, o](auto&& bl) mutable
	  -> PGBackend::ll_read_errorator::future<ceph::buffer::hash> {
	  hdr_hash << bl;
	  return seastar::make_ready_future<ceph::buffer::hash>(hdr_hash);
	},
	PGBackend::ll_read_errorator::all_same_way([hdr_hash, o, soid]() mutable {
	  logger().error("ReplicatedBackend::deep_info_omap_header(): read error on {}", soid);
	  o.read_error = true;
	  //return seastar::make_ready_future<ceph::buffer::hash>(hdr_hash);
	  return seastar::make_ready_future<ceph::buffer::hash>(hdr_hash);
	}));
  //});
#endif
}


// delay-sleep is handled by the caller
ReplicatedBackend::ll_read_errorator::future<> ReplicatedBackend::calc_deep_scrub_info(
  const hobject_t& soid, ScrubMap& map, ScrubMap::object& o) const
{
  logger().debug("{}: on {}", __func__, soid);
  return deep_info_on_data(soid, o)
    .  // not sure we need the ret value

    safe_then([this, &soid, /*&map,*/ &o](auto&& data_hash) mutable
	      -> PGBackend::ll_read_errorator::future<ceph::buffer::hash> {
      return deep_info_omap_header(soid, o).safe_then(
	[this, &soid, /*&map,*/ &o](auto&& hdr_hash) mutable {
	  return deep_info_omap(soid, o, hdr_hash);
	});
    })
    .safe_then([](auto&&) -> seastar::future<> {
      // RRR
      return seastar::make_ready_future<>();
    });
}


void ReplicatedBackend::on_actingset_changed(peering_info_t pi)
{
  peering.emplace(pi);
  crimson::common::actingset_changed e_actingset_changed{peering->is_primary};
  for (auto& [tid, pending_txn] : pending_trans) {
    pending_txn.all_committed.set_exception(e_actingset_changed);
  }
  pending_trans.clear();
}

void ReplicatedBackend::got_rep_op_reply(const MOSDRepOpReply& reply)
{
  auto found = pending_trans.find(reply.get_tid());
  if (found == pending_trans.end()) {
    logger().warn("{}: no matched pending rep op: {}", __func__, reply);
    return;
  }
  auto& peers = found->second;
  for (auto& peer : peers.acked_peers) {
    if (peer.shard == reply.from) {
      peer.last_complete_ondisk = reply.get_last_complete_ondisk();
      if (--peers.pending == 0) {
        peers.all_committed.set_value();
        peers.all_committed = {};
      }
      return;
    }
  }
}

seastar::future<> ReplicatedBackend::stop()
{
  logger().info("ReplicatedBackend::stop {}", coll->get_cid());
  stopping = true;
  for (auto& [tid, pending_on] : pending_trans) {
    pending_on.all_committed.set_exception(
	crimson::common::system_shutdown_exception());
  }
  pending_trans.clear();
  return seastar::now();
}

seastar::future<>
ReplicatedBackend::request_committed(const osd_reqid_t& reqid,
				    const eversion_t& at_version)
{
  if (std::empty(pending_trans)) {
    return seastar::now();
  }
  auto iter = pending_trans.begin();
  auto& pending_txn = iter->second;
  if (pending_txn.at_version > at_version) {
    return seastar::now();
  }
  for (; iter->second.at_version < at_version; ++iter);
  // As for now, the previous client_request with the same reqid
  // mustn't have finished, as that would mean later client_requests
  // has finished before earlier ones.
  //
  // The following line of code should be "assert(pending_txn.at_version == at_version)",
  // as there can be only one transaction at any time in pending_trans due to
  // PG::client_request_pg_pipeline. But there's a high possibility that we will
  // improve the parallelism here in the future, which means there may be multiple
  // client requests in flight, so we loosed the restriction to as follows. Correct
  // me if I'm wrong:-)
  assert(iter != pending_trans.end() && iter->second.at_version == at_version);
  if (iter->second.pending) {
    return iter->second.all_committed.get_shared_future();
  } else {
    return seastar::now();
  }
}
