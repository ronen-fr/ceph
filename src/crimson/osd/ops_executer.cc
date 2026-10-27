// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "ops_executer.h"

#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm_ext/push_back.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <boost/range/numeric.hpp>

#include <fmt/format.h>
#include <fmt/ostream.h>

#include <seastar/core/thread.hh>
#include <seastar/util/defer.hh>

#include "common/errno.h"
#include "common/snap_fmt.h"
#include "crimson/common/log.h"
#include "crimson/osd/exceptions.h"
#include "crimson/osd/pg.h"
#include "crimson/osd/watch.h"
#include "osd/ClassHandler.h"
#include "osd/SnapMapper.h"

SET_SUBSYS(osd);

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

namespace crimson::osd {

// workaround for clang 19
// when a .cc file includes ops_executer.h but doesn't include the pg.h,
// it seems that clang++-19 can't retrieve the type hierarchy of PG, so
// that the destructor of boost::intrusive_ptr<PG> could not find the hidden
// friend of intrusive_ptr_release.
// Moving the destructor invocation of intrusive_ptr to this file could
// solve this issue.
OpsExecuter::~OpsExecuter() {}

OpsExecuter::call_ierrorator::future<> OpsExecuter::do_op_call(OSDOp& osd_op)
{
  std::string cname, mname;
  ceph::bufferlist indata;
  try {
    auto bp = std::begin(osd_op.indata);
    bp.copy(osd_op.op.cls.class_len, cname);
    bp.copy(osd_op.op.cls.method_len, mname);
    bp.copy(osd_op.op.cls.indata_len, indata);
  } catch (buffer::error&) {
    logger().warn("call unable to decode class + method + indata");
    return crimson::ct_error::invarg::make();
  }

  // NOTE: opening a class can actually result in dlopen(), and thus
  // blocking the entire reactor. Thankfully to ClassHandler's cache
  // this is supposed to be extremely infrequent.
  ClassHandler::ClassData* cls;
  int r = ClassHandler::get_instance().open_class(cname, &cls);
  if (r) {
    logger().warn("class {} open got {}", cname, cpp_strerror(r));
    if (r == -ENOENT) {
      return crimson::ct_error::operation_not_supported::make();
    } else if (r == -EPERM) {
      // propagate permission errors
      return crimson::ct_error::permission_denied::make();
    }
    return crimson::ct_error::input_output_error::make();
  }

  ClassHandler::ClassMethod* method = cls->get_method(mname);
  if (!method) {
    logger().warn("call method {}.{} does not exist", cname, mname);
    return crimson::ct_error::operation_not_supported::make();
  }

  const auto flags = method->get_flags();

  if (flags & CLS_METHOD_WR) {
    check_init_op_params(modified_by::user);
  }

  logger().debug("calling method {}.{}, num_read={}, num_write={}",
                 cname, mname, num_read, num_write);
  const auto prev_rd = num_read;
  const auto prev_wr = num_write;
  return interruptor::async(
    [this, method, indata=std::move(indata)]() mutable {
      ceph::bufferlist outdata;
      auto cls_context = reinterpret_cast<cls_method_context_t>(this);
      const auto ret = method->exec(cls_context, indata, outdata);
      return std::make_pair(ret, std::move(outdata));
    }
  ).then_interruptible(
    [this, prev_rd, prev_wr, &osd_op, flags]
    (auto outcome) -> call_errorator::future<> {
      auto& [ret, outdata] = outcome;
      osd_op.rval = ret;

      logger().debug("do_op_call: method returned ret={}, outdata.length()={}"
                     " while num_read={}, num_write={}",
                     ret, outdata.length(), num_read, num_write);
      if (num_read > prev_rd && !(flags & CLS_METHOD_RD)) {
        logger().error("method tried to read object but is not marked RD");
        osd_op.rval = -EIO;
        return crimson::ct_error::input_output_error::make();
      }
      if (num_write > prev_wr && !(flags & CLS_METHOD_WR)) {
        logger().error("method tried to update object but is not marked WR");
        osd_op.rval = -EIO;
        return crimson::ct_error::input_output_error::make();
      }
      // ceph-osd has this implemented in `PrimaryLogPG::execute_ctx`,
      // grep for `ignore_out_data`.
      using crimson::common::local_conf;
      if (op_info.allows_returnvec() &&
          op_info.may_write() &&
          ret >= 0 &&
          outdata.length() > local_conf()->osd_max_write_op_reply_len) {
        // the justification of this limit it to not inflate the pg log.
        // that's the reason why we don't worry about pure reads.
        logger().error("outdata overflow due to .length()={}, limit={}",
                       outdata.length(),
                       local_conf()->osd_max_write_op_reply_len);
        osd_op.rval = -EOVERFLOW;
        return crimson::ct_error::value_too_large::make();
      }
      // for write calls we never return data expect errors or RETURNVEC.
      // please refer cls/cls_hello.cc to details.
      if (!op_info.may_write() || op_info.allows_returnvec() || ret < 0) {
        osd_op.op.extent.length = outdata.length();
        osd_op.outdata.claim_append(outdata);
      }
      if (ret < 0) {
        return crimson::stateful_ec{
          std::error_code(-ret, std::generic_category()) };
      } else {
        return seastar::now();
      }
    }
  );
}

static watch_info_t create_watch_info(const OSDOp& osd_op,
                                      const OpsExecuter::ExecutableMessage& msg,
                                      entity_addr_t peer_addr)
{
  using crimson::common::local_conf;
  const uint32_t timeout =
    osd_op.op.watch.timeout == 0 ? local_conf()->osd_client_watch_timeout
                                 : osd_op.op.watch.timeout;
  return {
    osd_op.op.watch.cookie,
    timeout,
    peer_addr
  };
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch_subop_watch(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  logger().debug("{}", __func__);
  struct connect_ctx_t {
    ObjectContext::watch_key_t key;
    crimson::net::ConnectionXcoreRef conn;
    watch_info_t info;

    connect_ctx_t(
      const OSDOp& osd_op,
      const ExecutableMessage& msg,
      crimson::net::ConnectionXcoreRef conn)
      : key(osd_op.op.watch.cookie, msg.get_reqid().name),
        conn(conn),
        info(create_watch_info(osd_op, msg, conn->get_peer_addr())) {
    }
  };

  return with_effect_on_obc(
    connect_ctx_t{ osd_op, get_message(), conn },
    [&](auto& ctx) {
      const auto& entity = ctx.key.second;
      auto [it, emplaced] =
        os.oi.watchers.try_emplace(ctx.key, std::move(ctx.info));
      if (emplaced) {
        logger().info("registered new watch {} by {}", it->second, entity);
        txn.nop();
      } else {
        logger().info("found existing watch {} by {}", it->second, entity);
      }
      return seastar::now();
    },
    [](auto&& ctx, ObjectContextRef obc, Ref<PG> pg) {
      assert(pg);
      auto [it, emplaced] = obc->watchers.try_emplace(ctx.key, nullptr);
      if (emplaced) {
        const auto& [cookie, entity] = ctx.key;
        it->second = crimson::osd::Watch::create(
          obc, ctx.info, entity, std::move(pg));
        logger().info("op_effect: added new watcher: {}", ctx.key);
      } else {
        logger().info("op_effect: found existing watcher: {}", ctx.key);
      }
      return it->second->connect(std::move(ctx.conn), true /* will_ping */);
    }
  );
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch_subop_reconnect(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  const entity_name_t& entity = get_message().get_reqid().name;
  const auto& cookie = osd_op.op.watch.cookie;
  if (!os.oi.watchers.count(std::make_pair(cookie, entity))) {
    return crimson::ct_error::not_connected::make();
  } else {
    logger().info("found existing watch by {}", entity);
    return do_op_watch_subop_watch(osd_op, os, txn);
  }
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch_subop_unwatch(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  logger().info("{}", __func__);

  struct disconnect_ctx_t {
    ObjectContext::watch_key_t key;
    disconnect_ctx_t(const OSDOp& osd_op, const ExecutableMessage& msg)
      : key(osd_op.op.watch.cookie, msg.get_reqid().name) {
    }
  };
  return with_effect_on_obc(disconnect_ctx_t{ osd_op, get_message() },
    [&] (auto& ctx) {
      const auto& entity = ctx.key.second;
      if (auto nh = os.oi.watchers.extract(ctx.key); !nh.empty()) {
        logger().info("removed watch {} by {}", nh.mapped(), entity);
        txn.nop();
      } else {
        logger().info("can't remove: no watch by {}", entity);
      }
      return seastar::now();
    },
    [] (auto&& ctx, ObjectContextRef obc, Ref<PG>) {
      if (auto nh = obc->watchers.extract(ctx.key); !nh.empty()) {
        return seastar::do_with(std::move(nh.mapped()),
                         [ctx](auto&& watcher) {
          logger().info("op_effect: disconnect watcher {}", ctx.key);
          return watcher->remove();
        });
      } else {
        logger().info("op_effect: disconnect failed to find watcher {}", ctx.key);
        return seastar::now();
      }
    });
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch_subop_ping(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  const entity_name_t& entity = get_message().get_reqid().name;
  const auto& cookie = osd_op.op.watch.cookie;
  const auto key = std::make_pair(cookie, entity);

  // Note: WATCH with PING doesn't cause may_write() to return true,
  // so if there is nothing else in the transaction, this is going
  // to run do_osd_op_effects, but not write out a log entry */
  if (!os.oi.watchers.count(key)) {
    return crimson::ct_error::not_connected::make();
  }
  auto it = obc->watchers.find(key);
  if (it == std::end(obc->watchers) || !it->second->is_connected()) {
    return crimson::ct_error::timed_out::make();
  }
  logger().info("found existing watch by {}", entity);
  it->second->got_ping(ceph_clock_now());
  return seastar::now();
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_watch(
  OSDOp& osd_op,
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  logger().debug("{}", __func__);
  if (!os.exists) {
    return crimson::ct_error::enoent::make();
  }
  switch (osd_op.op.watch.op) {
    case CEPH_OSD_WATCH_OP_WATCH:
      return do_op_watch_subop_watch(osd_op, os, txn);
    case CEPH_OSD_WATCH_OP_RECONNECT:
      return do_op_watch_subop_reconnect(osd_op, os, txn);
    case CEPH_OSD_WATCH_OP_PING:
      return do_op_watch_subop_ping(osd_op, os, txn);
    case CEPH_OSD_WATCH_OP_UNWATCH:
      return do_op_watch_subop_unwatch(osd_op, os, txn);
    case CEPH_OSD_WATCH_OP_LEGACY_WATCH:
      logger().warn("ignoring CEPH_OSD_WATCH_OP_LEGACY_WATCH");
      return crimson::ct_error::invarg::make();
  }
  logger().warn("unrecognized WATCH subop: {}", osd_op.op.watch.op);
  return crimson::ct_error::invarg::make();
}

static uint64_t get_next_notify_id(epoch_t e)
{
  // FIXME
  static std::uint64_t next_notify_id = 0;
  return (((uint64_t)e) << 32) | ((uint64_t)(next_notify_id++));
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_notify(
  OSDOp& osd_op,
  const ObjectState& os)
{
  logger().debug("{}, msg epoch: {}", __func__, get_message().get_map_epoch());

  if (!os.exists) {
    return crimson::ct_error::enoent::make();
  }
  struct notify_ctx_t {
    crimson::net::ConnectionXcoreRef conn;
    notify_info_t ninfo;
    const uint64_t client_gid;
    const epoch_t epoch;

    notify_ctx_t(const ExecutableMessage& msg,
                 crimson::net::ConnectionXcoreRef conn)
      : conn(conn),
        client_gid(msg.get_reqid().name.num()),
        epoch(msg.get_map_epoch()) {
    }
  };
  return with_effect_on_obc(
    notify_ctx_t{ get_message(), conn },
    [&](auto& ctx) {
      try {
        auto bp = osd_op.indata.cbegin();
        uint32_t ver; // obsolete
        ceph::decode(ver, bp);
        ceph::decode(ctx.ninfo.timeout, bp);
        ceph::decode(ctx.ninfo.bl, bp);
      } catch (const buffer::error&) {
        ctx.ninfo.timeout = 0;
      }
      if (!ctx.ninfo.timeout) {
        using crimson::common::local_conf;
        ctx.ninfo.timeout = local_conf()->osd_default_notify_timeout;
      }
      ctx.ninfo.notify_id = get_next_notify_id(ctx.epoch);
      ctx.ninfo.cookie = osd_op.op.notify.cookie;
      // return our unique notify id to the client
      ceph::encode(ctx.ninfo.notify_id, osd_op.outdata);
      return seastar::now();
    },
    [](auto&& ctx, ObjectContextRef obc, Ref<PG>) {
      auto alive_watchers = obc->watchers | boost::adaptors::map_values
        | boost::adaptors::filtered(
          [] (const auto& w) {
            // FIXME: filter as for the `is_ping` in `Watch::start_notify`
            return w->is_alive();
          });
      return crimson::osd::Notify::create_n_propagate(
        std::begin(alive_watchers),
        std::end(alive_watchers),
        std::move(ctx.conn),
        ctx.ninfo,
        ctx.client_gid,
        obc->obs.oi.user_version);
    }
  );
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_list_watchers(
  OSDOp& osd_op,
  const ObjectState& os)
{
  logger().debug("{}", __func__);

  obj_list_watch_response_t response;
  for (const auto& [key, info] : os.oi.watchers) {
    logger().debug("{}: key cookie={}, entity={}",
                   __func__, key.first, key.second);
    assert(key.first == info.cookie);
    assert(key.second.is_client());
    response.entries.emplace_back(watch_item_t{
      key.second, info.cookie, info.timeout_seconds, info.addr});
  }
  response.encode(osd_op.outdata, get_message().get_features());
  return watch_ierrorator::now();
}

OpsExecuter::watch_ierrorator::future<> OpsExecuter::do_op_notify_ack(
  OSDOp& osd_op,
  const ObjectState& os)
{
  logger().debug("{}", __func__);

  struct notifyack_ctx_t {
    const entity_name_t entity;
    uint64_t watch_cookie;
    uint64_t notify_id;
    ceph::bufferlist reply_bl;

    notifyack_ctx_t(const ExecutableMessage& msg)
      : entity(msg.get_reqid().name) {
    }
  };
  return with_effect_on_obc(notifyack_ctx_t{ get_message() },
    [&] (auto& ctx) -> watch_errorator::future<> {
      try {
        auto bp = osd_op.indata.cbegin();
        ceph::decode(ctx.notify_id, bp);
        ceph::decode(ctx.watch_cookie, bp);
        if (!bp.end()) {
          ceph::decode(ctx.reply_bl, bp);
        }
      } catch (const buffer::error&) {
        // here we behave differently than ceph-osd. For historical reasons,
        // it falls back to using `osd_op.op.watch.cookie` as `ctx.notify_id`.
        // crimson just returns EINVAL if the data cannot be decoded.
        return crimson::ct_error::invarg::make();
      }
      return watch_errorator::now();
    },
    [] (auto&& ctx, ObjectContextRef obc, Ref<PG>) {
      logger().info("notify_ack watch_cookie={}, notify_id={}",
                    ctx.watch_cookie, ctx.notify_id);
      return seastar::do_for_each(obc->watchers,
        [ctx=std::move(ctx)] (auto& kv) {
          const auto& [key, watchp] = kv;
          static_assert(
            std::is_same_v<std::decay_t<decltype(watchp)>,
                           seastar::shared_ptr<crimson::osd::Watch>>);
          auto& [cookie, entity] = key;
          if (ctx.entity != entity) {
            logger().debug("skipping watch {}; entity name {} != {}",
                           key, entity, ctx.entity);
            return seastar::now();
          }
          if (ctx.watch_cookie != cookie) {
            logger().debug("skipping watch {}; cookie {} != {}",
                           key, ctx.watch_cookie, cookie);
            return seastar::now();
          }
          logger().info("acking notify on watch {}", key);
          return watchp->notify_ack(ctx.notify_id, ctx.reply_bl);
        });
  });
}

// Defined here because there is a circular dependency between OpsExecuter and PG
template <class Func>
auto OpsExecuter::do_const_op(Func&& f) {
  // TODO: pass backend as read-only
  return std::forward<Func>(f)(pg->get_backend(), std::as_const(obc->obs));
}

template <class Func>
auto OpsExecuter::do_read_attr_cache(Func&& f) {
  ++num_read;
  // TODO: pass backend as read-only
  return std::invoke(
    std::forward<Func>(f),
    pg->get_backend(),
    std::as_const(obc->attr_cache),
    std::as_const(obc->obs));
}

// Defined here because there is a circular dependency between OpsExecuter and PG
template <class Func>
auto OpsExecuter::do_write_op(Func&& f, OpsExecuter::modified_by m) {
  ++num_write;
  check_init_op_params(m);
  return std::forward<Func>(f)(pg->get_backend(), obc->obs, txn);
}
template <class Func>
auto OpsExecuter::do_write_op_attr_cache(Func&& f, OpsExecuter::modified_by m) {
  ++num_write;
  check_init_op_params(m);
  return std::forward<Func>(f)(pg->get_backend(), obc->obs, txn, obc->attr_cache);
}
OpsExecuter::call_errorator::future<> OpsExecuter::do_assert_ver(
  OSDOp& osd_op,
  const ObjectState& os)
{
  if (!osd_op.op.assert_ver.ver) {
    return crimson::ct_error::invarg::make();
  } else if (osd_op.op.assert_ver.ver < os.oi.user_version) {
    return crimson::ct_error::erange::make();
  } else if (osd_op.op.assert_ver.ver > os.oi.user_version) {
    return crimson::ct_error::value_too_large::make();
  }
  return seastar::now();
}

OpsExecuter::list_snaps_iertr::future<> OpsExecuter::do_list_snaps(
  OSDOp& osd_op,
  const ObjectState& os,
  const SnapSet& ss)
{
  if (msg->get_snapid() != CEPH_SNAPDIR) {
    logger().debug("LIST_SNAPS with incorrect context");
    return crimson::ct_error::invarg::make();
  }

  obj_list_snap_response_t resp;
  resp.clones.reserve(ss.clones.size() + 1);
  for (auto &clone: ss.clones) {
    clone_info ci;
    ci.cloneid = clone;

    {
      auto p = ss.clone_snaps.find(clone);
      if (p == ss.clone_snaps.end()) {
	logger().error(
	  "OpsExecuter::do_list_snaps: {} has inconsistent "
	  "clone_snaps, missing clone {}",
	  os.oi.soid,
	  clone);
	return crimson::ct_error::invarg::make();
      }
      ci.snaps.reserve(p->second.size());
      ci.snaps.insert(ci.snaps.end(), p->second.rbegin(), p->second.rend());
    }

    {
      auto p = ss.clone_overlap.find(clone);
      if (p == ss.clone_overlap.end()) {
	logger().error(
	  "OpsExecuter::do_list_snaps: {} has inconsistent "
	  "clone_overlap, missing clone {}",
	  os.oi.soid,
	  clone);
	return crimson::ct_error::invarg::make();
      }
      ci.overlap.reserve(p->second.num_intervals());
      ci.overlap.insert(ci.overlap.end(), p->second.begin(), p->second.end());
    }

    {
      auto p = ss.clone_size.find(clone);
      if (p == ss.clone_size.end()) {
	logger().error(
	  "OpsExecuter::do_list_snaps: {} has inconsistent "
	  "clone_size, missing clone {}",
	  os.oi.soid,
	  clone);
	return crimson::ct_error::invarg::make();
      }
      ci.size = p->second;
    }
    resp.clones.push_back(std::move(ci));
  }

  if (!os.oi.is_whiteout()) {
    clone_info ci;
    ci.cloneid = CEPH_NOSNAP;
    ci.size = os.oi.size;
    resp.clones.push_back(std::move(ci));
  }
  resp.seq = ss.seq;
  logger().error(
    "OpsExecuter::do_list_snaps: {}, resp.clones.size(): {}",
    os.oi.soid,
    resp.clones.size());
  resp.encode(osd_op.outdata);
  return read_ierrorator::now();
}

OpsExecuter::copy_get_iertr::future<> OpsExecuter::do_copy_get(
  OSDOp& osd_op,
  const ObjectState& os)
{
  LOG_PREFIX(OpsExecuter::do_copy_get);
  object_copy_cursor_t cursor;
  uint64_t out_max = 0;
  bool decoded = true;
  try {
    auto p = osd_op.indata.cbegin();
    decode(cursor, p);
    decode(out_max, p);
  } catch (const ceph::buffer::error&) {
    decoded = false;
  }
  if (!decoded) {
    co_await copy_get_iertr::future<>(crimson::ct_error::invarg::make());
  }

  const auto& oi = os.oi;
  const uint64_t features = msg->get_features();
  object_copy_data_t reply;
  if (!os.exists || oi.is_whiteout()) {
    // the requester still needs the reqids, so that a resent
    // copy-from (whose source was meanwhile removed) can be detected
    // as a dup on the destination.
    pg->get_log().get_log().get_object_reqids(
      oi.soid, 10, &reply.reqids, &reply.reqid_return_codes);
    encode(reply, osd_op.outdata, features);
    co_await copy_get_iertr::future<>(crimson::ct_error::enoent::make());
  }

  reply.size = oi.size;
  reply.mtime = oi.mtime;
  ceph_assert(obc->ssc);
  if (oi.soid.is_snap()) {
    const auto& clone_snaps = obc->ssc->snapset.clone_snaps;
    auto p = clone_snaps.find(oi.soid.snap);
    if (p == clone_snaps.end()) {
      ERRORDPP("{} missing from clone_snaps", *pg, oi.soid);
      co_await copy_get_iertr::future<>(
	crimson::ct_error::object_corrupted::make());
    }
    reply.snaps = p->second;
  } else {
    reply.snap_seq = obc->ssc->snapset.seq;
  }
  if (oi.is_data_digest()) {
    reply.flags |= object_copy_data_t::FLAG_DATA_DIGEST;
    reply.data_digest = oi.data_digest;
  }
  if (oi.is_omap_digest()) {
    reply.flags |= object_copy_data_t::FLAG_OMAP_DIGEST;
    reply.omap_digest = oi.omap_digest;
  }
  reply.truncate_seq = oi.truncate_seq;
  reply.truncate_size = oi.truncate_size;

  co_await pg->get_backend().fill_in_copy_get(
    os, obc->attr_cache, pg->get_pgpool().info.supports_omap(),
    out_max, osd_op.op.flags, cursor, reply);

  if (cursor.is_complete()) {
    // reqids are only included in the final chunk
    pg->get_log().get_log().get_object_reqids(
      oi.soid, 10, &reply.reqids, &reply.reqid_return_codes);
  }
  DEBUGDPP("{}: complete={} {} reqids", *pg, oi.soid,
	   cursor.is_complete(), reply.reqids.size());
  reply.cursor = cursor;
  encode(reply, osd_op.outdata, features);
}

OpsExecuter::copy_from_iertr::future<OpsExecuter::copy_source_t>
OpsExecuter::fetch_copy_source(
  const object_t& src_oid,
  const object_locator_t& src_oloc,
  const ceph_osd_op& op)
{
  LOG_PREFIX(OpsExecuter::fetch_copy_source);
  using crimson::common::local_conf;
  auto& shard_services = pg->get_shard_services();
  const uint64_t chunk_size = local_conf()->osd_copyfrom_max_chunk;
  // the whole source object is held in memory until it is written
  const uint64_t max_size = local_conf().get_val<Option::size_t>(
    "crimson_osd_copyfrom_max_inmem_bytes");
  int read_flags = 0;
  if (op.copy_from.flags & CEPH_OSD_COPY_FROM_FLAG_RWORDERED) {
    read_flags |= CEPH_OSD_FLAG_RWORDERED;
  }

  copy_source_t src;
  // All chunks but the first are fetched along with an ASSERT_VER
  // on the source's user_version, which makes the copy atomic.
  src.user_version = op.copy_from.src_version;
  object_copy_cursor_t cursor;
  uint32_t src_flags = 0;
  uint32_t src_data_digest = 0;
  uint32_t src_omap_digest = 0;
  uint32_t data_digest = -1;
  uint32_t omap_digest = -1;
  uint64_t fetched_bytes = 0;
  do {
    const bool first = cursor.is_initial();
    std::vector<OSDOp> ops;
    if (src.user_version) {
      auto& assert_ver = ops.emplace_back();
      assert_ver.op.op = CEPH_OSD_OP_ASSERT_VER;
      assert_ver.op.assert_ver.ver = src.user_version;
    }
    auto& copy_get = ops.emplace_back();
    copy_get.op.op = CEPH_OSD_OP_COPY_GET;
    copy_get.op.copy_get.max = chunk_size;
    copy_get.op.flags = op.copy_from.src_fadvise_flags;
    encode(cursor, copy_get.indata);
    encode(chunk_size, copy_get.indata);

    const ceph_tid_t tid = shard_services.get_tid();
    pg->internal_client_reads.insert(tid);
    auto unregister = seastar::defer([this, tid] {
      pg->internal_client_reads.erase(tid);
    });
    auto result = co_await interruptor::make_interruptible(
      shard_services.internal_client_read(
	tid, src_oid, src_oloc, snapid_t(op.copy_from.snapid),
	std::move(ops), read_flags));
    if (const int r = result ? (*result)->get_result() : result.error();
	r < 0) {
      DEBUGDPP("{}: fetching {} failed: {}",
	       *pg, get_target(), src_oid.name, cpp_strerror(r));
      co_await copy_from_iertr::future<>(
	crimson::stateful_ec{std::error_code(-r, std::generic_category())});
    }

    auto reply = std::move(*result);
    std::vector<OSDOp> reply_ops;
    reply->claim_ops(reply_ops);
    object_copy_data_t chunk;
    std::map<std::string, ceph::bufferlist> chunk_omap;
    bool decoded = false;
    try {
      if (!reply_ops.empty()) {
	auto p = reply_ops.back().outdata.cbegin();
	decode(chunk, p);
	if (chunk.omap_data.length()) {
	  auto q = chunk.omap_data.cbegin();
	  decode(chunk_omap, q);
	}
	decoded = true;
      }
    } catch (const ceph::buffer::error&) {
    }
    if (!decoded) {
      ERRORDPP("{}: failed to decode COPY_GET reply for {}",
	       *pg, get_target(), src_oid.name);
      co_await copy_from_iertr::future<>(
	crimson::ct_error::input_output_error::make());
    }

    if (first) {
      if (!src.user_version) {
	src.user_version = reply->get_user_version();
      }
      src.obj.size = chunk.size;
      src.obj.mtime = chunk.mtime;
      src.obj.truncate_seq = chunk.truncate_seq;
      src.obj.truncate_size = chunk.truncate_size;
      src_flags = chunk.flags;
      src_data_digest = chunk.data_digest;
      src_omap_digest = chunk.omap_digest;
      for (auto& [key, val] : chunk.attrs) {
	src.obj.attrs["_" + key] = std::move(val);
      }
      if (chunk.size > max_size) {
	INFODPP("{}: source {} is too large ({} > {})",
		*pg, get_target(), src_oid.name, chunk.size, max_size);
	co_await copy_from_iertr::future<>(
	  crimson::ct_error::file_too_large::make());
      }
    }
    if (!cursor.data_complete) {
      data_digest = chunk.data.crc32c(data_digest);
      fetched_bytes += chunk.data.length();
      src.obj.data.claim_append(chunk.data);
    }
    if (!cursor.omap_complete) {
      if (chunk.omap_header.length()) {
	src.obj.has_omap = true;
	omap_digest = chunk.omap_header.crc32c(omap_digest);
	fetched_bytes += chunk.omap_header.length();
	src.obj.omap_header.claim_append(chunk.omap_header);
      }
      if (chunk.omap_data.length()) {
	src.obj.has_omap = true;
	// the digest does not cover the encoded count of keys
	ceph::bufferlist keys;
	keys.substr_of(chunk.omap_data, 4, chunk.omap_data.length() - 4);
	omap_digest = keys.crc32c(omap_digest);
	fetched_bytes += chunk.omap_data.length();
	src.obj.omap.merge(chunk_omap);
      }
    }
    if (fetched_bytes > max_size) {
      INFODPP("{}: source {} is too large (more than {} bytes)",
	      *pg, get_target(), src_oid.name, max_size);
      co_await copy_from_iertr::future<>(
	crimson::ct_error::file_too_large::make());
    }
    cursor = chunk.cursor;
    if (cursor.is_complete()) {
      src.reqids = std::move(chunk.reqids);
      src.reqid_return_codes = std::move(chunk.reqid_return_codes);
    } else if (!src.user_version) {
      ERRORDPP("{}: source {} has no user_version", *pg, get_target(), src_oid.name);
      co_await copy_from_iertr::future<>(
	crimson::ct_error::input_output_error::make());
    }
  } while (!cursor.is_complete());

  if (src.obj.has_omap && !pg->get_pgpool().info.supports_omap()) {
    co_await copy_from_iertr::future<>(
      crimson::ct_error::operation_not_supported::make());
  }
  if (src.obj.data.length() != src.obj.size) {
    ERRORDPP("{}: source {} size {} != {} bytes copied",
	     *pg, get_target(), src_oid.name, src.obj.size, src.obj.data.length());
    co_await copy_from_iertr::future<>(
      crimson::ct_error::input_output_error::make());
  }
  if (src_flags & object_copy_data_t::FLAG_DATA_DIGEST) {
    if (data_digest != src_data_digest) {
      ERRORDPP("{}: data digest {:#x} != source {} data digest {:#x}",
	       *pg, get_target(), data_digest, src_oid.name, src_data_digest);
      pg->get_clog_error() << pg->get_pgid() << " copy from " << src_oid
			   << " to " << get_target() << " data digest 0x"
			   << std::hex << data_digest << " != source 0x"
			   << src_data_digest << std::dec;
      co_await copy_from_iertr::future<>(
	crimson::ct_error::input_output_error::make());
    }
    src.obj.data_digest = data_digest;
  }
  if (src_flags & object_copy_data_t::FLAG_OMAP_DIGEST) {
    if (omap_digest != src_omap_digest) {
      ERRORDPP("{}: omap digest {:#x} != source {} omap digest {:#x}",
	       *pg, get_target(), omap_digest, src_oid.name, src_omap_digest);
      pg->get_clog_error() << pg->get_pgid() << " copy from " << src_oid
			   << " to " << get_target() << " omap digest 0x"
			   << std::hex << omap_digest << " != source 0x"
			   << src_omap_digest << std::dec;
      co_await copy_from_iertr::future<>(
	crimson::ct_error::input_output_error::make());
    }
    src.obj.omap_digest = omap_digest;
  }
  co_return src;
}

OpsExecuter::copy_from_iertr::future<> OpsExecuter::do_copy_from(
  OSDOp& osd_op)
{
  LOG_PREFIX(OpsExecuter::do_copy_from);
  const ceph_osd_op& op = osd_op.op;
  const auto flags = op.copy_from.flags;
  object_t src_oid;
  object_locator_t src_oloc;
  std::optional<std::pair<uint32_t, uint64_t>> truncate;
  bool decoded = true;
  try {
    auto p = osd_op.indata.cbegin();
    decode(src_oid, p);
    decode(src_oloc, p);
    // COPY_FROM2 may override the truncate_seq/size of the source
    if (op.op == CEPH_OSD_OP_COPY_FROM2 &&
	(flags & CEPH_OSD_COPY_FROM_FLAG_TRUNCATE_SEQ)) {
      uint32_t truncate_seq;
      uint64_t truncate_size;
      decode(truncate_seq, p);
      decode(truncate_size, p);
      truncate.emplace(truncate_seq, truncate_size);
    }
  } catch (const ceph::buffer::error&) {
    decoded = false;
  }
  if (!decoded) {
    co_await copy_from_iertr::future<>(crimson::ct_error::invarg::make());
  }
  if (op.op == CEPH_OSD_OP_COPY_FROM2 && (flags & ~CEPH_OSD_COPY_FROM_FLAGS)) {
    DEBUGDPP("{}: unknown flags {:#x}", *pg, get_target(), flags);
    co_await copy_from_iertr::future<>(crimson::ct_error::invarg::make());
  }
  if (flags & (CEPH_OSD_COPY_FROM_FLAG_FLUSH |
	       CEPH_OSD_COPY_FROM_FLAG_MAP_SNAP_CLONE)) {
    // cache-tiering only
    DEBUGDPP("{}: unsupported flags {:#x}", *pg, get_target(), flags);
    co_await copy_from_iertr::future<>(
      crimson::ct_error::operation_not_supported::make());
  }
  if (pg->get_pgpool().info.is_erasure()) {
    // TODO: support EC destinations
    DEBUGDPP("{}: EC destination", *pg, get_target());
    co_await copy_from_iertr::future<>(
      crimson::ct_error::operation_not_supported::make());
  }
  pg_t src_raw_pg;
  if (pg->get_osdmap()->object_locator_to_pg(
	src_oid, src_oloc, src_raw_pg) < 0) {
    co_await copy_from_iertr::future<>(crimson::ct_error::enoent::make());
  }
  if (const hobject_t src(src_oid, src_oloc.key,
			  snapid_t(op.copy_from.snapid),
			  src_raw_pg.ps(), src_raw_pg.pool(), src_oloc.nspace);
      src == get_target()) {
    DEBUGDPP("{}: copy from self", *pg, get_target());
    co_await copy_from_iertr::future<>(crimson::ct_error::invarg::make());
  }

  ++num_write;
  // the user_version is set explicitly, to that of the source object
  check_init_op_params(modified_by::sys);
  // Note: the target object stays write-locked while the source is
  // fetched. Two concurrent copies, each from the target of the other,
  // would block each other until the next interval change (as in the
  // classic OSD).
  auto src = co_await fetch_copy_source(src_oid, src_oloc, op);
  if (truncate) {
    std::tie(src.obj.truncate_seq, src.obj.truncate_size) = *truncate;
  }
  DEBUGDPP("{}: copying {} ({} bytes, {} omap keys, {} xattrs) uv {}",
	   *pg, get_target(), src_oid.name, src.obj.size, src.obj.omap.size(),
	   src.obj.attrs.size(), src.user_version);
  pg->get_backend().copy_from(
    obc->obs, src.obj, op.flags, txn, *osd_op_params, delta_stats);
  osd_op_params->user_version_override = src.user_version;
  osd_op_params->mtime = src.obj.mtime;
  osd_op_params->extra_reqids = std::move(src.reqids);
  osd_op_params->extra_reqid_return_codes = std::move(src.reqid_return_codes);
}

OpsExecuter::interruptible_errorated_future<OpsExecuter::osd_op_errorator>
OpsExecuter::execute_op(OSDOp& osd_op)
{
  return do_execute_op(osd_op).handle_error_interruptible(
    osd_op_errorator::all_same_way([&osd_op](auto e, auto&& e_raw)
      -> OpsExecuter::osd_op_errorator::future<> {
        // All ops except for CMPEXT should have rval set to -e.value(),
        // CMPEXT sets rval itself and shouldn't be overridden.
        if (e.value() != ct_error::cmp_fail_error_value) {
          osd_op.rval = -e.value();
        }
        if ((osd_op.op.flags & CEPH_OSD_OP_FLAG_FAILOK) &&
	  e.value() != EAGAIN && e.value() != EINPROGRESS) {
          return osd_op_errorator::now();
        } else {
          return std::move(e_raw);
	}
      }));
}

OpsExecuter::interruptible_errorated_future<OpsExecuter::osd_op_errorator>
OpsExecuter::do_execute_op(OSDOp& osd_op)
{
  // TODO: dispatch via call table?
  // TODO: we might want to find a way to unify both input and output
  // of each op.
  logger().debug(
    "handling op {} on object {}",
    ceph_osd_op_name(osd_op.op.op),
    get_target());
  // Best-effort capacity gate (failsafe): drop data-allocating ops before a
  // transaction is created if the local store is full. This is Crimson's
  // analog to classic's osd_failsafe_full_ratio check (PrimaryLogPG.cc:2162),
  // preventing allocator exhaustion assert aborts when the local store fills
  // before the monitor's pool FLAG_FULL has propagated. We reply -EAGAIN so
  // the client resends, matching both classic's silent drop and Crimson's own
  // mon-driven full path (PG::run_executer_fut, pg.cc), rather than returning
  // a result that depends on whether full flags have reached the client yet.
  switch (osd_op.op.op) {
  case CEPH_OSD_OP_CREATE:
  case CEPH_OSD_OP_WRITE:
  case CEPH_OSD_OP_WRITEFULL:
  case CEPH_OSD_OP_WRITESAME:
  case CEPH_OSD_OP_APPEND:
  case CEPH_OSD_OP_COPY_FROM:
  case CEPH_OSD_OP_COPY_FROM2:
    if (pg->is_local_store_full() && !get_message().has_flag(CEPH_OSD_FLAG_FULL_TRY)) {
      return crimson::ct_error::eagain::make();
    }
    break;
  default:
    break;
  }
  switch (const ceph_osd_op& op = osd_op.op; op.op) {
  case CEPH_OSD_OP_SYNC_READ:
    [[fallthrough]];
  case CEPH_OSD_OP_READ:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.read(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_SPARSE_READ:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.sparse_read(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_CHECKSUM:
    return do_read_op([&osd_op](auto& backend, const auto& os) {
      return backend.checksum(os, osd_op);
    });
  case CEPH_OSD_OP_CMPEXT:
    return do_read_op([&osd_op](auto& backend, const auto& os) {
      return backend.cmp_ext(os, osd_op);
    });
  case CEPH_OSD_OP_GETXATTR:
    return do_read_attr_cache([this, &osd_op](auto& backend,
					      const auto& attr_cache,
					      const auto& os) {
      return backend.getxattr(os, attr_cache, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_GETXATTRS:
    return do_read_attr_cache([this, &osd_op](auto& backend,
					      const auto& attr_cache,
					      const auto& os) {
      return backend.get_xattrs(os, attr_cache, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_CMPXATTR:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.cmp_xattr(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_RMXATTR:
    return do_write_op_attr_cache([&osd_op](auto& backend, auto& os, auto& txn, auto& attr_cache) {
      return backend.rm_xattr(os, osd_op, txn, attr_cache);
    });
  case CEPH_OSD_OP_CREATE:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.create(os, osd_op, txn, delta_stats);
    });
  case CEPH_OSD_OP_WRITE:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.write(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_WRITESAME:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.write_same(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_WRITEFULL:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.writefull(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_ROLLBACK:
    return do_write_op([this, &head=obc,
                        &osd_op](auto& backend, auto& os, auto& txn) {
      ceph_assert(obc->ssc);
      return backend.rollback(os, obc->ssc->snapset,
			      osd_op, txn, *osd_op_params, delta_stats,
                              head, pg->obc_loader, snapc);
    });
  case CEPH_OSD_OP_APPEND:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.append(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_TRUNCATE:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      // FIXME: rework needed. Move this out to do_write_op(), introduce
      // do_write_op_no_user_modify()...
      return backend.truncate(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_ZERO:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.zero(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_SETALLOCHINT:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.set_allochint(os, osd_op, txn, delta_stats);
    });
  case CEPH_OSD_OP_SETXATTR:
    return do_write_op_attr_cache([this, &osd_op](auto& backend, auto& os, auto& txn, auto& attr_cache) {
      return backend.setxattr(os, osd_op, txn, delta_stats, attr_cache);
    });
  case CEPH_OSD_OP_DELETE:
  {
    bool whiteout = false;
    if (should_whiteout(obc->ssc->snapset, snapc)) {  // existing obj is old
      logger().debug("{} has or will have clones, will whiteout {}",
                     __func__, obc->obs.oi.soid);
      whiteout = true;
    }
    return do_write_op([this, whiteout](auto& backend, auto& os, auto& txn) {
      struct emptyctx_t {};
      return with_effect_on_obc(
	emptyctx_t{},
	[&](auto &ctx) {
	  int num_bytes = 0;
	  // Calculate num_bytes to be removed
	  if (obc->obs.oi.soid.is_snap()) {
	    ceph_assert(obc->ssc->snapset.clone_overlap.count(
			  obc->obs.oi.soid.snap));
	    num_bytes = obc->ssc->snapset.get_clone_bytes(
	      obc->obs.oi.soid.snap);
	  } else {
	    num_bytes = obc->obs.oi.size;
	  }
	  return backend.remove(os, txn, *osd_op_params,
				delta_stats, whiteout, num_bytes);
	},
	[](auto &&ctx, ObjectContextRef obc, Ref<PG>) {
	  return seastar::do_for_each(
	    obc->watchers,
	    [](auto &p) { return p.second->remove(); }
	  ).then([obc] {
	    obc->watchers.clear();
	    return seastar::now();
	  });
	});
    });
  }
  case CEPH_OSD_OP_CALL:
    return this->do_op_call(osd_op);
  case CEPH_OSD_OP_STAT:
    // note: stat does not require RD
    return do_const_op([this, &osd_op] (/* const */auto& backend, const auto& os) {
      return backend.stat(os, osd_op, delta_stats);
    });

  case CEPH_OSD_OP_TMAPPUT:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.tmapput(os, osd_op, txn, delta_stats, *osd_op_params);
    });
  case CEPH_OSD_OP_TMAPUP:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto &txn) {
      return backend.tmapup(os, osd_op, txn, delta_stats, *osd_op_params);
    });
  case CEPH_OSD_OP_TMAPGET:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.tmapget(os, osd_op, delta_stats);
    });

  // OMAP
  case CEPH_OSD_OP_OMAPGETKEYS:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_get_keys(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAPGETVALS:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_get_vals(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAP_CMP:
    return  do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_cmp(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAPGETHEADER:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_get_header(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAPGETVALSBYKEYS:
    return do_read_op([this, &osd_op](auto& backend, const auto& os) {
      return backend.omap_get_vals_by_keys(os, osd_op, delta_stats);
    });
  case CEPH_OSD_OP_OMAPSETVALS:
    if (!pg->get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_set_vals(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_OMAPSETHEADER:
    if (!pg->get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_set_header(os, osd_op, txn, *osd_op_params,
        delta_stats);
    });
  case CEPH_OSD_OP_OMAPRMKEYRANGE:
    if (!pg->get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_remove_range(os, osd_op, txn, delta_stats);
    });
  case CEPH_OSD_OP_OMAPRMKEYS:
    if (!pg->get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
    return do_write_op([&osd_op, this](auto& backend, auto& os, auto& txn) {
      return backend.omap_remove_key(os, osd_op, txn, *osd_op_params, delta_stats);
    });
  case CEPH_OSD_OP_OMAPCLEAR:
    if (!pg->get_pgpool().info.supports_omap()) {
      return crimson::ct_error::operation_not_supported::make();
    }
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return backend.omap_clear(os, osd_op, txn, *osd_op_params, delta_stats);
    });

  // watch/notify
  case CEPH_OSD_OP_WATCH:
    return do_write_op([this, &osd_op](auto& backend, auto& os, auto& txn) {
      return do_op_watch(osd_op, os, txn);
    }, modified_by::sys);
  case CEPH_OSD_OP_LIST_WATCHERS:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_op_list_watchers(osd_op, os);
    });
  case CEPH_OSD_OP_NOTIFY:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_op_notify(osd_op, os);
    });
  case CEPH_OSD_OP_NOTIFY_ACK:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_op_notify_ack(osd_op, os);
    });
  case CEPH_OSD_OP_ASSERT_VER:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_assert_ver(osd_op, os);
    });
  case CEPH_OSD_OP_LIST_SNAPS:
    return do_snapset_op([this, &osd_op](const auto &os, const auto &ss) {
      return do_list_snaps(osd_op, os, ss);
    });
  case CEPH_OSD_OP_COPY_GET:
    return do_read_op([this, &osd_op](auto&, const auto& os) {
      return do_copy_get(osd_op, os);
    });
  case CEPH_OSD_OP_COPY_FROM:
  case CEPH_OSD_OP_COPY_FROM2:
    return do_copy_from(osd_op);

  default:
    logger().warn("unknown op {}", ceph_osd_op_name(op.op));
    throw std::runtime_error(
      fmt::format("op '{}' not supported", ceph_osd_op_name(op.op)));
  }
}

OpsExecuter::rep_op_fut_t
OpsExecuter::flush_changes_and_submit(
  const std::vector<OSDOp>& ops,
  SnapMapper& snap_mapper,
  OSDriver& osdriver)
{
  const bool want_mutate = !txn.empty();
  // osd_op_params are instantiated by every wr-like operation.
  assert(osd_op_params || !want_mutate);
  assert(obc);

  auto submitted = interruptor::now();
  auto all_completed = interruptor::now();

  if (cloning_ctx) {
    ceph_assert(want_mutate);
  }

  apply_stats();
  if (want_mutate) {
    osd_op_params->at_version = pg->get_next_version();
    osd_op_params->pg_trim_to = pg->get_pg_trim_to();
    osd_op_params->pg_committed_to = pg->get_pg_committed_to();
    osd_op_params->last_complete = pg->get_info().last_complete;

    std::vector<pg_log_entry_t> log_entries;

    if (cloning_ctx) {
      log_entries.emplace_back(complete_cloning_ctx());
    }

    log_entries.emplace_back(prepare_head_update(ops, txn));

    if (auto log_rit = log_entries.rbegin(); log_rit != log_entries.rend()) {
      ceph_assert(log_rit->version == osd_op_params->at_version);
    }
    auto [_submitted, _all_completed] = co_await pg->submit_transaction(
      std::move(obc),
      cloning_ctx ? std::move(cloning_ctx->clone_obc) : nullptr,
      std::move(txn),
      std::move(*osd_op_params),
      std::move(log_entries)
    );

    submitted = std::move(_submitted);
    all_completed = std::move(_all_completed);
  }

  if (op_effects.size()) [[unlikely]] {
    // need extra ref pg due to apply_stats() which can be executed after
    // informing snap mapper
    all_completed =
      std::move(all_completed).then_interruptible([this, pg=this->pg] {
      // let's do the cleaning of `op_effects` in destructor
      return interruptor::do_for_each(op_effects,
        [pg=std::move(pg)](auto& op_effect) {
        return op_effect->execute(pg);
      });
    });
  }

  co_return std::make_tuple(
    std::move(submitted),
    std::move(all_completed));
}

pg_log_entry_t OpsExecuter::prepare_head_update(
  const std::vector<OSDOp>& ops,
  ceph::os::Transaction &txn)
{
  LOG_PREFIX(OpsExecuter::prepare_head_update);
  assert(obc->obs.oi.soid.snap >= CEPH_MAXSNAP);
  assert(obc->obs.oi.soid.is_head());

  update_clone_overlap();
  if (cloning_ctx) {
    obc->ssc->snapset = std::move(cloning_ctx->new_snapset);
  }
  if (snapc.seq > obc->ssc->snapset.seq) {
     // update snapset with latest snap context
     obc->ssc->snapset.seq = snapc.seq;
  }

  // a user modification bumps the user_version. Otherwise, COPY_FROM
  // sets the object's user_version to that of the source object.
  const std::optional<version_t> new_user_version =
    osd_op_params->user_modify ?
      std::optional<version_t>{osd_op_params->at_version.version} :
      osd_op_params->user_version_override;

  pg_log_entry_t ret{
    obc->obs.exists ?
      pg_log_entry_t::MODIFY : pg_log_entry_t::DELETE,
    obc->obs.oi.soid,
    osd_op_params->at_version,
    obc->obs.oi.version,
    new_user_version.value_or(0),
    osd_op_params->req_id,
    osd_op_params->mtime,
    op_info.allows_returnvec() && !ops.empty() ? ops.back().rval.code : 0};
  ret.extra_reqids = std::move(osd_op_params->extra_reqids);
  ret.extra_reqid_return_codes =
    std::move(osd_op_params->extra_reqid_return_codes);

  if (op_info.allows_returnvec()) {
    // also the per-op values are recorded in the pg log
    ret.set_op_returns(ops);
    DEBUGDPP("op returns: {}", *pg, ret.op_returns);
  }
  ret.clean_regions = std::move(osd_op_params->clean_regions);


  if (obc->obs.exists) {
    obc->obs.oi.prior_version = obc->obs.oi.version;
    obc->obs.oi.version = osd_op_params->at_version;
    if (new_user_version)
      obc->obs.oi.user_version = *new_user_version;
    obc->obs.oi.last_reqid = osd_op_params->req_id;
    obc->obs.oi.mtime = osd_op_params->mtime;
    obc->obs.oi.local_mtime = ceph_clock_now();
    
    obc->ssc->exists = true;
    pg->get_backend().set_metadata(
      obc->obs.oi.soid,
      obc->obs.oi,
      obc->obs.oi.soid.is_head() ? &(obc->ssc->snapset) : nullptr,
      txn);
  } else {
    // reset cached ObjectState without enforcing eviction
    obc->obs.oi = object_info_t(obc->obs.oi.soid);
  }
  
  DEBUGDPP("entry: {}", *pg, ret);
  return ret;
}

// Defined here because there is a circular dependency between OpsExecuter and PG
uint32_t OpsExecuter::get_pool_stripe_width() const {
  return pg->get_pgpool().info.get_stripe_width();
}

// Defined here because there is a circular dependency between OpsExecuter and PG
version_t OpsExecuter::get_last_user_version() const
{
  return pg->get_last_user_version();
}

void OpsExecuter::prepare_cloning_ctx(
  const SnapContext& snapc,
  const ObjectState& initial_obs,
  const SnapSet& initial_snapset,
  PGBackend& backend,
  ceph::os::Transaction& txn)
{
  LOG_PREFIX(OpsExecuter::prepare_cloning_ctx);
  const hobject_t& soid = initial_obs.oi.soid;
  logger().debug("{} {} snapset={} snapc={}",
                 __func__, soid,
                 initial_snapset, snapc);

  cloning_ctx = std::make_unique<CloningContext>();
  cloning_ctx->new_snapset = initial_snapset;

  // clone object, the snap field is set to the seq of the SnapContext
  // at its creation.
  cloning_ctx->coid = soid;
  cloning_ctx->coid.snap = snapc.seq;

  // existing snaps are stored in descending order in snapc,
  // cloned_snaps vector will hold all the snaps stored until snapset.seq
  const std::vector<snapid_t> cloned_snaps = [&] {
    auto last = std::find_if(
      std::begin(snapc.snaps), std::end(snapc.snaps),
      [&](snapid_t snap_id) { return snap_id <= initial_snapset.seq; });
    return std::vector<snapid_t>{std::begin(snapc.snaps), last};
  }();

  // make clone here, but populate in metadata in complete_cloning_ctx
  backend.clone_for_write(soid, cloning_ctx->coid, txn);

  cloning_ctx->clone_obc = prepare_clone(cloning_ctx->coid, initial_obs);

  delta_stats.num_objects++;
  if (cloning_ctx->clone_obc->obs.oi.is_omap()) {
    delta_stats.num_objects_omap++;
  }
  delta_stats.num_object_clones++;
  // newsnapset is obc's ssc
  cloning_ctx->new_snapset.clones.push_back(cloning_ctx->coid.snap);
  cloning_ctx->new_snapset.clone_size[cloning_ctx->coid.snap] = initial_obs.oi.size;
  cloning_ctx->new_snapset.clone_snaps[cloning_ctx->coid.snap] = cloned_snaps;

  // clone_overlap should contain an entry for each clone
  // (an empty interval_set if there is no overlap)
  auto &overlap = cloning_ctx->new_snapset.clone_overlap[cloning_ctx->coid.snap];
  if (initial_obs.oi.size) {
    overlap.insert(0, initial_obs.oi.size);
  }

  // log clone
  DEBUGDPP("cloning v {} to {} v {} snaps={} snapset={}", *pg,
	   initial_obs.oi.version, cloning_ctx->coid,
	   osd_op_params->at_version, cloned_snaps, cloning_ctx->new_snapset);
}

pg_log_entry_t OpsExecuter::complete_cloning_ctx()
{
  ceph_assert(cloning_ctx);
  const auto &coid = cloning_ctx->coid;
  cloning_ctx->clone_obc->obs.oi.version = osd_op_params->at_version;

  osd_op_params->at_version.version++;

  pg->get_backend().set_metadata(
    cloning_ctx->coid,
    cloning_ctx->clone_obc->obs.oi,
    nullptr /* snapset */,
    txn);

  pg_log_entry_t ret{
    pg_log_entry_t::CLONE,
    coid,
    cloning_ctx->clone_obc->obs.oi.version,
    cloning_ctx->clone_obc->obs.oi.prior_version,
    cloning_ctx->clone_obc->obs.oi.user_version,
    osd_reqid_t(),
    cloning_ctx->clone_obc->obs.oi.mtime, // will be replaced in `apply_to()`
    0
  };
  ceph_assert(cloning_ctx->new_snapset.clone_snaps.count(coid.snap));
  encode(cloning_ctx->new_snapset.clone_snaps[coid.snap], ret.snaps);
  ret.clean_regions.mark_data_region_dirty(0, cloning_ctx->clone_obc->obs.oi.size);
  ret.mtime = cloning_ctx->clone_obc->obs.oi.mtime;
  return ret;
}

void OpsExecuter::update_clone_overlap() {
  interval_set<uint64_t> *newest_overlap;
  if (cloning_ctx) {
    newest_overlap =
      &cloning_ctx->new_snapset.clone_overlap.rbegin()->second;
  } else if (op_info.may_write() 
    && obc->obs.exists 
    && !obc->ssc->snapset.clones.empty()) {
    newest_overlap =
      &obc->ssc->snapset.clone_overlap.rbegin()->second;
  } else {
    return;
  }

  assert(osd_op_params);
  osd_op_params->modified_ranges.intersection_of(*newest_overlap);
  newest_overlap->subtract(osd_op_params->modified_ranges);
  delta_stats.num_bytes += osd_op_params->modified_ranges.size();
}

ObjectContextRef OpsExecuter::prepare_clone(
  const hobject_t& coid,
  const ObjectState& initial_obs)
{
  ceph_assert(pg->is_primary());
  ObjectState clone_obs{coid};
  clone_obs.exists = true;
  // clone_obs.oi.version will be populated in complete_cloning_ctx
  clone_obs.oi.prior_version = initial_obs.oi.version;
  clone_obs.oi.copy_user_bits(initial_obs.oi);
  clone_obs.oi.clear_flag(object_info_t::FLAG_WHITEOUT);

  osd_op_params->at_version.version++;

  auto [clone_obc, existed] = pg->obc_registry.get_cached_obc(std::move(coid));
  ceph_assert(!existed);

  clone_obc->set_clone_state(std::move(clone_obs));
  clone_obc->ssc = obc->ssc;
  return clone_obc;
}

void OpsExecuter::apply_stats()
{
  pg->apply_stats(get_target(), delta_stats);
}

OpsExecuter::OpsExecuter(Ref<PG> _pg,
                         ObjectContextRef _obc,
                         const OpInfo& op_info,
                         abstracted_msg_t&& msg,
                         crimson::net::ConnectionXcoreRef conn,
                         const SnapContext& _snapc)
  : pg(std::move(_pg)),
    obc(std::move(_obc)),
    op_info(op_info),
    msg(std::move(msg)),
    conn(conn),
    txn(pg->min_peer_features()),
    snapc(_snapc)
{
  if (op_info.may_write() && should_clone(*obc, snapc)) {
    do_write_op([this](auto& backend, auto& os, auto& txn) {
      prepare_cloning_ctx(
	std::as_const(snapc),
	std::as_const(obc->obs),
	std::as_const(obc->ssc->snapset),
	backend,
	txn);
    });
  }
}

static inline std::unique_ptr<const PGLSFilter> get_pgls_filter(
  const std::string& type,
  bufferlist::const_iterator& iter)
{
  // storing non-const PGLSFilter for the sake of ::init()
  std::unique_ptr<PGLSFilter> filter;
  if (type.compare("plain") == 0) {
    filter = std::make_unique<PGLSPlainFilter>();
  } else {
    std::size_t dot = type.find(".");
    if (dot == type.npos || dot == 0 || dot == type.size() - 1) {
      throw crimson::osd::invalid_argument{};
    }

    const std::string class_name = type.substr(0, dot);
    const std::string filter_name = type.substr(dot + 1);
    ClassHandler::ClassData *cls = nullptr;
    int r = ClassHandler::get_instance().open_class(class_name, &cls);
    if (r != 0) {
      logger().warn("can't open class {}: {}", class_name, cpp_strerror(r));
      if (r == -EPERM) {
        // propogate permission error
        throw crimson::osd::permission_denied{};
      } else {
        throw crimson::osd::invalid_argument{};
      }
    } else {
      ceph_assert(cls);
    }

    ClassHandler::ClassFilter * const class_filter = cls->get_filter(filter_name);
    if (class_filter == nullptr) {
      logger().warn("can't find filter {} in class {}", filter_name, class_name);
      throw crimson::osd::invalid_argument{};
    }

    filter.reset(class_filter->fn());
    if (!filter) {
      // Object classes are obliged to return us something, but let's
      // give an error rather than asserting out.
      logger().warn("buggy class {} failed to construct filter {}",
                    class_name, filter_name);
      throw crimson::osd::invalid_argument{};
    }
  }

  ceph_assert(filter);
  int r = filter->init(iter);
  if (r < 0) {
    logger().warn("error initializing filter {}: {}", type, cpp_strerror(r));
    throw crimson::osd::invalid_argument{};
  }

  // successfully constructed and initialized, return it.
  return filter;
}

static PG::interruptible_future<hobject_t> pgls_filter(
  const PGLSFilter& filter,
  const PGBackend& backend,
  const hobject_t& sobj)
{
  if (std::string xattr = filter.get_xattr(); !xattr.empty()) {
    logger().debug("pgls_filter: filter is interested in xattr={} for obj={}",
                   xattr, sobj);
    return backend.getxattr(sobj, std::move(xattr)).safe_then_interruptible(
      [&filter, sobj] (ceph::bufferlist val) {
        logger().debug("pgls_filter: got xvalue for obj={}", sobj);

        const bool filtered = filter.filter(sobj, val);
        return seastar::make_ready_future<hobject_t>(filtered ? sobj : hobject_t{});
      }, PGBackend::get_attr_errorator::all_same_way([&filter, sobj] {
        logger().debug("pgls_filter: got error for obj={}", sobj);

        if (filter.reject_empty_xattr()) {
          return seastar::make_ready_future<hobject_t>();
        }
        ceph::bufferlist val;
        const bool filtered = filter.filter(sobj, val);
        return seastar::make_ready_future<hobject_t>(filtered ? sobj : hobject_t{});
      }));
  } else {
    ceph::bufferlist empty_lvalue_bl;
    const bool filtered = filter.filter(sobj, empty_lvalue_bl);
    return seastar::make_ready_future<hobject_t>(filtered ? sobj : hobject_t{});
  }
}

static PG::interruptible_future<ceph::bufferlist> do_pgnls_common(
  const hobject_t& pg_start,
  const hobject_t& pg_end,
  const PGBackend& backend,
  const hobject_t& lower_bound,
  const std::string& nspace,
  const uint64_t limit,
  const PGLSFilter* const filter)
{
  if (!(lower_bound.is_min() ||
        lower_bound.is_max() ||
        (lower_bound >= pg_start && lower_bound < pg_end))) {
    // this should only happen with a buggy client.
    throw std::invalid_argument("outside of PG bounds");
  }

  return backend.list_objects(lower_bound, limit).then_interruptible(
    [&backend, filter, nspace](auto&& ret)
    -> PG::interruptible_future<std::tuple<std::vector<hobject_t>, hobject_t>> {
      auto& [objects, next] = ret;
      auto in_my_namespace = [&nspace](const hobject_t& obj) {
        using crimson::common::local_conf;
        if (obj.get_namespace() == local_conf()->osd_hit_set_namespace) {
          return false;
        } else if (nspace == librados::all_nspaces) {
          return true;
        } else {
          return obj.get_namespace() == nspace;
        }
      };
      auto to_pglsed = [&backend, filter] (const hobject_t& obj)
		       -> PG::interruptible_future<hobject_t> {
        // this transformation looks costly. However, I don't have any
        // reason to think PGLS* operations are critical for, let's say,
        // general performance.
        //
        // from tchaikov: "another way is to use seastar::map_reduce(),
        // to 1) save the effort to filter the already filtered objects
        // 2) avoid the space to keep the tuple<bool, object> even if
        // the object is filtered out".
        if (filter) {
          return pgls_filter(*filter, backend, obj);
        } else {
          return seastar::make_ready_future<hobject_t>(obj);
        }
      };

      auto range = objects | boost::adaptors::filtered(in_my_namespace)
                           | boost::adaptors::transformed(to_pglsed);
      logger().debug("do_pgnls_common: finishing the 1st stage of pgls");
      return seastar::when_all_succeed(std::begin(range),
                                       std::end(range)).then(
        [next=std::move(next)] (auto items) mutable {
          // the sole purpose of this chaining is to pass `next` to 2nd
          // stage altogether with items
          logger().debug("do_pgnls_common: 1st done");
          return seastar::make_ready_future<
            std::tuple<std::vector<hobject_t>, hobject_t>>(
              std::move(items), std::move(next));
      });
  }).then_interruptible(
    [pg_end] (auto&& ret) {
      auto& [items, next] = ret;
      auto is_matched = [] (const auto& obj) {
        return !obj.is_min();
      };
      auto to_entry = [] (const auto& obj) {
        return librados::ListObjectImpl{
          obj.get_namespace(), obj.oid.name, obj.get_key()
        };
      };

      pg_nls_response_t response;
      boost::push_back(response.entries, items | boost::adaptors::filtered(is_matched)
                                               | boost::adaptors::transformed(to_entry));
      response.handle = next.is_max() ? pg_end : next;
      ceph::bufferlist out;
      encode(response, out);
      logger().debug("do_pgnls_common: response.entries.size()= {}",
                     response.entries.size());
      return seastar::make_ready_future<ceph::bufferlist>(std::move(out));
  });
}

static PG::interruptible_future<> do_pgnls(
  const PG& pg,
  const std::string& nspace,
  OSDOp& osd_op)
{
  hobject_t lower_bound;
  try {
    ceph::decode(lower_bound, osd_op.indata);
  } catch (const buffer::error&) {
    throw std::invalid_argument("unable to decode PGNLS handle");
  }
  const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
  const auto pg_end = \
    pg.get_pgid().pgid.get_hobj_end(pg.get_pgpool().info.get_pg_num());
  return do_pgnls_common(pg_start,
                         pg_end,
                         pg.get_backend(),
                         lower_bound,
                         nspace,
                         osd_op.op.pgls.count,
                         nullptr /* no filter */)
    .then_interruptible([&osd_op](bufferlist bl) {
      osd_op.outdata = std::move(bl);
      return seastar::now();
  });
}

static PG::interruptible_future<> do_pgnls_filtered(
  const PG& pg,
  const std::string& nspace,
  OSDOp& osd_op)
{
  std::string cname, mname, type;
  auto bp = osd_op.indata.cbegin();
  try {
    ceph::decode(cname, bp);
    ceph::decode(mname, bp);
    ceph::decode(type, bp);
  } catch (const buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  auto filter = get_pgls_filter(type, bp);

  hobject_t lower_bound;
  try {
    lower_bound.decode(bp);
  } catch (const buffer::error&) {
    throw std::invalid_argument("unable to decode PGNLS_FILTER description");
  }

  logger().debug("{}: cname={}, mname={}, type={}, lower_bound={}, filter={}",
                 __func__, cname, mname, type, lower_bound,
                 static_cast<const void*>(filter.get()));
  return seastar::do_with(std::move(filter),
    [&, lower_bound=std::move(lower_bound)](auto&& filter) {
      const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
      const auto pg_end = pg.get_pgid().pgid.get_hobj_end(pg.get_pgpool().info.get_pg_num());
      return do_pgnls_common(pg_start,
                             pg_end,
                             pg.get_backend(),
                             lower_bound,
                             nspace,
                             osd_op.op.pgls.count,
                             filter.get())
        .then_interruptible([&osd_op](bufferlist bl) {
          osd_op.outdata = std::move(bl);
          return seastar::now();
      });
  });
}

static PG::interruptible_future<ceph::bufferlist> do_pgls_common(
  const hobject_t& pg_start,
  const hobject_t& pg_end,
  const PGBackend& backend,
  const hobject_t& lower_bound,
  const std::string& nspace,
  const uint64_t limit,
  const PGLSFilter* const filter)
{
  if (!(lower_bound.is_min() ||
        lower_bound.is_max() ||
        (lower_bound >= pg_start && lower_bound < pg_end))) {
    // this should only happen with a buggy client.
    throw std::invalid_argument("outside of PG bounds");
  }

  using entries_t = decltype(pg_ls_response_t::entries);
  return backend.list_objects(lower_bound, limit).then_interruptible(
    [&backend, filter, nspace](auto&& ret) {
      auto& [objects, next] = ret;
      return PG::interruptor::when_all(
        PG::interruptor::map_reduce(std::move(objects),
          [&backend, filter, nspace](const hobject_t& obj)
	  -> PG::interruptible_future<hobject_t>{
            if (obj.get_namespace() == nspace) {
              if (filter) {
                return pgls_filter(*filter, backend, obj);
              } else {
                return seastar::make_ready_future<hobject_t>(obj);
              }
            } else {
              return seastar::make_ready_future<hobject_t>();
            }
          },
          entries_t{},
          [](entries_t entries, hobject_t obj) {
            if (!obj.is_min()) {
              entries.emplace_back(obj.oid, obj.get_key());
            }
            return entries;
          }),
        seastar::make_ready_future<hobject_t>(next));
    }).then_interruptible([pg_end](auto&& ret) {
      auto entries = std::get<0>(ret).get();
      auto next = std::get<1>(std::move(ret)).get();
      pg_ls_response_t response;
      response.handle = next.is_max() ? pg_end : next;
      response.entries = std::move(entries);
      ceph::bufferlist out;
      encode(response, out);
      logger().debug("{}: response.entries.size()=",
                     __func__, response.entries.size());
      return seastar::make_ready_future<ceph::bufferlist>(std::move(out));
  });
}

static PG::interruptible_future<> do_pgls(
   const PG& pg,
   const std::string& nspace,
   OSDOp& osd_op)
{
  hobject_t lower_bound;
  auto bp = osd_op.indata.cbegin();
  try {
    lower_bound.decode(bp);
  } catch (const buffer::error&) {
    throw std::invalid_argument{"unable to decode PGLS handle"};
  }
  const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
  const auto pg_end =
    pg.get_pgid().pgid.get_hobj_end(pg.get_pgpool().info.get_pg_num());
  return do_pgls_common(pg_start,
			pg_end,
			pg.get_backend(),
			lower_bound,
			nspace,
			osd_op.op.pgls.count,
			nullptr /* no filter */)
    .then_interruptible([&osd_op](bufferlist bl) {
      osd_op.outdata = std::move(bl);
      return seastar::now();
    });
}

static PG::interruptible_future<> do_pgls_filtered(
  const PG& pg,
  const std::string& nspace,
  OSDOp& osd_op)
{
  std::string cname, mname, type;
  auto bp = osd_op.indata.cbegin();
  try {
    ceph::decode(cname, bp);
    ceph::decode(mname, bp);
    ceph::decode(type, bp);
  } catch (const buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  auto filter = get_pgls_filter(type, bp);

  hobject_t lower_bound;
  try {
    lower_bound.decode(bp);
  } catch (const buffer::error&) {
    throw std::invalid_argument("unable to decode PGLS_FILTER description");
  }

  logger().debug("{}: cname={}, mname={}, type={}, lower_bound={}, filter={}",
                 __func__, cname, mname, type, lower_bound,
                 static_cast<const void*>(filter.get()));
  return seastar::do_with(std::move(filter),
    [&, lower_bound=std::move(lower_bound)](auto&& filter) {
      const auto pg_start = pg.get_pgid().pgid.get_hobj_start();
      const auto pg_end = pg.get_pgid().pgid.get_hobj_end(pg.get_pgpool().info.get_pg_num());
      return do_pgls_common(pg_start,
                            pg_end,
                            pg.get_backend(),
                            lower_bound,
                            nspace,
                            osd_op.op.pgls.count,
                            filter.get())
        .then_interruptible([&osd_op](bufferlist bl) {
          osd_op.outdata = std::move(bl);
          return seastar::now();
      });
  });
}

PgOpsExecuter::interruptible_future<>
PgOpsExecuter::execute_op(OSDOp& osd_op)
{
  logger().debug("handling op {}", ceph_osd_op_name(osd_op.op.op));
  switch (const ceph_osd_op& op = osd_op.op; op.op) {
  case CEPH_OSD_OP_PGLS:
    return do_pgls(pg, nspace, osd_op);
  case CEPH_OSD_OP_PGLS_FILTER:
    return do_pgls_filtered(pg, nspace, osd_op);
  case CEPH_OSD_OP_PGNLS:
    return do_pgnls(pg, nspace, osd_op);
  case CEPH_OSD_OP_PGNLS_FILTER:
    return do_pgnls_filtered(pg, nspace, osd_op);
  default:
    logger().warn("unknown op {}", ceph_osd_op_name(op.op));
    throw std::runtime_error(
      fmt::format("op '{}' not supported", ceph_osd_op_name(op.op)));
  }
}

} // namespace crimson::osd
