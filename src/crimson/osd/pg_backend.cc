// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "pg_backend.h"

#include <optional>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/transformed.hpp>
#include <boost/range/algorithm/copy.hpp>
#include <fmt/format.h>
#include <fmt/ostream.h>
#include <seastar/core/print.hh>

#include "messages/MOSDOp.h"
#include "os/Transaction.h"
#include "common/Checksummer.h"
#include "common/Clock.h"

#include "crimson/common/exception.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/osd_operation.h"
#include "replicated_backend.h"
#include "replicated_recovery_backend.h"
#include "ec_backend.h"
#include "exceptions.h"

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}

using crimson::common::local_conf;

std::unique_ptr<PGBackend>
PGBackend::create(pg_t pgid,
		  const pg_shard_t pg_shard,
		  const pg_pool_t& pool,
		  crimson::os::CollectionRef coll,
		  crimson::osd::ShardServices& shard_services,
		  const ec_profile_t& ec_profile)
{
  switch (pool.type) {
  case pg_pool_t::TYPE_REPLICATED:
    return std::make_unique<ReplicatedBackend>(pgid, pg_shard,
					       coll, shard_services);
  case pg_pool_t::TYPE_ERASURE:
    //return std::make_unique<ECBackend>(pg_shard, coll, shard_services,
      //                                 /*std::move(*/ec_profile,
      //                                 pool.stripe_width);
  default:
    throw runtime_error(seastar::format("unsupported pool type '{}'",
                                        pool.type));
  }
}

PGBackend::PGBackend(pg_shard_t shard,
                     CollectionRef coll,
                     crimson::os::FuturizedStore* store,
		     crimson::osd::ShardServices& shard_services)
  : shard{shard.shard},
    coll{coll},
    store{store},
    shard_services{shard_services},
    pg_shard{shard}
{
  logger().warn("{}: {:p}/{:p}", __func__ , (void*)store, (void*)(&shard_services.get_store()));
  logger().warn("{}: collection: {}", __func__ , coll->get_cid());
}

PGBackend::load_metadata_ertr::future<PGBackend::loaded_object_md_t::ref>
PGBackend::load_metadata(const hobject_t& oid)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  logger().debug("{}: {}", __func__, oid.to_str());
  return store->get_attrs(
    coll,
    ghobject_t{oid, ghobject_t::NO_GEN, shard}).safe_then(
      [oid](auto &&attrs) -> load_metadata_ertr::future<loaded_object_md_t::ref>{
	loaded_object_md_t::ref ret(new loaded_object_md_t{});
	if (auto oiiter = attrs.find(OI_ATTR); oiiter != attrs.end()) {
	  bufferlist bl;
	  //logger().info("load_metadata {}: {}", oid, oiiter->second);
	  bl.push_back(std::move(oiiter->second));
	  ret->os = ObjectState(
	    object_info_t(bl),
	    true);
	  logger().info("load_metadata {}: {}", oid, ret->os.oi.get_flag_string());
	} else {
	  logger().error(
	    "load_metadata: object {} present but missing object info",
	    oid);
	  return crimson::ct_error::object_corrupted::make();
	}

	if (oid.is_head()) {
	  if (auto ssiter = attrs.find(SS_ATTR); ssiter != attrs.end()) {
	    bufferlist bl;
	    bl.push_back(std::move(ssiter->second));
	    ret->ss = SnapSet(bl);
	  } else {
	    /* TODO: add support for writing out snapsets
	    logger().error(
	      "load_metadata: object {} present but missing snapset",
	      oid);
	    //return crimson::ct_error::object_corrupted::make();
	    */
	    ret->ss = SnapSet();
	  }
	}

	return load_metadata_ertr::make_ready_future<loaded_object_md_t::ref>(
	  std::move(ret));
      }, crimson::ct_error::enoent::handle([oid] {
	logger().debug(
	  "load_metadata: object {} doesn't exist, returning empty metadata",
	  oid);
	return load_metadata_ertr::make_ready_future<loaded_object_md_t::ref>(
	  new loaded_object_md_t{
	    ObjectState(
	      object_info_t(oid),
	      false),
	    oid.is_head() ? std::optional<SnapSet>(SnapSet()) : std::nullopt
	  });
      }));
}

PGBackend::load_metadata_ertr::future<PGBackend::attrs_t>
PGBackend::get_the_attrs(const hobject_t& oid)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  logger().debug("{}: {}", __func__, oid.to_str());
  return store->get_attrs(
    coll,
    ghobject_t{oid, ghobject_t::NO_GEN, shard}).safe_then(
    [oid](auto &&attrs) -> load_metadata_ertr::future<PGBackend::attrs_t>{
      return load_metadata_ertr::make_ready_future<PGBackend::attrs_t>(std::move(attrs));

    }, crimson::ct_error::enoent::handle([oid] {
      logger().debug(
	"get_the_attrs: object {} doesn't exist, returning empty attrs map",
	oid);
      return load_metadata_ertr::make_ready_future<PGBackend::attrs_t>(
	PGBackend::attrs_t{}
      );
    }));
}

seastar::future<crimson::osd::acked_peers_t>
PGBackend::mutate_object(
  std::set<pg_shard_t> pg_shards,
  crimson::osd::ObjectContextRef &&obc,
  ceph::os::Transaction&& txn,
  osd_op_params_t&& osd_op_p,
  epoch_t min_epoch,
  epoch_t map_epoch,
  std::vector<pg_log_entry_t>&& log_entries)
{
  logger().trace("mutate_object: num_ops={}", txn.get_num_ops());
  if (obc->obs.exists) {
#if 0
    obc->obs.oi.version = ctx->at_version;
    obc->obs.oi.prior_version = ctx->obs->oi.version;
#endif

    auto& m = osd_op_p.req;
    obc->obs.oi.prior_version = obc->obs.oi.version;
    obc->obs.oi.version = osd_op_p.at_version;
    if (osd_op_p.user_at_version > obc->obs.oi.user_version)
      obc->obs.oi.user_version = osd_op_p.user_at_version;
    obc->obs.oi.last_reqid = m->get_reqid();
    obc->obs.oi.mtime = m->get_mtime();
    obc->obs.oi.local_mtime = ceph_clock_now();

    // object_info_t
    {
      ceph::bufferlist osv;
      encode(obc->obs.oi, osv, CEPH_FEATURES_ALL);
      // TODO: get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr));
      txn.setattr(coll->get_cid(), ghobject_t{obc->obs.oi.soid}, OI_ATTR, osv);
    }
  } else {
    // reset cached ObjectState without enforcing eviction
    obc->obs.oi = object_info_t(obc->obs.oi.soid);
  }
  return _submit_transaction(
    std::move(pg_shards), obc->obs.oi.soid, std::move(txn),
    std::move(osd_op_p), min_epoch, map_epoch, std::move(log_entries));
}

static inline bool _read_verify_data(
  const object_info_t& oi,
  const ceph::bufferlist& data)
{
  if (oi.is_data_digest() && oi.size == data.length()) {
    // whole object?  can we verify the checksum?
    if (auto crc = data.crc32c(-1); crc != oi.data_digest) {
      logger().error("full-object read crc {} != expected {} on {}",
                     crc, oi.data_digest, oi.soid);
      // todo: mark soid missing, perform recovery, and retry
      return false;
    }
  }
  return true;
}

PGBackend::read_errorator::future<>
PGBackend::read(const ObjectState& os, OSDOp& osd_op)
{
  const auto& oi = os.oi;
  const ceph_osd_op& op = osd_op.op;
  const uint64_t offset = op.extent.offset;
  uint64_t length = op.extent.length;
  logger().trace("read: {} {}~{}", oi.soid, offset, length);

  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: {} DNE", __func__, os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  // are we beyond truncate_size?
  size_t size = oi.size;
  if ((op.extent.truncate_seq > oi.truncate_seq) &&
      (op.extent.truncate_size < offset + length) &&
      (op.extent.truncate_size < size)) {
    size = op.extent.truncate_size;
  }
  if (offset >= size) {
    // read size was trimmed to zero and it is expected to do nothing,
    return read_errorator::now();
  }
  if (!length) {
    // read the whole object if length is 0
    length = size;
  }
  return _read(oi.soid, offset, length, op.flags).safe_then(
    [&oi, &osd_op](auto&& bl) -> read_errorator::future<> {
    if (!_read_verify_data(oi, bl)) {
      // crc mismatches
      return crimson::ct_error::object_corrupted::make();
    }
    logger().debug("read: data length: {}", bl.length());
    osd_op.rval = bl.length();
    osd_op.outdata = std::move(bl);
    return read_errorator::now();
  }, crimson::ct_error::input_output_error::handle([] {
    return read_errorator::future<>{crimson::ct_error::object_corrupted::make()};
  }),
  read_errorator::pass_further{});
}

PGBackend::read_errorator::future<>
PGBackend::sparse_read(const ObjectState& os, OSDOp& osd_op)
{
  const auto& op = osd_op.op;
  logger().trace("sparse_read: {} {}~{}",
                 os.oi.soid, op.extent.offset, op.extent.length);
  return store->fiemap(coll, ghobject_t{os.oi.soid},
		       op.extent.offset,
		       op.extent.length).then([&os, &osd_op, this](auto&& m) {
    return seastar::do_with(interval_set<uint64_t>{std::move(m)},
			    [&os, &osd_op, this](auto&& extents) {
      return store->readv(coll, ghobject_t{os.oi.soid},
                          extents, osd_op.op.flags).safe_then(
        [&os, &osd_op, &extents](auto&& bl) -> read_errorator::future<> {
        if (_read_verify_data(os.oi, bl)) {
          osd_op.op.extent.length = bl.length();
          // re-encode since it might be modified
          ceph::encode(extents, osd_op.outdata);
          encode_destructively(bl, osd_op.outdata);
          logger().trace("sparse_read got {} bytes from object {}",
                         osd_op.op.extent.length, os.oi.soid);
          return read_errorator::make_ready_future<>();
        } else {
          // crc mismatches
          return crimson::ct_error::object_corrupted::make();
        }
      }, crimson::ct_error::input_output_error::handle([] {
        return read_errorator::future<>{crimson::ct_error::object_corrupted::make()};
      }),
      read_errorator::pass_further{});
    });
  });
}

namespace {

  template<class CSum>
  PGBackend::checksum_errorator::future<>
  do_checksum(ceph::bufferlist& init_value_bl,
	      size_t chunk_size,
	      const ceph::bufferlist& buf,
	      ceph::bufferlist& result)
  {
    typename CSum::init_value_t init_value;
    auto init_value_p = init_value_bl.cbegin();
    try {
      decode(init_value, init_value_p);
      // chop off the consumed part
      init_value_bl.splice(0, init_value_p.get_off());
    } catch (const ceph::buffer::end_of_buffer&) {
      logger().warn("{}: init value not provided", __func__);
      return crimson::ct_error::invarg::make();
    }
    const uint32_t chunk_count = buf.length() / chunk_size;
    ceph::bufferptr csum_data{
      ceph::buffer::create(sizeof(typename CSum::value_t) * chunk_count)};
    Checksummer::calculate<CSum>(
      init_value, chunk_size, 0, buf.length(), buf, &csum_data);
    encode(chunk_count, result);
    result.append(std::move(csum_data));
    return PGBackend::checksum_errorator::now();
  }
}

PGBackend::checksum_errorator::future<>
PGBackend::checksum(const ObjectState& os, OSDOp& osd_op)
{
  // sanity tests and normalize the argments
  auto& checksum = osd_op.op.checksum;
  if (checksum.offset == 0 && checksum.length == 0) {
    // zeroed offset+length implies checksum whole object
    checksum.length = os.oi.size;
  } else if (checksum.offset >= os.oi.size) {
    // read size was trimmed to zero, do nothing,
    // see PGBackend::read()
    return checksum_errorator::now();
  }
  if (checksum.chunk_size > 0) {
    if (checksum.length == 0) {
      logger().warn("{}: length required when chunk size provided", __func__);
      return crimson::ct_error::invarg::make();
    }
    if (checksum.length % checksum.chunk_size != 0) {
      logger().warn("{}: length not aligned to chunk size", __func__);
      return crimson::ct_error::invarg::make();
    }
  } else {
    checksum.chunk_size = checksum.length;
  }
  if (checksum.length == 0) {
    uint32_t count = 0;
    encode(count, osd_op.outdata);
    return checksum_errorator::now();
  }

  // read the chunk to be checksum'ed
  return _read(os.oi.soid, checksum.offset, checksum.length, osd_op.op.flags).safe_then(
    [&osd_op](auto&& read_bl) mutable -> checksum_errorator::future<> {
    auto& checksum = osd_op.op.checksum;
    if (read_bl.length() != checksum.length) {
      logger().warn("checksum: bytes read {} != {}",
                        read_bl.length(), checksum.length);
      return crimson::ct_error::invarg::make();
    }
    // calculate its checksum and put the result in outdata
    switch (checksum.type) {
    case CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH32:
      return do_checksum<Checksummer::xxhash32>(osd_op.indata,
                                                checksum.chunk_size,
                                                read_bl,
                                                osd_op.outdata);
    case CEPH_OSD_CHECKSUM_OP_TYPE_XXHASH64:
      return do_checksum<Checksummer::xxhash64>(osd_op.indata,
                                                checksum.chunk_size,
                                                read_bl,
                                                osd_op.outdata);
    case CEPH_OSD_CHECKSUM_OP_TYPE_CRC32C:
      return do_checksum<Checksummer::crc32c>(osd_op.indata,
                                              checksum.chunk_size,
                                              read_bl,
                                              osd_op.outdata);
    default:
      logger().warn("checksum: unknown crc type ({})",
		    static_cast<uint32_t>(checksum.type));
      return crimson::ct_error::invarg::make();
    }
  });
}

PGBackend::cmp_ext_errorator::future<>
PGBackend::cmp_ext(const ObjectState& os, OSDOp& osd_op)
{
  const ceph_osd_op& op = osd_op.op;
  // return the index of the first unmatched byte in the payload, hence the
  // strange limit and check
  if (op.extent.length > MAX_ERRNO) {
    return crimson::ct_error::invarg::make();
  }
  uint64_t obj_size = os.oi.size;
  if (os.oi.truncate_seq < op.extent.truncate_seq &&
      op.extent.offset + op.extent.length > op.extent.truncate_size) {
    obj_size = op.extent.truncate_size;
  }
  uint64_t ext_len;
  if (op.extent.offset >= obj_size) {
    ext_len = 0;
  } else if (op.extent.offset + op.extent.length > obj_size) {
    ext_len = obj_size - op.extent.offset;
  } else {
    ext_len = op.extent.length;
  }
  auto read_ext = ll_read_errorator::make_ready_future<ceph::bufferlist>();
  if (ext_len == 0) {
    logger().debug("{}: zero length extent", __func__);
  } else if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: {} DNE", __func__, os.oi.soid);
  } else {
    read_ext = _read(os.oi.soid, op.extent.offset, ext_len, 0);
  }
  return read_ext.safe_then([&osd_op](auto&& read_bl) {
    int32_t retcode = 0;
    for (unsigned index = 0; index < osd_op.indata.length(); index++) {
      char byte_in_op = osd_op.indata[index];
      char byte_from_disk = (index < read_bl.length() ? read_bl[index] : 0);
      if (byte_in_op != byte_from_disk) {
        logger().debug("cmp_ext: mismatch at {}", index);
        retcode = -MAX_ERRNO - index;
	break;
      }
    }
    logger().debug("cmp_ext: {}", retcode);
    osd_op.rval = retcode;
  });
}

PGBackend::stat_errorator::future<> PGBackend::stat(
  const ObjectState& os,
  OSDOp& osd_op)
{
  if (os.exists/* TODO: && !os.is_whiteout() */) {
    logger().debug("stat os.oi.size={}, os.oi.mtime={}", os.oi.size, os.oi.mtime);
    encode(os.oi.size, osd_op.outdata);
    encode(os.oi.mtime, osd_op.outdata);
  } else {
    logger().debug("stat object does not exist");
    return crimson::ct_error::enoent::make();
  }
  return stat_errorator::now();
  // TODO: ctx->delta_stats.num_rd++;
}



/*
  PGBackend::loaded_object_md_t::ref
  after the load_metadata() we have:
   - the exists flag
   - the snapset
 */

#if 0
/*
 * outcome (if the object exists):
 * - the scrub-map entry in the 'pos' position has its length updated from the store
 * - if deep:
 *   we compute checksums, and update the relevant fields in the map entry
 */
PGBackend::ll_read_errorator::future<> PGBackend::scan_obj_from_list(ScrubMap& map,
								  ScrubMapBuilder& pos)
{
  //logger().debug("{}: in position {} NOT IMPLEMENTED", __func__, pos.ls[pos.pos].get_key() );

  return seastar::make_ready_future<>();

#ifdef NOT_YET

  // hobject_t& poid_nu = pos.ls[pos.pos];

  // auto the_obj = ghobject_t{poid, ghobject_t::NO_GEN, shard};

  return seastar::do_with(  //                                                         -
    &pos.ls[pos.pos],	    //                                                        -
    [map, pos, this](auto poid) mutable -> ll_read_errorator::future<> {
      return load_metadata(*poid)
	.safe_then(
	  [poid, pos, this, map](auto md_ref) mutable -> ll_read_errorator::future<> {
	    if (!md_ref->os.exists) {

	      return crimson::ct_error::enoent::make();
	    }

	    // the object exists
	    ScrubMap::object& o = map.objects[*poid];
	    o.size = md_ref->os.oi.size;

	    if (pos.deep) {

	      return calc_deep_scrub_info(*poid, map, pos, o);

	    } else {

	      return ll_read_errorator::make_ready_future();
	    }
	  },

	  load_metadata_ertr::all_same_way(
	    []() { return ll_read_errorator::make_ready_future(); })
	  //);
	  //   }


	  )
	.safe_then(

	  []() { return PGBackend::ll_read_errorator::make_ready_future(); }

	  /*,
	  crimson::ct_error::object_corrupted::handle([indx = pos.pos]() {
	    logger().error(
	      "PGBackend::scan_obj_from_list(): object xx at pos {} missing object info",
	      indx);
	    PGBackend::stat_errorator::assert_all{"object corrupted"};
	    return ll_read_errorator::make_ready_future();
	  })*/,

	  ll_read_errorator::all_same_way(
	    []() { return ll_read_errorator::make_ready_future(); })


	  );
    });
#endif
}
#endif

/*
 * outcome (if the object exists):
 * - the scrub-map entry in the 'pos' position has its length updated from the store
 * - if deep:
 *   we compute checksums, and update the relevant fields in the map entry
 */
PGBackend::ll_read_errorator::future<> PGBackend::scan_obj_from_list(
  ScrubMap& map, hobject_t& obj_in_ls, bool is_deep, std::chrono::milliseconds deep_delay)
{
  logger().debug("{}: scanning {}", __func__, obj_in_ls);

  // RRR should we query about a NO_GEN version of obj_in_ls?


  return load_metadata(obj_in_ls).safe_then(
    [&obj_in_ls, this, &map, is_deep,
     deep_delay](auto&& md_ref) mutable -> ll_read_errorator::future<> {

    if (!md_ref->os.exists) {
      // load_metadata() returns an empty info struct on "no-ent"
      logger().debug("PGBackend::scan_obj_from_list(): obj does not exist");
      return crimson::ct_error::enoent::make();
    }

    // the object exists
    ScrubMap::object& o = map.objects[obj_in_ls];
    return get_the_attrs(obj_in_ls).safe_then(
      [&obj_in_ls, this, &map, is_deep, deep_delay, &o, &md_ref](auto&& ats) {
	o.attrs.merge(ats);
	o.size = md_ref->os.oi.size;
	logger().debug("kend::scan_obj_from_list(): obj:{} sz:{}", obj_in_ls, o.size);

	{
	  // dump the first two entries
	  static int dbg_c{20};
	  if (dbg_c-- > 0) {
	    stringstream lb;
	    lb << " =pgbeat= " << obj_in_ls << " ==pgbeat== \n";
	    for (const auto& [k, v] : o.attrs) {
	      lb << "\t\tPGBackend attr: " << k << " <-> " << v << "\n";
	    }
	    logger().debug("==scan_obj_from_list: {}", lb.str());
	  }
	}

	if (is_deep) {

	  return seastar::sleep(deep_delay)
	    .  // consider using this sleep for chunking
	    then([this, &obj_in_ls, &map, o]() mutable {
	      return calc_deep_scrub_info(obj_in_ls, map, o);
	    });

	} else {

	  return ll_read_errorator::make_ready_future();
	}
      },
      load_metadata_ertr::all_same_way([&obj_in_ls]() {
	logger().warn("PGBackend::scan_obj_from_list: error fetching {} attrs",
		      obj_in_ls);

	return ll_read_errorator::make_ready_future();
      })
    );
    },

     load_metadata_ertr::all_same_way([&obj_in_ls]() {
	logger().warn("PGBackend::scan_obj_from_list: error fetching {} metadata",
		      obj_in_ls);

	return ll_read_errorator::make_ready_future();
      })
    );
}

#if 0
	.safe_then(

	  []() { return PGBackend::ll_read_errorator::make_ready_future(); }

	  /*,
	  crimson::ct_error::object_corrupted::handle([indx = pos.pos]() {
	    logger().error(
	      "PGBackend::scan_obj_from_list(): object xx at pos {} missing object info",
	      indx);
	    PGBackend::stat_merrorator::assert_all{"object corrupted"};
	    return ll_read_errorator::make_ready_future();
	  })*/,

	  ll_read_errorator::all_same_way(
	    []() { return ll_read_errorator::make_ready_future(); })


	);
    //});
#endif


/* go over all objects in the list, starting from current pos.
 *

*/

PGBackend::ll_read_errorator::future<> PGBackend::scan_list(ScrubMap& map, ScrubMapBuilder& pos)
{
  // RRR handle the time conversion:
  utime_t sleeptime;
  //sleeptime.set_from_double(get_cct()->_conf->osd_debug_deep_scrub_sleep)
  std::chrono::milliseconds deep_delay{1ms};

  logger().debug("{}: pos.ls sz:{} pos.deep:{}", __func__, pos.ls.size(), pos.deep);

  return ::crimson::do_for_each(pos.ls, [&map, &pos, this, deep_delay](auto& poid) mutable {

    // do what pos.next_object() would have done:
    //pos.data_pos = 0;
    //pos.omap_pos.clear();
    //pos.omap_keys = 0;
    //pos.omap_bytes = 0;

    return scan_obj_from_list(map, poid, pos.deep, deep_delay).safe_then([](){
    	return PGBackend::ll_read_errorator::make_ready_future();}
    );

  });
}


void PGBackend::omap_checks(const map<pg_shard_t,ScrubMap*> &maps,
			       const set<hobject_t> &master_set,
			       omap_stat_t& omap_stats,
			       ostream &warnstream) const
{
  logger().debug("{}: size of master-set {}", __func__, master_set.size());

  if (std::none_of(maps.cbegin(), maps.cend(), [](auto& mp) {
    return mp.second->has_large_omap_object_errors || mp.second->has_omap_keys;
  })) {
    // no suspect maps
    return;
  }

  // Iterate through objects and update omap stats
  for (const auto& ho : master_set) {
    for (const auto& [map_shard, smap] : maps) {
      if (map_shard.shard != shard) {
	// Only set omap stats for the primary
	continue;
      }

      auto it = smap->objects.find(ho);
      if (it == smap->objects.end())
	continue;

      ScrubMap::object& obj = it->second;
      omap_stats.omap_bytes += obj.object_omap_bytes;
      omap_stats.omap_keys += obj.object_omap_keys;

      if (obj.large_omap_object_found) {
        // 'large_omap_object_found' may have been set by the scrubber
	pg_t pg;
	auto osdmap = shard_services.get_osdmap();
	osdmap->map_to_pg(ho.pool, ho.oid.name, ho.get_key(), ho.nspace, &pg);
	pg_t mpg = osdmap->raw_pg_to_pg(pg);
	omap_stats.large_omap_objects++;
	warnstream << "Large omap object found. Object: " << ho
		   << " PG: " << pg << " (" << mpg << ")"
		   << " Key count: " << obj.large_omap_object_key_count
		   << " Size (bytes): " << obj.large_omap_object_value_size
		   << '\n';
	break;
      }
    }
  }
}


using set_err_fn_t = void (shard_info_wrapper::*)();
using check_so_cond_t = bool ScrubMap::object::*;
enum class on_error_fl { cont, stop };

/// \returns should we continue checking
static bool auth_object_check(bool tested_flag,
		       shard_info_wrapper& shard_info,
		       set_err_fn_t set_err_fn,
		       ostringstream& errstream,
		       std::string_view err_message,
		       bool& prev_error,
		       on_error_fl stop_on_error)
{
  if (tested_flag) {
    logger().debug("{}: flag tested 'true': {}", __func__, err_message);

    (shard_info.*set_err_fn)();

    if (prev_error) {
      errstream << ", ";
    } else {
      prev_error = true;
    }

    errstream << err_message;
    return stop_on_error == on_error_fl::cont;
  }

  return true;
}

inline static int dcount(const object_info_t& oi)
{
  return (oi.is_data_digest() ? 1 : 0) +
    (oi.is_omap_digest() ? 1 : 0);
}

auto PGBackend::select_auth_object(const hobject_t& obj,
				   const map<pg_shard_t, ScrubMap*>& maps,
				   object_info_t* auth_oi,
				   map<pg_shard_t, shard_info_wrapper>& shard_map,
				   bool& digest_match,
				   spg_t pgid,
				   ostream& errorstream)
  -> ll_read_errorator::future<map<pg_shard_t, ScrubMap*>::const_iterator>
{
  // Create list of shards with primary first so it will be auth copy all
  // other things being equal.

  // RRR why do we do that (ordering the shards) for each object separately?
  list<pg_shard_t> shards;
  for (const auto& [sh, smap] : maps) {
    std::ignore = smap;
    if (sh.shard != shard)
      shards.push_back(sh);
  }
  shards.push_front(pg_shard);

  logger().debug("{}: for obj {} (pg {}) shards: xx", __func__, obj, pgid, shards);

  //

  auto auth = maps.cend();
  digest_match = true;
  eversion_t auth_version;

  for (auto& s : shards) {

    const auto shard_s_map = maps.find(s);
    auto [jshard, jmap] = *shard_s_map;

    logger().debug("{}: --- obj {} shard:{}: {} {:p}", __func__, obj, s, jshard,
		   (void*)jmap);

    {
      // dump the first two entries
      static int dbg_c{2};
      if (dbg_c-- > 0) {
	JSONFormatter f;
	stringstream lb;
	lb << " == " << obj << " ==-== ";
	jmap->dump(&f);
	f.flush(lb);
	logger().debug("{}: -v- obj {} shard:{}: {}", __func__, obj, s, lb.str());
      }
    }

    auto obj_in_smap = jmap->objects.find(obj);
    if (obj_in_smap == jmap->objects.end()) {
      continue;
    }

    const auto& [ho, smap_obj] = *obj_in_smap;

    {
      JSONFormatter f;
      stringstream lb;
      lb << ho << "===";
      smap_obj.dump(&f);
      f.flush(lb);
      logger().debug("{}: -z- obj {} shard:{}: {} {}", __func__, obj, s, ho, lb.str());
    }


    // isn't 's' the same as 'shard_s_map->first' ?

    auto& shard_info = shard_map[jshard];
    if (jshard == pg_shard) {
      shard_info.primary = true;
    }

    logger().debug("{}: --- obj {} shard:{}: ho:{} shard-info:XX (pr? {})", __func__, obj,
		   s, ho, /*shard_info,*/ shard_info.primary);


    bool error{false};
    ostringstream shard_errorstream;

    if (auth_object_check(smap_obj.read_error, shard_info,
			  &shard_info_wrapper::set_read_error, shard_errorstream,
			  "candidate had a read error", error, on_error_fl::cont) &&

	auth_object_check(smap_obj.ec_hash_mismatch, shard_info,
			  &shard_info_wrapper::set_ec_hash_mismatch, shard_errorstream,
			  "candidate had an ec hash mismatch", error,
			  on_error_fl::cont) &&

	auth_object_check(smap_obj.ec_size_mismatch, shard_info,
			  &shard_info_wrapper::set_ec_size_mismatch, shard_errorstream,
			  "candidate had an ec size mismatch", error,
			  on_error_fl::cont) &&

	// no further checking if a stat error. We don't need to also see a
	// missing_object_info_attr.
	auth_object_check(smap_obj.stat_error, shard_info,
			  &shard_info_wrapper::set_stat_error, shard_errorstream,
			  "candidate had a stat error", error, on_error_fl::stop)) {


      //  We won't pick an auth copy if the snapset is missing or won't decode.

#ifdef NOT_YET
      ceph_assert(!obj.is_snapdir());
      SnapSet snapset;
      bufferlist ss_bl;
      if (obj.is_head()) {
	auto snap_it = smap_obj.attrs.find(SS_ATTR);
	if (auth_object_check( (snap_it == smap_obj.attrs.end()), shard_info,
	                  &shard_info_wrapper::set_snapset_missing, shard_errorstream,
			   "candidate had a missing snapset key", error, on_error_fl::stop)) {
	  ss_bl.push_back(snap_it->second);
	  try {
	    auto bliter = ss_bl.cbegin();
	    decode(snapset, bliter);
	  } catch (...) {
	    // invalid snapset, probably corrupt
	    auth_object_check(
	      true, shard_info, &shard_info_wrapper::set_snapset_corrupted, shard_errorstream,
	      "candidate had a corrupt snapset", error, on_error_fl::cont);
	  }
	}
      }
#endif

      // RRR complete erasure-pool-specific checks

      // checking the attrs

      {
	// dump the first two entries
	static int dbg_c{20};
	if (dbg_c-- > 0) {
	  stringstream lb;
	  lb << " =at= " << ho << " ==at== ";
	  for (const auto& [k, v] : smap_obj.attrs) {
	    lb << "\t\tattr: " << k << " <-> " << v << "\n";
	  }

	  logger().debug("{}: -v- obj {} shard:{}: {}", __func__, obj, s, lb.str());
	}
      }

      auto k = smap_obj.attrs.find(OI_ATTR);

      logger().debug("{}: {} {} oi_attr? {}", __func__, obj, ho,
		     ((k == smap_obj.attrs.end()) ? "NOATR" : "found"));

      if (auth_object_check((k == smap_obj.attrs.end()), shard_info,
			    &shard_info_wrapper::set_info_missing, shard_errorstream,
			    "candidate had a missing info key", error,
			    on_error_fl::stop)) {

	// decode the object info
	bufferlist bl;
	object_info_t oi;
	bl.push_back(k->second);

	// make the following into an errorator RRR
	bool should_cont{true};
	try {
	  auto bl_iter = bl.cbegin();
	  decode(oi, bl_iter);
	} catch (...) {
	  // invalid object info, probably corrupt
	  logger().debug(
	    "select_auth_object(): {}: invalid object info, probably corrupt", obj);
	  should_cont = auth_object_check(
	    true, shard_info, &shard_info_wrapper::set_info_corrupted, shard_errorstream,
	    "candidate had a corrupt info", error, on_error_fl::cont);
	}

	if (should_cont) {

	  logger().debug("select_auth_object(): {} should = {}", oi.soid, obj);

	  // RRR do compare sizes
	  // RRR only handling Replicated pool disk-size for now
	  // \todo fix to contain the sizes in the message
	  auth_object_check((smap_obj.size != oi.size), shard_info,
			    &shard_info_wrapper::set_obj_size_info_mismatch, shard_errorstream,
			    "candidate size mismatch", error,
			    on_error_fl::cont);

	  // update digest_match
	  // digest_match will only be true if computed digests are the same
	  if (auth_version != eversion_t() && auth->second->objects[obj].digest_present &&
	      smap_obj.digest_present &&
	      auth->second->objects[obj].digest != smap_obj.digest) {
	    digest_match = false;
	  }

	  logger().debug("select_auth_object(): {}: errs:{} auv:{} oiv:{} cnt:{}/{} ",
	  	obj, shard_info.errors, auth_version, oi.version, dcount(oi), dcount(*auth_oi));

	  if (!shard_info.errors) {
	    // Otherwise - don't use this particular shard due to previous errors
	    // XXX: For now we can't pick one shard for repair and another's object info or snapset

	    if (auth_version == eversion_t{} || oi.version > auth_version ||
		(oi.version == auth_version && dcount(oi) > dcount(*auth_oi))) {

	      logger().debug("select_auth_object(): {}: selected {}", obj, oi.data_digest);
	      auth = shard_s_map;
	      *auth_oi = oi;
	      auth_version = oi.version;
	    }
	  }
	}
      }
    }

    // finished checking this shard

    if (error) {
      errorstream << pgid.pgid << " shard " << s << " soid " << obj << " : "
		  << shard_errorstream.str() << "\n";
    }
  }

  return ll_read_errorator::make_ready_future<map<pg_shard_t, ScrubMap*>::const_iterator>(
    auth);
}

bool PGBackend::maybe_create_new_object(
  ObjectState& os,
  ceph::os::Transaction& txn)
{
  if (!os.exists) {
    ceph_assert(!os.oi.is_whiteout());
    os.exists = true;
    os.oi.new_object();

    txn.touch(coll->get_cid(), ghobject_t{os.oi.soid});
    // TODO: delta_stats.num_objects++
    return false;
  } else if (os.oi.is_whiteout()) {
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
    // TODO: delta_stats.num_whiteouts--
  }
  return true;
}

static bool is_offset_and_length_valid(
  const std::uint64_t offset,
  const std::uint64_t length)
{
  if (const std::uint64_t max = local_conf()->osd_max_object_size;
      offset >= max || length > max || offset + length > max) {
    logger().debug("{} osd_max_object_size: {}, offset: {}, len: {}; "
                   "Hard limit of object size is 4GB",
                   __func__, max, offset, length);
    return false;
  } else {
    return true;
  }
}

seastar::future<> PGBackend::write(
    ObjectState& os,
    const OSDOp& osd_op,
    ceph::os::Transaction& txn,
    osd_op_params_t& osd_op_params)
{
  const ceph_osd_op& op = osd_op.op;
  uint64_t offset = op.extent.offset;
  uint64_t length = op.extent.length;
  bufferlist buf = osd_op.indata;
  if (auto seq = os.oi.truncate_seq;
      seq != 0 && op.extent.truncate_seq < seq) {
    // old write, arrived after trimtrunc
    if (offset + length > os.oi.size) {
      // no-op
      if (offset > os.oi.size) {
	length = 0;
	buf.clear();
      } else {
	// truncate
	auto len = os.oi.size - offset;
	buf.splice(len, length);
	length = len;
      }
    }
  } else if (op.extent.truncate_seq > seq) {
    // write arrives before trimtrunc
    if (os.exists && !os.oi.is_whiteout()) {
      txn.truncate(coll->get_cid(),
                   ghobject_t{os.oi.soid}, op.extent.truncate_size);
      if (op.extent.truncate_size != os.oi.size) {
        os.oi.size = length;
        // TODO: truncate_update_size_and_usage()
        if (op.extent.truncate_size > os.oi.size) {
          osd_op_params.clean_regions.mark_data_region_dirty(os.oi.size,
              op.extent.truncate_size - os.oi.size);
        } else {
          osd_op_params.clean_regions.mark_data_region_dirty(op.extent.truncate_size,
              os.oi.size - op.extent.truncate_size);
        }
      }
    }
    os.oi.truncate_seq = op.extent.truncate_seq;
    os.oi.truncate_size = op.extent.truncate_size;
  }
  maybe_create_new_object(os, txn);
  if (length == 0) {
    if (offset > os.oi.size) {
      txn.truncate(coll->get_cid(), ghobject_t{os.oi.soid}, op.extent.offset);
    } else {
      txn.nop();
    }
  } else {
    txn.write(coll->get_cid(), ghobject_t{os.oi.soid},
	      offset, length, std::move(buf), op.flags);
    os.oi.size = std::max(offset + length, os.oi.size);
  }
  osd_op_params.clean_regions.mark_data_region_dirty(op.extent.offset,
						     op.extent.length);

  return seastar::now();
}

seastar::future<> PGBackend::write_same(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  const ceph_osd_op& op = osd_op.op;
  const uint64_t len = op.writesame.length;
  if (len == 0) {
    return seastar::now();
  }
  if (op.writesame.data_length == 0 ||
      len % op.writesame.data_length != 0 ||
      op.writesame.data_length != osd_op.indata.length()) {
    throw crimson::osd::invalid_argument();
  }
  ceph::bufferlist repeated_indata;
  for (uint64_t size = 0; size < len; size += op.writesame.data_length) {
    repeated_indata.append(osd_op.indata);
  }
  maybe_create_new_object(os, txn);
  txn.write(coll->get_cid(), ghobject_t{os.oi.soid},
            op.writesame.offset, len,
            /*std::move( no move will occur*/repeated_indata/*)*/, op.flags);
  os.oi.size = len;
  osd_op_params.clean_regions.mark_data_region_dirty(op.writesame.offset, len);
  return seastar::now();
}

seastar::future<> PGBackend::writefull(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  const ceph_osd_op& op = osd_op.op;
  if (op.extent.length != osd_op.indata.length()) {
    throw crimson::osd::invalid_argument();
  }

  const bool existing = maybe_create_new_object(os, txn);
  if (existing && op.extent.length < os.oi.size) {
    txn.truncate(coll->get_cid(), ghobject_t{os.oi.soid}, op.extent.length);
    osd_op_params.clean_regions.mark_data_region_dirty(op.extent.length,
	os.oi.size - op.extent.length);
  }
  if (op.extent.length) {
    txn.write(coll->get_cid(), ghobject_t{os.oi.soid}, 0, op.extent.length,
              osd_op.indata, op.flags);
    os.oi.size = op.extent.length;
    osd_op_params.clean_regions.mark_data_region_dirty(0,
	std::max((uint64_t) op.extent.length, os.oi.size));
  }
  return seastar::now();
}

PGBackend::append_errorator::future<> PGBackend::append(
  ObjectState& os,
  OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  const ceph_osd_op& op = osd_op.op;
  if (op.extent.length != osd_op.indata.length()) {
    return crimson::ct_error::invarg::make();
  }
  maybe_create_new_object(os, txn);
  if (op.extent.length) {
    txn.write(coll->get_cid(), ghobject_t{os.oi.soid},
              os.oi.size /* offset */, op.extent.length,
              std::move(osd_op.indata), op.flags);
    os.oi.size += op.extent.length;
    osd_op_params.clean_regions.mark_data_region_dirty(os.oi.size,
                                                       op.extent.length);
  }
  return seastar::now();
}

PGBackend::write_ertr::future<> PGBackend::truncate(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{} object dne, truncate is a no-op", __func__);
    return write_ertr::now();
  }
  const ceph_osd_op& op = osd_op.op;
  if (!is_offset_and_length_valid(op.extent.offset, op.extent.length)) {
    return crimson::ct_error::file_too_large::make();
  }
  if (op.extent.truncate_seq) {
    assert(op.extent.offset == op.extent.truncate_size);
    if (op.extent.truncate_seq <= os.oi.truncate_seq) {
      logger().debug("{} truncate seq {} <= current {}, no-op",
                     __func__, op.extent.truncate_seq, os.oi.truncate_seq);
      return write_ertr::make_ready_future<>();
    } else {
      logger().debug("{} truncate seq {} > current {}, truncating",
                     __func__, op.extent.truncate_seq, os.oi.truncate_seq);
      os.oi.truncate_seq = op.extent.truncate_seq;
      os.oi.truncate_size = op.extent.truncate_size;
    }
  }
  maybe_create_new_object(os, txn);
  if (os.oi.size != op.extent.offset) {
    txn.truncate(coll->get_cid(),
                 ghobject_t{os.oi.soid}, op.extent.offset);
    if (os.oi.size > op.extent.offset) {
      // TODO: modified_ranges.union_of(trim);
      osd_op_params.clean_regions.mark_data_region_dirty(
        op.extent.offset,
	os.oi.size - op.extent.offset);
    } else {
      // os.oi.size < op.extent.offset
      osd_op_params.clean_regions.mark_data_region_dirty(
        os.oi.size,
        op.extent.offset - os.oi.size);
    }
    os.oi.size = op.extent.offset;
    os.oi.clear_data_digest();
  }
  // TODO: truncate_update_size_and_usage()
  // TODO: ctx->delta_stats.num_wr++;
  // ----
  // do no set exists, or we will break above DELETE -> TRUNCATE munging.
  return write_ertr::now();
}

PGBackend::write_ertr::future<> PGBackend::zero(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{} object dne, zero is a no-op", __func__);
    return write_ertr::now();
  }
  const ceph_osd_op& op = osd_op.op;
  if (!is_offset_and_length_valid(op.extent.offset, op.extent.length)) {
    return crimson::ct_error::file_too_large::make();
  }
  assert(op.extent.length);
  txn.zero(coll->get_cid(),
           ghobject_t{os.oi.soid},
           op.extent.offset,
           op.extent.length);
  // TODO: modified_ranges.union_of(zeroed);
  osd_op_params.clean_regions.mark_data_region_dirty(op.extent.offset,
						     op.extent.length);
  // TODO: ctx->delta_stats.num_wr++;
  os.oi.clear_data_digest();
  return write_ertr::now();
}

seastar::future<> PGBackend::create(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  if (os.exists && !os.oi.is_whiteout() &&
      (osd_op.op.flags & CEPH_OSD_OP_FLAG_EXCL)) {
    // this is an exclusive create
    throw crimson::osd::make_error(-EEXIST);
  }

  if (osd_op.indata.length()) {
    // handle the legacy. `category` is no longer implemented.
    try {
      auto p = osd_op.indata.cbegin();
      std::string category;
      decode(category, p);
    } catch (buffer::error&) {
      throw crimson::osd::invalid_argument();
    }
  }
  maybe_create_new_object(os, txn);
  txn.nop();
  return seastar::now();
}

seastar::future<> PGBackend::remove(ObjectState& os,
                                    ceph::os::Transaction& txn)
{
  // todo: snapset
  txn.remove(coll->get_cid(),
	     ghobject_t{os.oi.soid, ghobject_t::NO_GEN, shard});
  os.oi.size = 0;
  os.oi.new_object();
  os.exists = false;
  // todo: update watchers
  if (os.oi.is_whiteout()) {
    os.oi.clear_flag(object_info_t::FLAG_WHITEOUT);
  }
  return seastar::now();
}

seastar::future<std::tuple<std::vector<hobject_t>, hobject_t>>
PGBackend::list_objects(const hobject_t& start, uint64_t limit) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  auto gstart = start.is_min() ? ghobject_t{} : ghobject_t{start, 0, shard};

  logger().debug("RRR debug {} shard:{} start:{}", __func__, shard, gstart);

  return store->list_objects(coll,
                             gstart,
                             ghobject_t::get_max(),
                             limit)
    .then([](auto ret) {
      auto& [gobjects, next] = ret;
      std::vector<hobject_t> objects;

      //logger().debug("RRR debug list gobjects #: {}", gobjects.size());

      boost::copy(gobjects |
        boost::adaptors::filtered([](const ghobject_t& o) {
          if (o.is_pgmeta()) {
            return false;
          } else if (o.hobj.is_temp()) {
            return false;
          } else {
            return o.is_no_gen();
          }
        }) |
        boost::adaptors::transformed([](const ghobject_t& o) {
          return o.hobj;
        }),
        std::back_inserter(objects));

      //logger().debug("RRR debug list objects #: {}", objects.size());
      return seastar::make_ready_future<std::tuple<std::vector<hobject_t>, hobject_t>>(
        std::make_tuple(objects, next.hobj));
    });
}


seastar::future<vector<ghobject_t>> PGBackend::list_range(hobject_t start,
							  hobject_t end,
							  vector<hobject_t>& ls) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  auto gstart = start.is_min() ? ghobject_t{} : ghobject_t{start, 0, shard};
  ghobject_t gend{end, 0, shard};


  logger().debug("RRR debug #p22 {} {} {} ls{:p}", __func__, gstart, gend, (void*)&ls);

  return seastar::do_with(
    //std::move(gstart), ghobject_t{end, 0, shard},
    (start.is_min() ? ghobject_t{} : ghobject_t{start, 0, shard}), ghobject_t{end, 0, shard},
    [this, &ls](auto&& gstart, auto&& gend) mutable {

      logger().debug("RRR debug #p23 PGBackend::list_range {} {}", gstart, gend);
      //logger().debug("RRR debug #p233 PGBackend::list_range collection: {} {:p}", coll->get_cid(), (void*)store);


      return store->list_objects(coll, gstart, gend, INT_MAX)
	//    .then_wrapped([=](auto&& f){
	//      logger().debug("RRR debug 2 in list_range");
	//      if (f.failed())
	//      	(void)f.discard_result();
	//      return std::move(f);
	//    })
	.then([&ls](auto&& ret) mutable { // RRR 2.3

	  //auto& [gobjects, next] = ret;

	  logger().debug("RRR debug list_range xxx");
	  std::vector<ghobject_t>& gobjects = get<0>(ret);
	  ghobject_t next = get<1>(ret);

	  std::vector<ghobject_t> objects;

	  // RRR fix this to not double-walk
	  logger().debug("RRR debug list_range xxxx {}", gobjects.size());

	  boost::copy(
	    gobjects | boost::adaptors::filtered([](const ghobject_t& o) {
	      if (o.is_pgmeta()) {
		return false;
	      } else if (o.hobj.is_temp()) {
		return false;
	      } else {
		return o.is_no_gen();
	      }
	    }) |
	      boost::adaptors::transformed([](const ghobject_t& o) { return o.hobj; }),
	    std::back_inserter(ls));

	  boost::copy(gobjects | boost::adaptors::filtered([](const ghobject_t& o) {
			if (o.is_pgmeta()) {
			  return false;
			} else if (o.hobj.is_temp()) {
			  return false;
			} else {
			  return !o.is_no_gen();
			}
			//}) |
			// boost::adaptors::transformed([](const ghobject_t& o) {
			//  return o.hobj;
		      }),
		      std::back_inserter(objects));
	  logger().debug("RRR debug list_range end ls-sz:{} rollbacks#{} ls{:p}", ls.size(), objects.size(), (void*)(&ls));
	  return seastar::make_ready_future<std::vector<ghobject_t>>(objects);
	});
    });
}


seastar::future<> PGBackend::setxattr(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  if (local_conf()->osd_max_attr_size > 0 &&
      osd_op.op.xattr.value_len > local_conf()->osd_max_attr_size) {
    throw crimson::osd::make_error(-EFBIG);
  }

  const auto max_name_len = std::min<uint64_t>(
    store->get_max_attr_name_length(), local_conf()->osd_max_attr_name_len);
  if (osd_op.op.xattr.name_len > max_name_len) {
    throw crimson::osd::make_error(-ENAMETOOLONG);
  }

  maybe_create_new_object(os, txn);

  std::string name{"_"};
  ceph::bufferlist val;
  {
    auto bp = osd_op.indata.cbegin();
    bp.copy(osd_op.op.xattr.name_len, name);
    bp.copy(osd_op.op.xattr.value_len, val);
  }
  logger().debug("setxattr on obj={} for attr={}", os.oi.soid, name);

  txn.setattr(coll->get_cid(), ghobject_t{os.oi.soid}, name, val);
  return seastar::now();
  //ctx->delta_stats.num_wr++;
}

PGBackend::get_attr_errorator::future<> PGBackend::getxattr(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  std::string name;
  ceph::bufferlist val;
  {
    auto bp = osd_op.indata.cbegin();
    std::string aname;
    bp.copy(osd_op.op.xattr.name_len, aname);
    name = "_" + aname;
  }
  logger().debug("getxattr on obj={} for attr={}", os.oi.soid, name);
  return getxattr(os.oi.soid, name).safe_then([&osd_op] (ceph::bufferptr val) {
    osd_op.outdata.clear();
    osd_op.outdata.push_back(std::move(val));
    osd_op.op.xattr.value_len = osd_op.outdata.length();
    return get_attr_errorator::now();
    //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  });
  //ctx->delta_stats.num_rd++;
}

PGBackend::get_attr_errorator::future<ceph::bufferptr> PGBackend::getxattr(
  const hobject_t& soid,
  std::string_view key) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  return store->get_attr(coll, ghobject_t{soid}, key);
}

PGBackend::get_attr_errorator::future<> PGBackend::get_xattrs(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  return store->get_attrs(coll, ghobject_t{os.oi.soid}).safe_then(
    [&osd_op](auto&& attrs) {
    std::vector<std::pair<std::string, bufferlist>> user_xattrs;
    for (auto& [key, val] : attrs) {
      if (key.size() > 1 && key[0] == '_') {
	ceph::bufferlist bl;
	bl.append(std::move(val));
	user_xattrs.emplace_back(key.substr(1), std::move(bl));
      }
    }
    ceph::encode(user_xattrs, osd_op.outdata);
    return get_attr_errorator::now();
  });
}

PGBackend::rm_xattr_ertr::future<> PGBackend::rm_xattr(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: {} DNE", __func__, os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  auto bp = osd_op.indata.cbegin();
  string attr_name{"_"};
  bp.copy(osd_op.op.xattr.name_len, attr_name);
  txn.rmattr(coll->get_cid(), ghobject_t{os.oi.soid}, attr_name);
  return rm_xattr_ertr::now();
}

using get_omap_ertr =
  crimson::os::FuturizedStore::read_errorator::extend<
    crimson::ct_error::enodata>;
static
get_omap_ertr::future<
  crimson::os::FuturizedStore::omap_values_t>
maybe_get_omap_vals_by_keys(
  crimson::os::FuturizedStore* store,
  const crimson::os::CollectionRef& coll,
  const object_info_t& oi,
  const std::set<std::string>& keys_to_get)
{
  if (oi.is_omap()) {
    return store->omap_get_values(coll, ghobject_t{oi.soid}, keys_to_get);
  } else {
    return crimson::ct_error::enodata::make();
  }
}

static
get_omap_ertr::future<
  std::tuple<bool, crimson::os::FuturizedStore::omap_values_t>>
maybe_get_omap_vals(
  crimson::os::FuturizedStore* store,
  const crimson::os::CollectionRef& coll,
  const object_info_t& oi,
  const std::string& start_after)
{
  if (oi.is_omap()) {
    return store->omap_get_values(coll, ghobject_t{oi.soid}, start_after);
  } else {
    return crimson::ct_error::enodata::make();
  }
}

PGBackend::ll_read_errorator::future<ceph::bufferlist>
PGBackend::omap_get_header(
  const crimson::os::CollectionRef& c,
  const ghobject_t& oid) const
{
  return store->omap_get_header(c, oid);
}

PGBackend::ll_read_errorator::future<>
PGBackend::omap_get_header(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  return omap_get_header(coll, ghobject_t{os.oi.soid}).safe_then(
    [&osd_op] (ceph::bufferlist&& header) {
      osd_op.outdata = std::move(header);
      return seastar::now();
    });
}

PGBackend::ll_read_errorator::future<>
PGBackend::omap_get_keys(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: object does not exist: {}", os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  std::string start_after;
  uint64_t max_return;
  try {
    auto p = osd_op.indata.cbegin();
    decode(start_after, p);
    decode(max_return, p);
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }
  max_return =
    std::min(max_return, local_conf()->osd_max_omap_entries_per_request);

  // TODO: truly chunk the reading
  return maybe_get_omap_vals(store, coll, os.oi, start_after).safe_then(
    [=, &osd_op](auto ret) {
      ceph::bufferlist result;
      bool truncated = false;
      uint32_t num = 0;
      for (auto &[key, val] : std::get<1>(ret)) {
        if (num >= max_return ||
            result.length() >= local_conf()->osd_max_omap_bytes_per_request) {
          truncated = true;
          break;
        }
        encode(key, result);
        ++num;
      }
      encode(num, osd_op.outdata);
      osd_op.outdata.claim_append(result);
      encode(truncated, osd_op.outdata);
      return seastar::now();
    }).handle_error(
      crimson::ct_error::enodata::handle([&osd_op] {
        uint32_t num = 0;
	bool truncated = false;
	encode(num, osd_op.outdata);
	encode(truncated, osd_op.outdata);
	return seastar::now();
      }),
      ll_read_errorator::pass_further{}
    );
  // TODO:
  //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  //ctx->delta_stats.num_rd++;
}

PGBackend::ll_read_errorator::future<>
PGBackend::omap_get_vals(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }

  std::string start_after;
  uint64_t max_return;
  std::string filter_prefix;
  try {
    auto p = osd_op.indata.cbegin();
    decode(start_after, p);
    decode(max_return, p);
    decode(filter_prefix, p);
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  max_return = \
    std::min(max_return, local_conf()->osd_max_omap_entries_per_request);

  // TODO: truly chunk the reading
  return maybe_get_omap_vals(store, coll, os.oi, start_after).safe_then(
    [=, &osd_op] (auto&& ret) {
      auto [done, vals] = std::move(ret);
      assert(done);
      ceph::bufferlist result;
      bool truncated = false;
      uint32_t num = 0;
      auto iter = filter_prefix > start_after ? vals.lower_bound(filter_prefix)
                                              : std::begin(vals);
      for (; iter != std::end(vals); ++iter) {
        const auto& [key, value] = *iter;
        if (key.substr(0, filter_prefix.size()) != filter_prefix) {
          break;
        } else if (num >= max_return ||
            result.length() >= local_conf()->osd_max_omap_bytes_per_request) {
          truncated = true;
          break;
        }
        encode(key, result);
        encode(value, result);
        ++num;
      }
      encode(num, osd_op.outdata);
      osd_op.outdata.claim_append(result);
      encode(truncated, osd_op.outdata);
      return ll_read_errorator::now();
    }).handle_error(
      crimson::ct_error::enodata::handle([&osd_op] {
        encode(uint32_t{0} /* num */, osd_op.outdata);
        encode(bool{false} /* truncated */, osd_op.outdata);
        return ll_read_errorator::now();
      }),
      ll_read_errorator::pass_further{}
    );

  // TODO:
  //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  //ctx->delta_stats.num_rd++;
}

PGBackend::ll_read_errorator::future<>
PGBackend::omap_get_vals_by_keys(
  const ObjectState& os,
  OSDOp& osd_op) const
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: object does not exist: {}", os.oi.soid);
    return crimson::ct_error::enoent::make();
  }

  std::set<std::string> keys_to_get;
  try {
    auto p = osd_op.indata.cbegin();
    decode(keys_to_get, p);
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument();
  }
  return maybe_get_omap_vals_by_keys(store, coll, os.oi, keys_to_get).safe_then(
    [&osd_op] (crimson::os::FuturizedStore::omap_values_t&& vals) {
      encode(vals, osd_op.outdata);
      return ll_read_errorator::now();
    }).handle_error(
      crimson::ct_error::enodata::handle([&osd_op] {
        uint32_t num = 0;
        encode(num, osd_op.outdata);
        return ll_read_errorator::now();
      }),
      ll_read_errorator::pass_further{}
    );

  // TODO:
  //ctx->delta_stats.num_rd_kb += shift_round_up(osd_op.outdata.length(), 10);
  //ctx->delta_stats.num_rd++;
}

seastar::future<> PGBackend::omap_set_vals(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  maybe_create_new_object(os, txn);

  ceph::bufferlist to_set_bl;
  try {
    auto p = osd_op.indata.cbegin();
    decode_str_str_map_to_bl(p, &to_set_bl);
  } catch (buffer::error&) {
    throw crimson::osd::invalid_argument{};
  }

  txn.omap_setkeys(coll->get_cid(), ghobject_t{os.oi.soid}, to_set_bl);

  // TODO:
  //ctx->clean_regions.mark_omap_dirty();

  // TODO:
  //ctx->delta_stats.num_wr++;
  //ctx->delta_stats.num_wr_kb += shift_round_up(to_set_bl.length(), 10);
  os.oi.set_flag(object_info_t::FLAG_OMAP);
  os.oi.clear_omap_digest();
  osd_op_params.clean_regions.mark_omap_dirty();
  return seastar::now();
}

seastar::future<> PGBackend::omap_set_header(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  maybe_create_new_object(os, txn);
  txn.omap_setheader(coll->get_cid(), ghobject_t{os.oi.soid}, osd_op.indata);
  //TODO:
  //ctx->clean_regions.mark_omap_dirty();
  //ctx->delta_stats.num_wr++;
  os.oi.set_flag(object_info_t::FLAG_OMAP);
  os.oi.clear_omap_digest();
  return seastar::now();
}

seastar::future<> PGBackend::omap_remove_range(
  ObjectState& os,
  const OSDOp& osd_op,
  ceph::os::Transaction& txn)
{
  std::string key_begin, key_end;
  try {
    auto p = osd_op.indata.cbegin();
    decode(key_begin, p);
    decode(key_end, p);
  } catch (buffer::error& e) {
    throw crimson::osd::invalid_argument{};
  }
  txn.omap_rmkeyrange(coll->get_cid(), ghobject_t{os.oi.soid}, key_begin, key_end);
  //TODO:
  //ctx->delta_stats.num_wr++;
  os.oi.clear_omap_digest();
  return seastar::now();
}

PGBackend::omap_clear_ertr::future<>
PGBackend::omap_clear(
  ObjectState& os,
  OSDOp& osd_op,
  ceph::os::Transaction& txn,
  osd_op_params_t& osd_op_params)
{
  if (__builtin_expect(stopping, false)) {
    throw crimson::common::system_shutdown_exception();
  }
  if (!os.exists || os.oi.is_whiteout()) {
    logger().debug("{}: object does not exist: {}", os.oi.soid);
    return crimson::ct_error::enoent::make();
  }
  if (!os.oi.is_omap()) {
    return omap_clear_ertr::now();
  }
  txn.omap_clear(coll->get_cid(), ghobject_t{os.oi.soid});
  osd_op_params.clean_regions.mark_omap_dirty();
  os.oi.clear_omap_digest();
  os.oi.clear_flag(object_info_t::FLAG_OMAP);
  return omap_clear_ertr::now();
}

seastar::future<struct stat> PGBackend::stat(
  CollectionRef c,
  const ghobject_t& oid) const
{
  return store->stat(c, oid);
}

seastar::future<std::map<uint64_t, uint64_t>>
PGBackend::fiemap(
  CollectionRef c,
  const ghobject_t& oid,
  uint64_t off,
  uint64_t len)
{
  return store->fiemap(c, oid, off, len);
}

void PGBackend::on_activate_complete() {
  peering.reset();
}

