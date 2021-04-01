// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/scrubber/scrub_os_driver.h"

using ::std::string;
using ::std::vector;


namespace crimson::osd {


// the comment is copied from the original SnapMapper.cc


const string LEGACY_MAPPING_PREFIX = "MAP_";
const string MAPPING_PREFIX = "SNA_";
const string OBJECT_PREFIX = "OBJ_";

const char* PURGED_SNAP_PREFIX = "PSN_";

/*

  We have a bidirectional mapping, (1) from each snap+obj to object,
  sorted by snapshot, such that we can enumerate to identify all clones
  mapped to a particular snapshot, and (2) from object to snaps, so we
  can identify which reverse mappings exist for any given object (and,
  e.g., clean up on deletion).

  "MAP_"
  + ("%016x" % snapid)
  + "_"
  + (".%x" % shard_id)
  + "_"
  + hobject_t::to_str() ("%llx.%8x.%lx.name...." % pool, hash, snap)
  -> SnapMapping::Mapping { snap, hoid }

  "SNA_"
  + ("%lld" % poolid)
  + "_"
  + ("%016x" % snapid)
  + "_"
  + (".%x" % shard_id)
  + "_"
  + hobject_t::to_str() ("%llx.%8x.%lx.name...." % pool, hash, snap)
  -> SnapMapping::Mapping { snap, hoid }

  "OBJ_" +
  + (".%x" % shard_id)
  + hobject_t::to_str()
   -> SnapMapper::object_snaps { oid, set<snapid_t> }

  */

os::FuturizedStore::read_errorator::future<MapCacher::k_to_vlist_t> OSDriver::get_keys(
  const std::set<std::string>& keys)
{
  // seastar::future<crimson::os::FuturizedStore::CollectionRef> c{get_collection()};

  return get_collection().then([this, keys](auto&& clct) /*-> MapCacher::future_k_to_vlist_t*/ {
    return os->omap_get_values(clct.get(), hoid, keys);
  });

//  return get_collection().then([this, keys](auto&& clct) -> MapCacher::future_k_to_vlist_t {
//    return os->omap_get_values(clct.get(), hoid, keys).safe_then( // break here                 s
//    	[keys](crimson::os::FuturizedStore::omap_values_t&& vals) -> MapCacher::future_k_to_vlist_t {
//	  return seastar::make_ready_future<MapCacher::k_to_vlist_t>(std::move(vals));
//	});
//  });
}

seastar::future<std::unique_ptr<OSDriver::OSTransaction>> OSDriver::get_transaction(::ceph::os::Transaction* t)
{
  return get_collection().then( //                   - break
    [this, t](auto&& cl) -> seastar::future<std::unique_ptr<OSDriver::OSTransaction>> {
    	return seastar::make_ready_future<std::unique_ptr<OSDriver::OSTransaction>>(
	  std::make_unique<OSDriver::OSTransaction>(cl->get_cid(), hoid, t));
    });
}

auto OSDriver::get_next(const std::string& key)
  -> os::FuturizedStore::read_errorator::future<OptPair_KVlist>
{
  return get_collection().then(
    [this,
     key](auto&& clct) -> os::FuturizedStore::read_errorator::future<OptPair_KVlist> {
      return os->get_omap_iterator(clct, hoid)
	.then([this, clct, key](
		auto mit) mutable -> os::FuturizedStore::read_errorator::future<OptPair_KVlist> {
	  if (!mit->valid()) {
	    ceph_abort();
	    return crimson::ct_error::enoent::make();
	  }

	  return mit->upper_bound(key).then(
	    [this, key,
	     mit]() -> os::FuturizedStore::read_errorator::future<OptPair_KVlist> {
	      if (!mit->valid()) {
		return seastar::make_ready_future<OptPair_KVlist>(std::nullopt);
	      }
	      return os::FuturizedStore::read_errorator::make_ready_future<OptPair_KVlist>(std::pair(mit->key(), mit->value()));
	    });
	});
    });
}

#if 0
int OSDriver::get_keys(const std::set<std::string>& keys,
		       std::map<std::string, bufferlist>* out)
{
  std::ignore = get_collection().then([this](auto&& cl) { return seastar::now(); });
  return os->omap_get_values(cl, hoid, keys);
}


int OSDriver::get_next(
  const std::string &key,
  pair<std::string, bufferlist> *next)
{
  ObjectMap::ObjectMapIterator iter =
    os->get_omap_iterator(ch, hoid);
  if (!iter) {
    ceph_abort();
    return -EINVAL;
  }
  iter->upper_bound(key);
  if (iter->valid()) {
    if (next)
      *next = make_pair(iter->key(), iter->value());
    return 0;
  } else {
    return -ENOENT;
  }
}
#endif

#ifdef NOT_NEEDED_NOW
seastar::future<::crimson::os::FuturizedStore::omap_values_t> test333(
  OSDriver* e,
  const std::set<std::string>& keys,
  std::map<std::string, bufferlist>* out,
  ::crimson::os::FuturizedStore* os)
{

  return e->get_collection().then([e, keys, os](auto&& clct) {
    return ::crimson::os::FuturizedStore::omap_values_t{};

    // return os->omap_get_values(clct.get() , e->hoid, keys).then(
    //	[e]
  });
}
#endif

}  // namespace crimson::osd

