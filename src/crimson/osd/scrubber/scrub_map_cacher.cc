#include "./scrub_map_cacher.h"

namespace crimson::osd::MapCacher {


/// Fetch first key/value std::pair after specified key
auto MapCacher::get_next(
  std::string key /*,		      ///< [in] key after which to get next
	   std::pair<std::string, ::ceph::bufferlist>* next  ///< [out] next key
*/) -> os::FuturizedStore::read_errorator::future<KVOptPair>
{
  KVOptPairOpt kv = in_progress.get_next(key);
  return seastar::do_with(
    std::move(kv),
    // in_progress.get_next(key).then(
    [this, key](auto&& in_transit /*,
auto&& store*/) mutable -> os::FuturizedStore::read_errorator::future<KVOptPair> {
      // in the 'do_with' block

      bool got_cached = in_transit.has_value();

      return driver->get_next(key).safe_then(
	[this, got_cached, cached = *in_transit, key](auto&& kv_from_store) mutable
	-> os::FuturizedStore::read_errorator::future<KVOptPair> {
	  // get_next() got us an optional pair <str, buflist>, or an error

	  bool got_store = kv_from_store.has_value();

	  // todo handle error in get_next() RRRR normal path!!!

	  if (!got_cached && !got_store) {
	    return crimson::ct_error::enoent::make();
	  }

	  if (got_cached && (!got_store || kv_from_store->first >= cached.first)) {
	    if (cached.second) {
	      return os::FuturizedStore::read_errorator::make_ready_future<KVOptPair>(
		std::optional{std::pair<std::string, ::ceph::bufferlist>(
		  cached.first, cached.second.get())});
	    }

	    // value was cached as removed. recurse
	    return get_next(cached.first);

	  } else {

	    return os::FuturizedStore::read_errorator::make_ready_future<KVOptPair>(
	      kv_from_store.value());
	  }
	});
    });
}


/// Gets keys, uses cached values for unstable keys
/// Might return nullopt on store access error.
auto MapCacher::get_keys(const std::set<std::string>&
keys_to_get  ///< [in] std::set of keys to fetch
  // std::map<std::string, ::ceph::bufferlist>* got
  // ///< [out] keys gotten
)
-> os::FuturizedStore::read_errorator::future<
  std::optional<std::map<std::string, ::ceph::bufferlist>>>
{
  std::set<std::string> to_get;
  std::map<std::string, ::ceph::bufferlist> got;

  for (auto i = keys_to_get.begin(); i != keys_to_get.end(); ++i) {
    VPtr val = in_progress.lookup(*i);
    if (val) {
      if (*val)
	got.insert(make_pair(*i, val->get()));
      // else: value cached is empty, key doesn't exist
    } else {
      to_get.insert(*i);
    }
  }

  return driver->get_keys(to_get).safe_then(
    [&got](auto&& from_store) {
      got.merge(from_store);
      // return os::FuturizedStore::read_errorator::make_ready_future(std::optional(
      // std::move(got));
      return (std::optional(std::move(got)));
    } /**/,
    // for now - stop the error here
    os::FuturizedStore::read_errorator::all_same_way([]() {
      // RRR complete the error handling here

      return seastar::make_ready_future<
	std::optional<std::map<std::string, ::ceph::bufferlist>>>(
	std::optional<std::map<std::string, ::ceph::bufferlist>>(std::nullopt));
    }) /**/);
}


void MapCacher::set_keys(const std::map<std::string, ::ceph::bufferlist>&
keys,		///< [in] keys/values to std::set
	      Transaction* t	///< [out] transaction to use
)
{
  std::set<VPtr> vptrs;
      for (auto i = keys.begin(); i != keys.end(); ++i) {
        VPtr ip = in_progress.lookup_or_create(i->first, i->second);
        *ip = i->second;
        vptrs.insert(ip);
      }

//  for (auto& [k, v] : keys) {
//    VPtr ip = in_progress.lookup_or_create(k, v);
//    *ip = v;	// RRR strange lookup_or_create behaviour??
//    vptrs.insert(ip);
//  }

  t->set_keys(keys);
  t->add_callback(new TransHolder(vptrs));
}


/// Adds operation removing keys to Transaction
void MapCacher::remove_keys(const std::set<std::string>& keys,  ///< [in]
		 Transaction* t		       ///< [out] transaction to use
)
{
  std::set<VPtr> vptrs;
  for (auto i = keys.begin(); i != keys.end(); ++i) {
    boost::optional<::ceph::bufferlist> empty;
    VPtr ip = in_progress.lookup_or_create(*i, empty);
    *ip = empty;
    vptrs.insert(ip);
  }
  t->remove_keys(keys);
  t->add_callback(new TransHolder(vptrs));
}



}  // namespace crimson::osd::MapCacher
