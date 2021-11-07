// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>

#include "crimson/common/sharedptr_registry.h"
#include "crimson/os/futurized_store.h"
#include "os/Transaction.h"

//  --------------------------------------------------------
//
//  RRR a primitive MapCacher for the scrubber.
//
//  --------------------------------------------------------

namespace crimson::osd::MapCacher {

using k_to_vlist_t = std::map<std::string, ::ceph::bufferlist, std::less<void>>;
using future_k_to_vlist_t = seastar::future<k_to_vlist_t>;


/**
 * Abstraction for ordering key updates
 */
class Transaction {
 public:
  /// Std::set keys according to map
  virtual void set_keys(const std::map<std::string, ::ceph::bufferlist>&
			  keys	///< [in] keys/values to std::set
			) = 0;

  /// Remove keys
  virtual void remove_keys(
    const std::set<std::string>& to_remove  ///< [in] keys to remove
    ) = 0;

  /// Add context to fire when data is readable
  virtual void add_callback(Context* c	///< [in] Context to fire on readable
			    ) = 0;

  virtual ~Transaction() = default;
  // Transaction(Transaction&& o) noexcept {};
};

/**
 * Abstraction for fetching keys
 */
class StoreDriver {
 public:
  /// Returns requested key values
  virtual os::FuturizedStore::read_errorator::future<k_to_vlist_t> get_keys(
    const std::set<std::string>& keys /*(,	 ///< [in] keys requested
std::map<K, ::ceph::bufferlist>* got	 ///< [out] values for keys obtained
*/) = 0;			      ///< @return error value

  /// Returns next key
  virtual auto get_next(const std::string& key)
    -> os::FuturizedStore::read_errorator::future<
      std::optional<std::pair<std::string, ::ceph::bufferlist>>> = 0;

  //  virtual int get_next(const K& key,	      ///< [in] key after which to get
  //  next
  //		       std::pair<K, ::ceph::bufferlist>* next  ///< [out] first key after
  // key
  //  ) = 0;  ///< @return 0 on success, -ENOENT if there is no next

  virtual ~StoreDriver() = default;
};


/**
 * Uses SharedPtrRegistry to cache objects of in progress writes
 * allowing the user to read/write a consistent view of the map
 * without flushing writes.
 */
class MapCacher {
  StoreDriver* driver;

  ::crimson::osd::SharedPtrRegistry<std::string, boost::optional<::ceph::bufferlist>>
    in_progress;
  typedef
    typename SharedPtrRegistry<std::string, boost::optional<::ceph::bufferlist>>::VPtr
      VPtr;
  typedef ContainerContext<std::set<VPtr>> TransHolder;

 public:
  explicit MapCacher(StoreDriver* driver) : driver(driver) {}

  // a helper for the 'in_progress' object


  using KVOptPair = std::optional<std::pair<std::string, ::ceph::bufferlist>>;
  using KVOptPairOpt =
    std::optional<std::pair<std::string, boost::optional<::ceph::bufferlist>>>;


  /// Fetch first key/value std::pair after specified key
  auto get_next(
    std::string key /*,		      ///< [in] key after which to get next
	   std::pair<std::string, ::ceph::bufferlist>* next  ///< [out] next key
*/) -> os::FuturizedStore::read_errorator::future<KVOptPair>;

  /// Adds operation setting keys to Transaction
  void set_keys(const std::map<std::string, ::ceph::bufferlist>&
		  keys,		///< [in] keys/values to std::set
		Transaction* t	///< [out] transaction to use
  );


  /// Adds operation removing keys to Transaction
  void remove_keys(const std::set<std::string>& keys,  ///< [in]
		   Transaction* t		       ///< [out] transaction to use
  );

  /// Gets keys, uses cached values for unstable keys
  /// Might return nullopt on store access error.
  auto get_keys(const std::set<std::string>&
		  keys_to_get  ///< [in] std::set of keys to fetch
			       // std::map<std::string, ::ceph::bufferlist>* got
			       // ///< [out] keys gotten
		)
    -> os::FuturizedStore::read_errorator::future<
      std::optional<std::map<std::string, ::ceph::bufferlist>>>;

};


#if 0


/**
 * Uses SharedPtrRegistry to cache objects of in progress writes
 * allowing the user to read/write a consistent view of the map
 * without flushing writes.
 */
template <typename K, typename V> class MapCacher {
 private:
  StoreDriver<K, V>* driver;

  ::crimson::osd::SharedPtrRegistry<K, boost::optional<V>> in_progress;
  typedef typename SharedPtrRegistry<K, boost::optional<V>>::VPtr VPtr;
  typedef ContainerContext<std::set<VPtr>> TransHolder;

 public:
  explicit MapCacher(StoreDriver<K, V>* driver) : driver(driver) {}

  // a helper for the 'in_progress' object


  using KVOptPair = std::optional<std::pair<K, V>>;
  using KVOptPairOpt = std::optional<std::pair<K, boost::optional<V>>>;




};

#if 0
  /// Fetch first key/value std::pair after specified key
  int get_next(K key,		      ///< [in] key after which to get next
	       std::pair<K, V>* next  ///< [out] next key
  )
  {
    while (true) {
      std::pair<K, boost::optional<V>> cached;
      std::pair<K, V> store;
      bool got_cached = in_progress.get_next(key, &cached);

      bool got_store = false;
      int r = driver->get_next(key, &store);
      if (r < 0 && r != -ENOENT) {
	return r;
      } else if (r == 0) {
	got_store = true;
      }

      if (!got_cached && !got_store) {
	return -ENOENT;
      } else if (got_cached && (!got_store || store.first >= cached.first)) {
	if (cached.second) {
	  if (next)
	    *next = make_pair(cached.first, cached.second.get());
	  return 0;
	} else {
	  key = cached.first;
	  continue;  // value was cached as removed, recurse
	}
      } else {
	if (next)
	  *next = store;
	return 0;
      }
    }
    ceph_abort();  // not reachable
    return -EINVAL;
  }  ///< @return error value, 0 on success, -ENOENT if no more entries
#endif

  using KVOptPair = std::optional<std::pair<K, V>>;
  using KVOptPairOpt = std::optional<std::pair<K, boost::optional<V>>>;

#if 0
  /// Fetch first key/value std::pair after specified key
  auto get_nextZ(K key /*,		      ///< [in] key after which to get next
		       std::pair<K, V>* next  ///< [out] next key
	  */) -> os::FuturizedStore::read_errorator::future<KVOptPair>
  {
    return do_with(
      std::pair<K, boost::optional<V>>{},
      [this, key](auto&& cached,
		  auto&& store) -> os::FuturizedStore::read_errorator::future<KVOptPair> {
	// in the 'do_with' block


	bool got_cached = in_progress.get_next(key, &cached);
	return driver->get_next(key).safe_then(
	  [this, got_cached, cached, key](auto&& kv_from_store)
	    -> os::FuturizedStore::read_errorator::future<KVOptPair> {
	    // get_next() got us an optional pair <str, buflist>, or an error

	    bool got_store = kv_from_store.has_value();

	    // todo handle error in get_next() RRRR normal path!!!

	    if (!got_cached && !got_store) {
	      return crimson::ct_error::enoent::make();
	    }

	    if (got_cached && (!got_store || kv_from_store->first >= cached.first)) {
	      if (cached.second) {
		return seastar::make_ready_future(
		  std::pair(cached.first, cached.second.get()));
	      }

	      // value was cached as removed. recurse
	      return get_next(cached.first);

	    } else {

	      return kv_from_store.get_value();
	    }
	  });
      });
  }
#endif

  /// Fetch first key/value std::pair after specified key
  auto get_next(const K key /*,		      ///< [in] key after which to get next
		       std::pair<K, V>* next  ///< [out] next key
	  */) -> os::FuturizedStore::read_errorator::future<KVOptPair>
  {
    KVOptPairOpt kv = in_progress.get_next(key);
    return seastar::do_with(std::move(kv),
      //in_progress.get_next(key).then(
      [this, key](auto&& in_transit/*,
		  auto&& store*/) mutable -> os::FuturizedStore::read_errorator::future<KVOptPair> {
	// in the 'do_with' block

	bool got_cached = in_transit.has_value();

	return driver->get_next(key).safe_then(
	  [this, got_cached, cached=*in_transit, key](auto&& kv_from_store) mutable
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
		  std::optional{std::pair<K, V>(cached.first, cached.second.get())});
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

  /// Adds operation setting keys to Transaction
  void set_keys(const std::map<K, V>& keys,  ///< [in] keys/values to std::set
		Transaction<K, V>* t	     ///< [out] transaction to use
  )
  {
    std::set<VPtr> vptrs;
//    for (auto i = keys.begin(); i != keys.end(); ++i) {
//      VPtr ip = in_progress.lookup_or_create(i->first, i->second);
//      *ip = i->second;
//      vptrs.insert(ip);
//    }

    for (auto& [k, v] : keys) {
      VPtr ip = in_progress.lookup_or_create(k, v);
      *ip = v; // RRR strange lookup_or_create behaviour??
      vptrs.insert(ip);
    }

    t->set_keys(keys);
    t->add_callback(new TransHolder(vptrs));
  }

  /// Adds operation removing keys to Transaction
  void remove_keys(const std::set<K>& keys,  ///< [in]
		   Transaction<K, V>* t	     ///< [out] transaction to use
  )
  {
    std::set<VPtr> vptrs;
    for (auto i = keys.begin(); i != keys.end(); ++i) {
      boost::optional<V> empty;
      VPtr ip = in_progress.lookup_or_create(*i, empty);
      *ip = empty;
      vptrs.insert(ip);
    }
    t->remove_keys(keys);
    t->add_callback(new TransHolder(vptrs));
  }

  /// Gets keys, uses cached values for unstable keys
  int get_keys(const std::set<K>& keys_to_get,	///< [in] std::set of keys to fetch
	       std::map<K, V>* got		///< [out] keys gotten
  )
  {
    std::set<K> to_get;
    std::map<K, V> _got;
    for (auto i = keys_to_get.begin(); i != keys_to_get.end(); ++i) {
      VPtr val = in_progress.lookup(*i);
      if (val) {
	if (*val)
	  got->insert(make_pair(*i, val->get()));
	// else: value cached is empty, key doesn't exist
      } else {
	to_get.insert(*i);
      }
    }
    int r = driver->get_keys(to_get, &_got);
    if (r < 0)
      return r;

    got->merge(_got);
    return 0;
  }  ///< @return error value, 0 on success
};

#endif
}  // namespace crimson::osd::MapCacher
