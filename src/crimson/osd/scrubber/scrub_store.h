// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

//#include "os/ObjectStore.h"
#include <map>
#include <string_view>

#include "crimson/common/sharedptr_registry.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/scrubber/scrub_map_cacher.h"
#include "crimson/osd/scrubber/scrub_os_driver.h"
#include "os/Transaction.h"


namespace librados {
struct object_id_t;
}

struct inconsistent_obj_wrapper;
struct inconsistent_snapset_wrapper;

namespace crimson::osd::Scrub {

/*
 * The scrub store manages two sets of objects-with-errors: one for snap errors, and
 * a second for object inconsistencies. The lists are saved to disk as part of the
 * database (as a separate collection) (but not replicated).
 * Note that librados has an API to directly access this data. Thus - it should not
 * be scrapped without discussion.
 */
class Store {
 public:
  using BuffersVec = std::vector<ceph::bufferlist>;

  // using sstore_errorator =
  //  crimson::errorator<crimson::ct_error::enoent,
  //  crimson::ct_error::input_output_error>;

  using sstore_errorator = ::crimson::os::FuturizedStore::read_errorator;

  using ErrtBuffersVec = sstore_errorator::future<BuffersVec>;

  ~Store() {}

  static Store* create(crimson::os::FuturizedStore* store,
		       ceph::os::Transaction* t,
		       const spg_t& pgid,
		       const coll_t& coll);

  void add_object_error(int64_t pool, const inconsistent_obj_wrapper& e);

  void add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e);

  bool empty() const
  {
    return results.empty();
  }

  seastar::future<> flush(ceph::os::Transaction*);

  void cleanup(ceph::os::Transaction*);

  Store::ErrtBuffersVec get_snap_errors(int64_t pool,
					const librados::object_id_t& start,
					uint64_t max_return) /*const*/;

  Store::ErrtBuffersVec get_object_errors(int64_t pool,
					  const librados::object_id_t& start,
					  uint64_t max_return) /*const*/;

 private:
  Store(const coll_t& coll, const ghobject_t& oid, crimson::os::FuturizedStore* store);

  // std::vector<ceph::buffer::list> get_errors(const string& start,
  //					     const string& end,
  //					     int64_t max_return) const;

  ErrtBuffersVec get_errors(const std::string& start,
			    const std::string& end,
			    int64_t max_return) /*const*/;

 private:
  const coll_t coll;
  const ghobject_t hoid;
  // a temp object holding mappings from seq-id to inconsistencies found in
  // scrubbing
  ::crimson::osd::OSDriver driver;
  // mutable ::crimson::osd::MapCacher::MapCacher<std::string, ::ceph::bufferlist>
  // backend;
  /*mutable*/ ::crimson::osd::MapCacher::MapCacher backend;
  std::map<std::string, ::ceph::bufferlist> results;

  // using OptKV = std::optional<std::pair<std::string, ::ceph::bufferlist>>;
};

class StoreCreator {

  // RRR make this a seastar uniqp?
  seastar::shared_promise<Store*> m_store;

  /// send the transaction that will create the new store
  // void create();
};


}  // namespace crimson::osd::Scrub
