// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/scrubber/scrub_store.h"

#include <utility>

#include "common/scrub_types.h"
#include "crimson/osd/scrubber/scrub_os_driver.h"
#include "include/rados/rados_types.hpp"
#include "common/scrub_types.h"
#include "osd/osd_types.h"

using std::string;
using std::ostringstream;
using std::vector;

namespace {
ghobject_t make_scrub_object(const spg_t& pgid)
{
  ostringstream ss;
  ss << "scrub_" << pgid;
  return pgid.make_temp_ghobject(ss.str());
}

string first_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(), "", 0, 0x00000000, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

// the object_key should be unique across pools
string to_object_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(object_t(oid.name),
			oid.locator,  // key
			oid.snap,
			0,  // hash
			pool, oid.nspace);
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string last_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(), "", 0, 0xffffffff, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // the representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  auto hoid = hobject_t(object_t(), "", 0, 0x00000000, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string to_snap_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(object_t(oid.name),
			oid.locator,  // key
			oid.snap,
			0x77777777,  // hash
			pool, oid.nspace);
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string last_snap_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(), "", 0, 0xffffffff, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}
}  // namespace

namespace crimson::osd::Scrub {

Store* Store::create(crimson::os::FuturizedStore* store,
		     ceph::os::Transaction* t,
		     const spg_t& pgid,
		     const coll_t& coll)
{
  ceph_assert(store);
  ceph_assert(t);
  ghobject_t oid = make_scrub_object(pgid);
  t->touch(coll, oid);
  return new Store{coll, oid, store};
}

Store::Store(const coll_t& coll,
	     const ghobject_t& oid,
	     crimson::os::FuturizedStore* store)
    : coll{coll}, hoid{oid}, driver{store, coll, hoid}, backend{&driver}
{}

void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  bufferlist bl;
  // RRR e.encode(bl);
  // RRR results[to_object_key(pool, e.object)] = bl;
}

void Store::add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  bufferlist bl;
  // RRR e.encode(bl);
  // RRR results[to_snap_key(pool, e.object)] = bl;
}

void Store::cleanup(ceph::os::Transaction* t)
{
  t->remove(coll, hoid);
}

seastar::future<> Store::flush(::ceph::os::Transaction* t)
{
  if (t) {

    return driver.get_transaction(t).then([this](auto&& txn) {
      backend.set_keys(results, txn.get());  // make this a 'move' of the uniquep
      results.clear();
      return seastar::make_ready_future<>();
    });
  }

  results.clear();
  return seastar::make_ready_future<>();
  //  crimson::osd::OSDriver::OSTransaction txn = driver.get_transaction(t);
  //  backend.set_keys(results, &txn);
}

/// RRR check if makes sense to make this a range generator at some future

Store::ErrtBuffersVec Store::get_errors(const string& start,
					const string& end,
					int64_t max_return) /*const*/
{
  return ::seastar::do_with(
    vector<bufferlist>{},
    [this, start, end, max_return](auto&& errors) mutable -> Store::ErrtBuffersVec {
      return seastar::repeat(
	       [this, start, end, mx = max_return,
		&errors]() mutable -> ::seastar::future<seastar::stop_iteration> {
		 if (--mx < 0) {
		   return seastar::make_ready_future<::seastar::stop_iteration>(
		     ::seastar::stop_iteration::yes);
		 }

		 return backend.get_next(start).safe_then(
		   [end, &errors](
		     auto&& kv) mutable -> ::seastar::future<::seastar::stop_iteration> {
		     if (!kv.has_value() || kv->first >= end) {
		       return seastar::make_ready_future<::seastar::stop_iteration>(
			 ::seastar::stop_iteration::yes);
		     }

		     errors.push_back(kv->second);
		     return seastar::make_ready_future<seastar::stop_iteration>(
		       seastar::stop_iteration::no);
		   },
		   [](auto&& e) {
		     return seastar::make_ready_future<::seastar::stop_iteration>(
		       ::seastar::stop_iteration::yes);
		   });
	       })
	.then([end, &errors]() -> Store::ErrtBuffersVec {
	  return Store::sstore_errorator::make_ready_future<BuffersVec>(
	    std::forward<vector<bufferlist>>(errors));
	});
    });
}


#if 0
auto next = std::make_pair(start, bufferlist{});
  while (max_return && !backend.get_next(next.first, &next)) {
    if (next.first >= end)
      break;
    errors.push_back(next.second);
    max_return--;
  }
  return errors;
#endif

#if 0
future<vector<int>> try_the_loop_v2(int st, int gd, int mx)
{
  // loop
  unique_ptr<TryLoop> tl = make_unique<TryLoop>(st, gd);

  vector<int> v0;

  return do_with(move(tl), move(v0), [mx](auto&& tl, auto&& v)  {
    return repeat([mx=mx, &v, tlp=tl.get()]() mutable -> future<stop_iteration> {

      if (--mx < 0)
	return seastar::make_ready_future<seastar::stop_iteration>(
	  seastar::stop_iteration::yes);

      return tlp->step().then([mx, &v, tlp](Dt&& dt) {
	if (dt.has_value()) {
	  v.push_back(*dt);
	  return seastar::make_ready_future<seastar::stop_iteration>(
	    seastar::stop_iteration::no);
	} else {
	  return seastar::make_ready_future<seastar::stop_iteration>(
	    seastar::stop_iteration::yes);
	}
      });
    }).then([mx, &v]() -> future<vector<int>> {
      // we should have the result here
      return make_ready_future<vector<int>>(move(v));
    });
  });
}
#endif


Store::ErrtBuffersVec Store::get_snap_errors(int64_t pool,
					     const librados::object_id_t& start,
					     uint64_t max_return)
{
  const string begin =
    (start.name.empty() ? first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);
  return get_errors(begin, end, max_return);
}


Store::ErrtBuffersVec Store::get_object_errors(int64_t pool,
					       const librados::object_id_t& start,
					       uint64_t max_return)
{
  const string begin =
    (start.name.empty() ? first_object_key(pool) : to_object_key(pool, start));
  const string end = last_object_key(pool);
  return get_errors(begin, end, max_return);
}


}  // namespace crimson::osd::Scrub
