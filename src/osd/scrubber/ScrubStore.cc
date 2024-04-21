// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*- 
// vim: ts=8 sw=2 smarttab

#include "ScrubStore.h"
#include "osd/osd_types.h"
#include "common/scrub_types.h"
#include "include/rados/rados_types.hpp"

using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

namespace {
/**
 * create two special temp objects in the PG. Those are used to
 * hold the last/ongoing scrub errors detected. The errors are
 * coded as OMAP entries attached to the objects.
 * One of the objects stores detected shallow errors, and the other -
 * deep errors.
 */
auto make_scrub_objects(const spg_t& pgid)
{
  return std::pair{
     pgid.make_temp_ghobject(fmt::format("scrub_{}", pgid)),
     pgid.make_temp_ghobject(fmt::format("deep_scrub_{}", pgid))};
}

string first_object_key(int64_t pool)
{
  auto shallow_hoid = hobject_t(object_t(),
			"",
			0,
			0x00000000,
			pool,
			"");
  shallow_hoid.build_hash_cache();
  return "SCRUB_OBJ_" + shallow_hoid.to_str();
}

// the object_key should be unique across pools
string to_object_key(int64_t pool, const librados::object_id_t& oid)
{
  auto shallow_hoid = hobject_t(object_t(oid.name),
			oid.locator, // key
			oid.snap,
			0,		// hash
			pool,
			oid.nspace);
  shallow_hoid.build_hash_cache();
  return "SCRUB_OBJ_" + shallow_hoid.to_str();
}

string last_object_key(int64_t pool)
{
  auto shallow_hoid = hobject_t(object_t(),
			"",
			0,
			0xffffffff,
			pool,
			"");
  shallow_hoid.build_hash_cache();
  return "SCRUB_OBJ_" + shallow_hoid.to_str();
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // the representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  auto shallow_hoid = hobject_t(object_t(),
			"",
			0,
			0x00000000,
			pool,
			"");
  shallow_hoid.build_hash_cache();
  return "SCRUB_SS_" + shallow_hoid.to_str();
}

string to_snap_key(int64_t pool, const librados::object_id_t& oid)
{
  auto shallow_hoid = hobject_t(object_t(oid.name),
			oid.locator, // key
			oid.snap,
			0x77777777, // hash
			pool,
			oid.nspace);
  shallow_hoid.build_hash_cache();
  return "SCRUB_SS_" + shallow_hoid.to_str();
}

string last_snap_key(int64_t pool)
{
  auto shallow_hoid = hobject_t(object_t(),
			"",
			0,
			0xffffffff,
			pool,
			"");
  shallow_hoid.build_hash_cache();
  return "SCRUB_SS_" + shallow_hoid.to_str();
}
}

namespace Scrub {

Store*
Store::create(ObjectStore* store,
	      ObjectStore::Transaction* t,
	      const spg_t& pgid,
	      const coll_t& coll)
{
  ceph_assert(store);
  ceph_assert(t);
  auto [shallow_oid, deep_oid] = make_scrub_objects(pgid);
  t->touch(coll, shallow_oid);
  t->touch(coll, deep_oid);

  return new Store{coll, shallow_oid, deep_oid, store};
}

Store::Store(
    const coll_t& coll,
    const ghobject_t& sh_oid,
    const ghobject_t& dp_oid,
    ObjectStore* store)
    : coll(coll)
    , shallow_hoid(sh_oid)
    , shallow_driver(store, coll, sh_oid)
    , shallow_backend(&shallow_driver)
    , deep_hoid(dp_oid)
    , deep_driver(store, coll, dp_oid)
    , deep_backend(&deep_driver)
{}

Store::~Store()
{
  ceph_assert(shallow_results.empty());
  ceph_assert(deep_results.empty());
}

void Store::add_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  add_object_error(pool, e);
}

void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  if (e.has_deep_errors()) {
    deep_results[to_object_key(pool, e.object)] = bl;
  }
  shallow_results[to_object_key(pool, e.object)] = bl;
}

void Store::add_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  add_snap_error(pool, e);
}

void Store::add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  shallow_results[to_snap_key(pool, e.object)] = bl;
}

bool Store::empty() const
{
  return shallow_results.empty() && deep_results.empty();
}

void Store::flush(ObjectStore::Transaction* t)
{
  if (t) {
    OSDriver::OSTransaction txn = shallow_driver.get_transaction(t);
    shallow_backend.set_keys(shallow_results, &txn);

    OSDriver::OSTransaction deep_txn = deep_driver.get_transaction(t);
    deep_backend.set_keys(deep_results, &deep_txn);
  }

  shallow_results.clear();
  deep_results.clear();
}

void Store::cleanup(ObjectStore::Transaction* t, scrub_level_t level)
{
  // always clear the known shallow errors DB (as both shallow and deep scrubs
  // would recreate it)
  t->remove(coll, shallow_hoid);

  // only a deep scrub recreates the deep errors DB
  if (level == scrub_level_t::deep) {
    t->remove(coll, deep_hoid);
  }
}

std::vector<bufferlist>
Store::get_snap_errors(int64_t pool,
		       const librados::object_id_t& start,
		       uint64_t max_return) const
{
  const string begin = (start.name.empty() ?
			first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);
  return get_errors(begin, end, max_return);
}

std::vector<bufferlist>
Store::get_object_errors(int64_t pool,
			 const librados::object_id_t& start,
			 uint64_t max_return) const
{
  const string begin = (start.name.empty() ?
			first_object_key(pool) : to_object_key(pool, start));
  const string end = last_object_key(pool);
  return get_errors(begin, end, max_return);
}

using MT = tl::expected<std::pair<std::string, bufferlist>, std::string>;
inline void decode(librados::inconsistent_obj_t& obj,
		   ceph::buffer::list::const_iterator& bp) {
  reinterpret_cast<inconsistent_obj_wrapper&>(obj).decode(bp);
}

std::vector<bufferlist>
Store::get_errors(const string& from_key,
		  const string& end_key,
		  uint64_t max_return) const
{
  vector<bufferlist> errors;

  // until enough errors are collected, merge the input from the
  // two sorted DBs

  //auto latest_sh = std::make_pair(begin, bufferlist{});
  //auto latest_dp = std::make_pair(begin, bufferlist{});

  using MT = tl::expected<std::pair<std::string, bufferlist>, std::string>;

  MT latest_sh = shallow_backend.get_next_r(from_key);
  MT latest_dp = shallow_backend.get_next_r(from_key);

  while (max_return) {
    if (!latest_sh && !latest_dp) {
      break;
    }
    if (latest_sh == latest_dp) {
      // if !has_value(), the '==' is never true

      // combine the errors
      librados::inconsistent_obj_t sh{latest_sh->first};
      decode(sh, latest_sh->second.cbegin());


      errors.push_back(latest_sh->second);
      latest_sh = shallow_backend.get_next_r(latest_sh->first);
      latest_dp = shallow_backend.get_next_r(latest_dp->first);
      max_return--;
    } else
    if (latest_sh < latest_dp) {
      if (latest_sh) {
        errors.push_back(latest_dp.second);
        latest_sh = shallow_backend.get_next_r(latest_sh);
        max_return--;
      } else {
        errors.push_back(latest_sh.second);
        latest_sh = shallow_backend.get_next_r(latest_sh);
        max_return--;



 if (latest_dp.first == 


      if (latest_sh.first >= end)

  }

  auto next = std::make_pair(begin, bufferlist{});
  while (max_return && !shallow_backend.get_next(next.first, &next)) {
    if (next.first >= end)
      break;
    errors.push_back(next.second);
    max_return--;
  }
  return errors;
}

} // namespace Scrub