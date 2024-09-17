// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./ScrubStore.h"

#include "common/scrub_types.h"
#include "include/rados/rados_types.hpp"
//#include "osd/osd_types.h"
//#include "osd/osd_types_fmt.h"


#include "pg_scrubber.h"

using std::ostringstream;
using std::string;
using std::vector;

using ceph::bufferlist;

namespace {

string first_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(), "", CEPH_NOSNAP, 0x00000000, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

// the object_key should be unique across pools
string to_object_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(
      object_t(oid.name),
      oid.locator,  // key
      oid.snap,
      0,  // hash
      pool, oid.nspace);
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string last_object_key(int64_t pool)
{
  auto hoid = hobject_t(object_t(), "", CEPH_NOSNAP, 0xffffffff, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_OBJ_" + hoid.to_str();
}

string first_snap_key(int64_t pool)
{
  // scrub object is per spg_t object, so we can misuse the hash (pg.seed) for
  // representing the minimal and maximum keys. and this relies on how
  // hobject_t::to_str() works: hex(pool).hex(revhash).
  auto hoid = hobject_t(object_t(), "", 0, 0x00000000, pool, "");
  hoid.build_hash_cache();
  return "SCRUB_SS_" + hoid.to_str();
}

string to_snap_key(int64_t pool, const librados::object_id_t& oid)
{
  auto hoid = hobject_t(
      object_t(oid.name),
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

uint64_t decode_errors(hobject_t o, const ceph::buffer::list& bl)
{
  inconsistent_obj_wrapper iow{o};
  auto sbi = bl.cbegin();
  // should add an iterator for r-value
  iow.decode(sbi);
  return iow.errors;
}

void reencode_errors(hobject_t o, uint64_t errors, ceph::buffer::list& bl)
{
  inconsistent_obj_wrapper iow{o};
  auto sbi = bl.cbegin();
  // should add an iterator for r-value
  iow.decode(sbi);
  iow.errors = errors;
  iow.encode(bl);
}

}  // namespace

#undef dout_context
#define dout_context (m_scrubber.get_pg_cct())
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}

namespace Scrub {

// ////////////////////////// Store //////////////////////////

Store::Store(
    PgScrubber& scrubber,
    ObjectStore& osd_store,
    ObjectStore::Transaction* t,
    const spg_t& pgid,
    const coll_t& coll,
    LoggerSinkSet& logger)
    : m_scrubber{scrubber}
    , object_store{osd_store}
    , coll{coll}
    , clog{logger}
{
  ceph_assert(t);

  const auto sh_err_obj =
      pgid.make_temp_ghobject(fmt::format("scrub_{}", pgid));
  t->touch(coll, sh_err_obj);
  shallow_db.emplace(
      pgid, sh_err_obj, OSDriver{&object_store, coll, sh_err_obj});

  // and the DB for deep errors
  const auto dp_err_obj =
      pgid.make_temp_ghobject(fmt::format("deep_scrub_{}", pgid));
  t->touch(coll, dp_err_obj);
  deep_db.emplace(pgid, dp_err_obj, OSDriver{&object_store, coll, dp_err_obj});

  dout(20) << fmt::format(
		  "created Scrub::Store for pg[{}], shallow: {}, deep: {}",
		  pgid, sh_err_obj, dp_err_obj)
	   << dendl;
}


Store::~Store()
{
  //   ceph_assert(shallow_results.empty());
  //   ceph_assert(deep_results.empty());
}


// std::optional<Store::at_level_t> Store::create_level_store(
//     ObjectStore::Transaction* t,
//     const spg_t& pgid,
//     std::string_view obj_name_seed)
// {
//   using at_level_t = ::Scrub::Store::at_level_t;
//   const auto err_obj =
//       pgid.make_temp_ghobject(fmt::format("{}_{}", obj_name_seed, pgid));
//   t->touch(coll, err_obj);
//   return at_level_t{
//       pgid, err_obj, std::move(OSDriver{&object_store, coll, err_obj})};
// }


// Store::at_level_t Store::create_level_store(
//     ObjectStore::Transaction* t,
//     const spg_t& pgid,
//     std::string_view obj_name_seed)
// {
//   using at_level_t = ::Scrub::Store::at_level_t;
//   const auto err_obj =
//       pgid.make_temp_ghobject(fmt::format("{}_{}", obj_name_seed, pgid));
//   t->touch(coll, err_obj);
//   return at_level_t{
//       pgid, err_obj, std::move(OSDriver{&object_store, coll, err_obj})};
// }

std::ostream& Store::gen_prefix(std::ostream& out, std::string_view fn) const
{
  if (fn.starts_with("operator")) {
    // it's a lambda, and __func__ is not available
    return m_scrubber.gen_prefix(out) << "Store::";
  } else {
    return m_scrubber.gen_prefix(out) << "Store::" << fn << ": ";
  }
}


void Store::add_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  add_object_error(pool, e);
}

void Store::add_object_error(int64_t pool, const inconsistent_obj_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);

  const auto key = to_object_key(pool, e.object);
  if (e.has_deep_errors()) {
    deep_db->results[key] = bl;
  }
  shallow_db->results[key] = bl;
}

void Store::add_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  add_snap_error(pool, e);
}

void Store::add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e)
{
  bufferlist bl;
  e.encode(bl);
  // note: snap errors are only placed in the shallow store
  shallow_db->results[to_snap_key(pool, e.object)] = bl;
}

bool Store::is_empty() const
{
  return shallow_db->results.empty() && deep_db->results.empty();
}

void Store::flush(ObjectStore::Transaction* t)
{
  if (t) {
    auto txn = shallow_db->driver.get_transaction(t);
    shallow_db->backend.set_keys(shallow_db->results, &txn);
    txn = deep_db->driver.get_transaction(t);
    deep_db->backend.set_keys(deep_db->results, &txn);
  }

  shallow_db->results.clear();
  deep_db->results.clear();
}

void Store::clear_level_db(
    ObjectStore::Transaction* t,
    at_level_t& db,
    std::string_view db_name)
{
  dout(20) << fmt::format("removing (omap) entries for {} error DB", db_name)
	   << dendl;
  // easiest way to guarantee that the object representing the DB exists
  t->touch(coll, db.errors_hoid);

  // remove all the keys in the DB
  t->omap_clear(coll, db.errors_hoid);

  // restart the 'in progress' part of the MapCacher
  db.backend.reset();
}


void Store::reinit(ObjectStore::Transaction* t, scrub_level_t level)
{
  if (!t) {
    dout(20) << fmt::format(
		    "no transaction provided, skipping reinit of scrub errors")
	     << dendl;
    return;
  }

  // always clear the known shallow errors DB (as both shallow and deep scrubs
  // would recreate it)
  if (shallow_db) {
    clear_level_db(t, *shallow_db, "shallow");
  }

  // only a deep scrub recreates the deep errors DB
  if (level == scrub_level_t::deep && deep_db) {
    clear_level_db(t, *deep_db, "deep");
  }
}


void Store::cleanup(ObjectStore::Transaction* t, scrub_level_t level)
{
  //clog.warn() << fmt::format("{}: cleaning up scrub errors", __func__);
  dout(20) << fmt::format(
		  "cleaning up scrub errors (level:{})",
		  level == scrub_level_t::deep ? "deep" : "shallow")
	   << dendl;
  ceph_assert(t);

  // always clear the known shallow errors DB (as both shallow and deep scrubs
  // would recreate it)
  t->remove(coll, shallow_db->errors_hoid);

  // only a deep scrub recreates the deep errors DB
  if (level == scrub_level_t::deep) {
    t->remove(coll, deep_db->errors_hoid);
  }
}


void Store::discard_all(ObjectStore::Transaction* t)
{
  dout(20) << "discarding DB contents" << dendl;
  clog.warn() << fmt::format("{}: discarding all scrub errors", __func__);
  cleanup(t, scrub_level_t::deep);
}


std::vector<bufferlist> Store::get_snap_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const
{
  vector<bufferlist> errors;
  const string begin =
      (start.name.empty() ? first_snap_key(pool) : to_snap_key(pool, start));
  const string end = last_snap_key(pool);

  // the snap errors are stored only in the shallow store
  ExpCacherPosData latest_sh = shallow_db->backend.get_1st_after_key(begin);

  while (max_return && latest_sh.has_value() && latest_sh->last_key < end) {
    errors.push_back(latest_sh->data);
    max_return--;
    latest_sh = shallow_db->backend.get_1st_after_key(latest_sh->last_key);
  }

  return errors;
}


std::vector<bufferlist> Store::get_object_errors(
    int64_t pool,
    const librados::object_id_t& start,
    uint64_t max_return) const
{
  const string begin =
      (start.name.empty() ? first_object_key(pool)
			  : to_object_key(pool, start));
  const string end = last_object_key(pool);
  dout(20) << fmt::format("getting errors from {} to {}", begin, end) << dendl;
  return get_errors(begin, end, max_return);
}


inline void decode(
    librados::inconsistent_obj_t& obj,
    ceph::buffer::list::const_iterator& bp)
{
  reinterpret_cast<inconsistent_obj_wrapper&>(obj).decode(bp);
}


void Store::collect_specific_store(
    MapCacher::MapCacher<std::string, ceph::buffer::list>& backend,
    Store::ExpCacherPosData& latest,
    std::vector<bufferlist>& errors,
    const std::string& end_key,
    uint64_t& max_return) const
{
  while (max_return && latest.has_value() &&
	 latest.value().last_key < end_key) {
    errors.push_back(latest->data);
    max_return--;
    latest = shallow_db->backend.get_1st_after_key(latest->last_key);
  }
}

// a better way to implement this: use two generators, one for each store.
// and sort-merge the results. Almost like a merge-sort, but with equal
// keys combined. 'todo' once 'ranges' are really working.

std::vector<bufferlist> Store::get_errors(
    const std::string& from_key,
    const std::string& end_key,
    uint64_t max_return) const
{
  // merge the input from the two sorted DBs into 'errors' (until
  // enough errors are collected)
  vector<bufferlist> errors;

  auto& sh_level = shallow_db;
  auto& dp_level = deep_db;
  dout(10) << fmt::format("getting errors from {} to {}", from_key, end_key)
	   << dendl;

  ExpCacherPosData latest_sh = sh_level->backend.get_1st_after_key(from_key);
  ExpCacherPosData latest_dp = dp_level->backend.get_1st_after_key(from_key);

  while (max_return) {
    dout(20) << fmt::format(
		    "n:{} latest_sh: {}, latest_dp: {}", max_return,
		    (latest_sh ? latest_sh->last_key : "(none)"),
		    (latest_dp ? latest_dp->last_key : "(none)"))
	     << dendl;

    // returned keys that are greater than end_key are not interesting
    if (latest_sh.has_value() && latest_sh->last_key >= end_key) {
      latest_sh = tl::unexpected(-EINVAL);
    }
    if (latest_dp.has_value() && latest_dp->last_key >= end_key) {
      latest_dp = tl::unexpected(-EINVAL);
    }

    if (!latest_sh && !latest_dp) {
      // both stores are exhausted
      break;
    }
    if (!latest_sh.has_value()) {
      // continue with the deep store
      dout(10) << fmt::format("collecting from deep store") << dendl;
      collect_specific_store(
	  dp_level->backend, latest_dp, errors, end_key, max_return);
      break;
    }
    if (!latest_dp.has_value()) {
      // continue with the shallow store
      dout(10) << fmt::format("collecting from shallow store") << dendl;
      collect_specific_store(
	  sh_level->backend, latest_sh, errors, end_key, max_return);
      break;
    }

    // we have results from both stores. Select the one with a lower key.
    // If the keys are equal, combine the errors.
    if (latest_sh->last_key == latest_dp->last_key) {
      uint64_t known_sh_errs =
	  decode_errors(sh_level->errors_hoid.hobj, latest_sh->data);
      uint64_t known_dp_errs =
	  decode_errors(dp_level->errors_hoid.hobj, latest_dp->data);
      uint64_t known_errs =
	  known_sh_errs | (known_dp_errs & librados::obj_err_t::DEEP_ERRORS);
      dout(20) << fmt::format(
		      "key:{} errors: sh:{:x},dp:{} -> {}", latest_sh->last_key,
		      known_sh_errs, known_dp_errs, known_errs)
	       << dendl;

      reencode_errors(dp_level->errors_hoid.hobj, known_errs, latest_dp->data);
      errors.push_back(latest_dp->data);
      latest_sh = sh_level->backend.get_1st_after_key(latest_sh->last_key);
      latest_dp = dp_level->backend.get_1st_after_key(latest_dp->last_key);

    } else if (latest_sh->last_key < latest_dp->last_key) {
      dout(20) << fmt::format("shallow store element ({})", latest_sh->last_key)
	       << dendl;
      errors.push_back(latest_sh->data);
      latest_sh = sh_level->backend.get_1st_after_key(latest_sh->last_key);
    } else {
      dout(20) << fmt::format("deep store element ({})", latest_dp->last_key)
	       << dendl;
      errors.push_back(latest_dp->data);
      latest_dp = dp_level->backend.get_1st_after_key(latest_dp->last_key);
    }
    max_return--;
  }

  dout(10) << fmt::format("{} errors reported", errors.size()) << dendl;
  return errors;
}

}  // namespace Scrub