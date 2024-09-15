// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <utility>

#include "common/LogClient.h"
#include "common/map_cacher.hpp"
#include "osd/SnapMapper.h"  // for OSDriver

namespace librados {
struct object_id_t;
}

struct inconsistent_obj_wrapper;
struct inconsistent_snapset_wrapper;


namespace Scrub {

/**
 * Storing errors detected during scrubbing.
 *
 * From both functional and internal perspectives, the store is a pair of key-value
 * databases: one maps objects to shallow errors detected during their scrubbing,
 * and other stores deep errors.
 * Note that the first store is updated in both shallow and in deep scrubs. The
 * second - only while deep scrubbing.
 *
 * The DBs can be consulted by the operator, when trying to list 'errors known
 * at this point in time'. Whenever a scrub starts - the relevant entries in the
 * DBs are removed. Specifically - the shallow errors DB is recreated each scrub,
 * while the deep errors DB is recreated only when a deep scrub starts.
 *
 * When queried - the data from both DBs is merged for each named object, and
 * returned to the operator.
 *
 * Implementation:
 * Each of the two DBs is implemented as OMAP entries of a single, uniquely named,
 * object. Both DBs are cached using the general KV Cache mechanism.
 */

class Store {
 public:
  ~Store();
  static Store* create(
      ObjectStore* store,
      ObjectStore::Transaction* t,
      const spg_t& pgid,
      const coll_t& coll,
      LoggerSinkSet& logger);

  void add_object_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_snap_error(int64_t pool, const inconsistent_snapset_wrapper& e);

  // and a variant-friendly interface:
  void add_error(int64_t pool, const inconsistent_obj_wrapper& e);
  void add_error(int64_t pool, const inconsistent_snapset_wrapper& e);
  [[nodiscard]] bool is_empty() const;
  void flush(ObjectStore::Transaction*);
  void cleanup(ObjectStore::Transaction*, scrub_level_t level);

  std::vector<ceph::buffer::list> get_snap_errors(
      int64_t pool,
      const librados::object_id_t& start,
      uint64_t max_return) const;

  std::vector<ceph::buffer::list> get_object_errors(
      int64_t pool,
      const librados::object_id_t& start,
      uint64_t max_return) const;

 protected:
  // machinery for the error store of a specific scrub level
  struct at_level_t {
    at_level_t(const spg_t& pgid, const ghobject_t& err_obj, OSDriver&& driver);

    /// the object in the PG store, where the errors are stored
    ghobject_t errors_hoid;

    /// a temp object mapping seq-id to inconsistencies
    OSDriver driver;

    MapCacher::MapCacher<std::string, ceph::buffer::list> backend;

    std::map<std::string, ceph::buffer::list> results;

    static_assert(
        std::is_move_constructible<OSDriver>::value,
        "OSDriver must be move-constructible");
    static_assert(
        std::is_move_assignable<OSDriver>::value,
        "OSDriver must be move-assignable");
//     static_assert(
//         std::is_move_constructible<MapCacher::MapCacher<std::string, ceph::buffer::list>>::value,
//         "MapCacher must be move-constructible");
//     static_assert(
//         std::is_move_assignable<MapCacher::MapCacher<std::string, ceph::buffer::list>>::value,
//         "MapCacher must be move-assignable");
  };

    /// \todo make at_level_t move-constructible (to simplify swap etc.)
//     static_assert(
//         std::is_move_constructible<at_level_t>::value,
//         "at_level_t must be move-constructible");
//     static_assert(
//         std::is_move_assignable<at_level_t>::value,
//         "at_level_t must be move-assignable");


  using CacherPosData =
      MapCacher::MapCacher<std::string, ceph::buffer::list>::PosAndData;
  using ExpCacherPosData = tl::expected<CacherPosData, int>;

 private:
  ObjectStore& object_store;

  // the collection (i.e. - the PG store) in which the errors are stored
  const coll_t coll;

  LoggerSinkSet& clog;

  /// the machinery (backend details, cache, etc.) for storing both levels of errors
  /// (note: 'optional' to allow delayed creation w/o dynamic allocations, and
  /// 'mutable' as the caching mechanism is used in const methods)
  mutable std::array<std::optional<at_level_t>, 2> per_level_store;

  //  std::map<std::string, ceph::buffer::list> shallow_results;
  //  std::map<std::string, ceph::buffer::list> deep_results;
  //   using CacherPosData =
  //       MapCacher::MapCacher<std::string, ceph::buffer::list>::PosAndData;
  //   using ExpCacherPosData = tl::expected<CacherPosData, int>;

  //   static std::optional<at_level_t> create_level_store(
  //       ObjectStore::Transaction* t,
  //       const spg_t& pgid,
  //       std::string_view obj_name_seed);

//   Store(
//       ObjectStore& osd_store,
//       const coll_t& coll,
//       std::optional<at_level_t>&& shallow,
//       std::optional<at_level_t>&& deep,
//       LoggerSinkSet& logger);

  Store(
      ObjectStore& osd_store,
      const coll_t& coll,
      const spg_t& pgid,
      const ghobject_t& sh_err_obj,
      const ghobject_t& dp_err_obj,
      LoggerSinkSet& logger);

  std::vector<ceph::buffer::list> get_errors(
      const std::string& start,
      const std::string& end,
      uint64_t max_return) const;

  void collect_specific_store(
      MapCacher::MapCacher<std::string, ceph::buffer::list>& backend,
      ExpCacherPosData& latest,
      std::vector<bufferlist>& errors,
      const std::string& end_key,
      uint64_t& max_return) const;
};
}  // namespace Scrub
