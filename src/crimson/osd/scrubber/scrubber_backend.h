// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <optional>
#include <set>
#include <tuple>

#include "crimson/osd/scrubber/scrubber.h"

/*
  Dev notes (to be rephrased or removed):

  What does a replica needs of the ScrubberBE, and should we use a separate ctor?

  Note that the bulk of work performed by the replica, is done in build_replica_map_chunk:
  - we call build_scrub_map_chunk(), then
  - play around w/ cleaned_meta_map & replica_scrubmap, and
  - ** call the backend's clean_meta_map
  - (and we might later on have be use in _scan_snaps)

  Uses:
  - (check the proper location for scan_rollback_obs()) <- can stay in PgScrubber,
     it seems
  - check repair_oinfo_oid() <- seems like something for the be.
  - calls to m_be (now) only for clean_meta_map():
    - clean_meta_map() can be made static!
    - for the replica - it is called on cleaned_meta_map

  Ctor:
   - no STATE_REPAIR (I think)
   - no primary map


 DECISION: WILL ONLY USE STATIC BE FUNCTIONS FOR THE REPLICA. WILL NOT CREATE THE
    OBJECT.

 */

namespace crimson::osd::Scrub {

using data_omap_digests_t = std::pair<std::optional<uint32_t>, std::optional<uint32_t>>;

/**
 * the back-end data that is per-chunk
 *
 * Created by the Scrubber after all the replica maps have arrived.
 */

struct ScrubberBeChunk {

  // per chunk:

  std::map<pg_shard_t, ScrubMap> m_received_maps;

  std::map<pg_shard_t, ScrubMap*> maps;

  std::set<hobject_t> master_set;

  utime_t started{ceph_clock_now()};

  std::map<hobject_t, data_omap_digests_t> missing_digest;

  // Map from object with errors to good peers
  std::map<hobject_t, list<pg_shard_t>> authoritative;


  // these must be reset for each element:

  std::set<pg_shard_t> cur_missing;
  std::set<pg_shard_t> cur_inconsistent;
  bool fix_digest{false};
};


/*
 * Wraps the data required for the back-end part of the scrubbing:
 * comparing the maps and fixing objects.
 *
 * Created upon the initiation of a scrub session.
 *
 * Uses 'optional' entries for winking-out the auxiliary collections.
 */
class ScrubberBE {
 public:
  ScrubberBE(PgScrubber& scrubber,
	     PGBackend& backend,
	     pg_shard_t i_am,
	     bool repair,
	     ScrubMap* primary_map,
	     std::set<pg_shard_t> acting);

  friend class ::crimson::osd::PgScrubber;

  /**
   * reset the per-chunk data structure (ScrubberBeChunk),
   * and attached the m_primary_map to it.
   */
  void new_chunk();

  // note: used by both Primary & replicas
  // RRR to document
  static ScrubMap clean_meta_map(ScrubMap& cleaned, bool max_reached);

  /**
   * decode the arriving MOSDRepScrubMap message, placing the replica's scrub-map
   * into m_received_maps[from].
   *
   * @param from replica
   * @param pool TBD
   */
  void decode_received_map(pg_shard_t from, const MOSDRepScrubMap& msg, int64_t pool);

  seastar::future<> scrub_compare_maps(bool max_reached);

 private:
  // set/constructed at the ctor():
  PgScrubber& m_scrubber;
  PGBackend& m_pgbe;
  const pg_shard_t m_pg_whoami;
  bool m_repair;
  const spg_t m_pg_id;
  ScrubMap* m_primary_map;
  std::set<pg_shard_t> m_acting;
  std::vector<pg_shard_t> m_acting_but_me;
  const bool m_is_replicated{true}; /// \todo handle EC

 public: // as used by PgScrubber::final_cstat_update(). consider relocating.

  // actually - only filled in by the PG backend, and used by the scrubber.
  // We are not handling it. So consider getting it from the Scrubber, or
  // creating it by the PG-BE
  omap_stat_t m_omap_stats = (const struct omap_stat_t){0};

 private:

  using IterToSMap = map<pg_shard_t, ScrubMap*>::const_iterator;

  using AuthAndObjErrors = std::tuple<std::list<pg_shard_t>,  ///< the auth-list
				      std::set<pg_shard_t>    ///< object_errors
				      >;

//  struct auth_n_objerrs_t {
//    std::list<pg_shard_t>;  ///< the auth-list
//    std::set<pg_shard_t>;   ///< object_errors
//  };

  std::optional<ScrubberBeChunk> this_chunk;

  /// Maps from objects with errors to missing peers
  HobjToShardSetMapping m_missing; // used by scrub_process_inconsistent()

  /// Maps from objects with errors to inconsistent peers
  HobjToShardSetMapping m_inconsistent; // used by scrub_process_inconsistent()

  /// Cleaned std::map pending snap metadata scrub
  ScrubMap m_cleaned_meta_map;

  void merge_to_master_set();

  seastar::future<> compare_smaps(ostream& errstream);

  seastar::future<> compare_obj_in_maps(const hobject_t& ho, ostream& errstream);

  std::optional<AuthAndObjErrors> empty_auth_list(std::list<pg_shard_t>&& auths,
						  std::set<pg_shard_t>&& obj_errors,
						  IterToSMap auth,
						  ostream& errstream);

  // RRR to rename
  AuthAndObjErrors match_in_shards(IterToSMap auth,
			const hobject_t& ho,
			object_info_t& auth_oi,
			inconsistent_obj_wrapper& obj_result,
			map<pg_shard_t, shard_info_wrapper>& shard_map,
			bool digest_match,  // RRR ?
			ostream& errstream);

  // returns: true if a discrepancy was found
  bool compare_obj_details(pg_shard_t auth_shard,
			     const ScrubMap::object& auth,
			     const object_info_t& auth_oi,
			     const ScrubMap::object& candidate,
			     shard_info_wrapper& shard_result,
			     inconsistent_obj_wrapper& obj_result,
			     ostream& errorstream,
			     bool has_snapset);

  enum class digest_fixing_t { no, if_aged, force};

  // an aux used by inconsistents() to determine whether to fix the digest

  [[nodiscard]] digest_fixing_t should_fix_digest(const hobject_t& ho, const ScrubMap::object& auth_object,
  		const object_info_t& auth_oi, bool repair_flag, ostream& errstream);

  seastar::future<> inconsistents(const hobject_t& ho,
				  ScrubMap::object& auth_object,
				  object_info_t& auth_oi,  // RRR move to object?
				  AuthAndObjErrors&& auth_n_errs,
				  ostream& errstream);


  /**
   * Validate consistency of the object info and snap sets.
   */
  void scrub_snapshot_metadata(ScrubMap& map, const missing_map_t& missing_digest);

  void record_object_digests(int missing);

  seastar::future<> update_authoritative(stringstream& errstream);
};


}  // namespace crimson::osd::Scrub