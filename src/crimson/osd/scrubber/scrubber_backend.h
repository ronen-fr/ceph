// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include "./scrub_backend_if.h"

#include <common/LogClient.h>

#include <string_view>

class PG;


//namespace crimson::osd::Scrub {

using data_omap_digests_t =
  std::pair<std::optional<uint32_t>, std::optional<uint32_t>>;

using shard_info_map_t = std::map<pg_shard_t, shard_info_wrapper>;

using shard_to_smap_t = std::map<pg_shard_t, ScrubMap*>;

using IterToSMap = std::map<pg_shard_t, ScrubMap*>::const_iterator;

/**
 * A structure used internally by select_auth_object()
 */
struct shard_as_auth_t {
  enum class usable_t : uint8_t { not_usable, usable };

  // the ctor used when the shard should not be considered as auth
  explicit shard_as_auth_t(std::string err_msg)
      : possible_auth{usable_t::not_usable}
      , error_text{err_msg}
      , oi{}
      , auth_iter{}
      , digest{std::nullopt}
  {}

  shard_as_auth_t(std::string err_msg, std::optional<uint32_t> data_digest)
      : possible_auth{usable_t::not_usable}
      , error_text{err_msg}
      , oi{}
      , auth_iter{}
      , digest{data_digest}
  {}

  // possible auth candidate
  shard_as_auth_t(const object_info_t& anoi,
		  IterToSMap it,
		  std::string err_msg,
		  std::optional<uint32_t> data_digest)
      : possible_auth{usable_t::usable}
      , error_text{err_msg}
      , oi{anoi}
      , auth_iter{it}
      , digest{data_digest}
  {}


  usable_t possible_auth;  //{usable_t::not_usable};
  std::string error_text;
  object_info_t oi;
  IterToSMap auth_iter;
  std::optional<uint32_t> digest;
  // when used for Crimson, we'll probably want to return 'digest_match' (and
  // other in/out arguments) via this struct
};

struct auth_selection_t {
  IterToSMap auth;	  // an iter into one of this_chunk->maps
  pg_shard_t auth_shard;  // set to auth->first
  object_info_t auth_oi;
  shard_info_map_t shard_map;
  bool is_auth_available{
    false};		    // true if we've managed to select an auth' source
  bool digest_match{true};  // true if all (existing) digests match
};

/**
 * the back-end data that is per-chunk
 *
 * Created by the Scrubber after all the replica maps have arrived.
 */
struct ScrubBeChunk {

  using clock = ceph::coarse_real_clock;

  // per chunk:

  std::map<pg_shard_t, ScrubMap> m_received_maps;

  std::map<pg_shard_t, ScrubMap*> maps;

  std::set<hobject_t> master_set;

  utime_t started{clock::now() /*ceph_clock_now()*/};

  std::map<hobject_t, data_omap_digests_t> missing_digest;

  // Map from object with errors to good peers
  std::map<hobject_t, std::list<pg_shard_t>> authoritative;


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
class ScrubBackend : public ScrubBackendIF {
 public:
  ScrubBackend(PgScrubber& scrubber,
	       PGBackend& backend,
	       PG& pg,
	       pg_shard_t i_am,
	       bool repair,
	       scrub_level_t shallow_or_deep,
	       ScrubMap* primary_map,
	       std::set<pg_shard_t> acting);

  // friend class ::crimson::osd::PgScrubber;
  friend class PgScrubber;


  /**
   * reset the per-chunk data structure (ScrubBeChunk),
   * and attached the m_primary_map to it.
   */
  void new_chunk() final;

  void update_repair_status(bool should_repair) final;

  // note: used by both Primary & replicas
  // RRR to document
  static ScrubMap clean_meta_map(ScrubMap& cleaned, bool max_reached);

  /**
   * decode the arriving MOSDRepScrubMap message, placing the replica's
   * scrub-map into m_received_maps[from].
   *
   * @param from replica
   * @param pool TBD
   */
  void decode_received_map(pg_shard_t from,
			   const MOSDRepScrubMap& msg,
			   int64_t pool) final;

  seastar::future<> scrub_compare_maps(bool max_reached) final;

  int get_num_digest_updates_pending() const final
  {
    return num_digest_updates_pending;
  }

  void scan_snaps(ScrubMap& smap) final;

  int scrub_process_inconsistent() final;

  void repair_oinfo_oid(ScrubMap& smap) final;

  static std::ostream& logger_prefix(std::ostream* _dout, ScrubBackend* t);

 private:
  // set/constructed at the ctor():
  PgScrubber& m_scrubber;
  PGBackend& m_pgbe;
  const pg_shard_t m_pg_whoami;
  bool m_repair;
  const scrub_level_t m_depth;
  const spg_t m_pg_id;
  ScrubMap* m_primary_map;  // fix ownership
  std::set<pg_shard_t> m_acting;
  std::vector<pg_shard_t> m_acting_but_me;
  const bool m_is_replicated{true};  /// \todo handle EC
  std::string_view m_mode_desc;
  std::string m_formatted_id;

  // shorthands:
  ConfigProxy& m_conf;
  LogChannelRef clog;

  int num_digest_updates_pending{0};

 public:  // as used by PgScrubber::final_cstat_update(). consider relocating.
  // actually - only filled in by the PG backend, and used by the scrubber.
  // We are not handling it. So consider getting it from the Scrubber, or
  // creating it by the PG-BE
  omap_stat_t m_omap_stats = (const struct omap_stat_t){0};

 private:
  using IterToSMap = std::map<pg_shard_t, ScrubMap*>::const_iterator;

  using AuthAndObjErrors = std::tuple<std::list<pg_shard_t>,  ///< the auth-list
				      std::set<pg_shard_t>    ///< object_errors
				      >;

  //  struct auth_n_objerrs_t {
  //    std::list<pg_shard_t>;  ///< the auth-list
  //    std::set<pg_shard_t>;   ///< object_errors
  //  };

  std::optional<ScrubBeChunk> this_chunk;

  /// Maps from objects with errors to missing peers
  HobjToShardSetMapping m_missing;  // used by scrub_process_inconsistent()

  /// Maps from objects with errors to inconsistent peers
  HobjToShardSetMapping m_inconsistent;	 // used by scrub_process_inconsistent()

  /// Cleaned std::map pending snap metadata scrub
  ScrubMap m_cleaned_meta_map;

  void merge_to_master_set();

  void compare_smaps(std::stringstream& errstream);

  seastar::future<> compare_obj_in_maps(const hobject_t& ho,
					std::stringstream& errstream);

  std::optional<AuthAndObjErrors> for_empty_auth_list(
    std::list<pg_shard_t>&& auths,
    std::set<pg_shard_t>&& obj_errors,
    IterToSMap auth,
    const hobject_t& ho,
    std::stringstream& errstream);

  // RRR to rename
  AuthAndObjErrors match_in_shards(const hobject_t& ho,
				   object_info_t& auth_oi,
				   inconsistent_obj_wrapper& obj_result,
				   // map<pg_shard_t, shard_info_wrapper>&
				   // shard_map, bool digest_match,  // RRR ?
				   std::stringstream& errstream);

  // returns: true if a discrepancy was found
  bool compare_obj_details(pg_shard_t auth_shard,
			   const ScrubMap::object& auth,
			   const object_info_t& auth_oi,
			   const ScrubMap::object& candidate,
			   shard_info_wrapper& shard_result,
			   inconsistent_obj_wrapper& obj_result,
			   std::stringstream& errorstream,
			   bool has_snapset);
  //  bool compare_obj_details(pg_shard_t auth_shard,
  //			     const ScrubMap::object& auth,
  //			     const object_info_t& auth_oi,
  //			     const ScrubMap::object& candidate,
  //			     shard_info_wrapper& shard_result,
  //			     inconsistent_obj_wrapper& obj_result,
  //			     ostream& errorstream,
  //			     bool has_snapset);


  void repair_object(
    const hobject_t& soid,
    const std::list<std::pair<ScrubMap::object, pg_shard_t>>& ok_peers,
    const std::set<pg_shard_t>& bad_peers);

  /**
   * An auxiliary used by select_auth_object() to test a specific shard
   * as a possible auth candidate.
   * @param ho        the hobject for which we are looking for an auth source
   * @param srd       the candidate shard
   * @param shard_map [out] a collection of shard_info-s per shard.
   * possible_auth_shard() might set error flags in the relevant (this shard's)
   * entry.
   */
  shard_as_auth_t possible_auth_shard(const hobject_t& ho,
				      pg_shard_t& srd,
				      shard_info_map_t& shard_map);

  auth_selection_t select_auth_object(const hobject_t& ho,
				      std::stringstream& errstream);


  enum class digest_fixing_t { no, if_aged, force };

  /*
   *  an aux used by inconsistents() to determine whether to fix the digest
   */
  [[nodiscard]] digest_fixing_t should_fix_digest(
    const hobject_t& ho,
    const ScrubMap::object& auth_object,
    const object_info_t& auth_oi,
    bool repair_flag,
    std::stringstream& errstream);


  //  [[nodiscard]] digest_fixing_t should_fix_digest(const hobject_t& ho, const
  //  ScrubMap::object& auth_object,
  //		const object_info_t& auth_oi, bool repair_flag, ostream&
  //errstream);

  seastar::future<> inconsistents(
    const hobject_t& ho,
    ScrubMap::object& auth_object,
    object_info_t& auth_oi,  // RRR move to object?
    AuthAndObjErrors&& auth_n_errs,
    std::stringstream& errstream);

  int process_clones_to(const std::optional<hobject_t>& head,
			const std::optional<SnapSet>& snapset,
			bool allow_incomplete_clones,
			std::optional<snapid_t> target,
			std::vector<snapid_t>::reverse_iterator* curclone,
			inconsistent_snapset_wrapper& e);

  /**
   * Validate consistency of the object info and snap sets.
   */
  void scrub_snapshot_metadata(ScrubMap& map);

  void record_object_digests(const hobject_t& obj,
			     std::optional<uint32_t> data_digest,
			     std::optional<uint32_t> omap_digest);

  seastar::future<> update_authoritative(std::stringstream& errstream);

  void log_missing(int missing,
		   const std::optional<hobject_t>& head,
		   // const spg_t& pgid,
		   const char* logged_func_name,
		   bool allow_incomplete_clones);

  void scan_object_snaps(const hobject_t& hoid,
			 ScrubMap::object& scrmap_obj,
			 const SnapSet& snapset);
};


//} // namespace crimson::osd::Scrub