// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrubber_backend.h"

//#include <fmt/core.h>
#include <fmt/format.h>

#include <algorithm>

//#include <iostream>
//#include <map>
//#include <unordered_map>
//#include <vector>

#include "common/debug.h"

#include "common/errno.h"
#include "common/scrub_types.h"
#include "crimson/common/log.h"
#include "crimson/os/futurized_store.h"
#include "crimson/osd/pg_backend.h"
#include "messages/MOSDRepScrubMap.h"

#include "scrubber.h"

using std::list;
using std::pair;
using std::set;
using std::stringstream;
using std::vector;
//using namespace crimson::Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}


#ifdef WITH_SEASTAR
#define prefix_temp "--prefix-- "
#define RRLOG(LVL, S) logger().debug(std::string{prefix_temp}+(S))
#else
#define RRLOG(LVL, S) { dout << (S) << dendl; }
#endif

//RRLOG(13, (fmt::format("{}-{}-{}", "hello", i, "world")));

//namespace crimson::osd::Scrub {

ScrubBackend::ScrubBackend(PgScrubber& scrubber,
		       PGBackend& backend,
		       pg_shard_t i_am,
		       bool repair,
		       ScrubMap* primary_map,
		       /* for now*/ std::set<pg_shard_t> acting)
    : m_scrubber{scrubber}
    , m_pgbe{backend}
    , m_pg_whoami{i_am}
    , m_repair{repair}
    , m_pg_id{scrubber.m_pg_id}
    , m_primary_map{primary_map}
    , m_conf{m_scrubber.get_pg_cct()->_conf}
    , clog{m_scrubber.m_osds->clog}
{
  {
    // create the formatted ID string
    // (RRR copied - and I don't like the orig:)
    char buf[spg_t::calc_name_buf_size];
    buf[spg_t::calc_name_buf_size - 1] = '\0';
    m_formatted_id = m_pg_id.calc_name(buf + spg_t::calc_name_buf_size - 1, "");
  }

  m_acting_but_me.reserve(acting.size());
  std::copy_if(acting.begin(), acting.end(),
	       std::back_inserter(m_acting_but_me),
	       [i_am](const pg_shard_t& shard){return shard != i_am; });

  m_is_replicated = true; // as for now that's the only game in Crimson town // m_pg.get_pool().info.is_replicated();
  m_mode_desc =
    (m_repair ? "repair"sv
	      : (m_depth == scrub_level_t::deep ? "deep-scrub"sv : "scrub"sv));
}


void ScrubBackend::update_repair_status(bool should_repair)
{
  RRLOG(15, (fmt::format("{}: repair state set to: {}", __func__, (should_repair ? "true" : "false")));
//   dout(15) << __func__
// 	   << ": repair state set to :" << (should_repair ? "true" : "false")
// 	   << dendl;
  m_repair = should_repair;
  m_mode_desc =
    (m_repair ? "repair"sv
	      : (m_depth == scrub_level_t::deep ? "deep-scrub"sv : "scrub"sv));
}

void ScrubBackend::new_chunk()
{
  RRLOG(15, __func__);
  this_chunk.emplace();
  this_chunk->maps[m_pg_whoami] = m_primary_map;
}


void ScrubBackend::merge_to_master_set()
{
  RRLOG(15, __func__);
  ceph_assert(this_chunk->master_set.empty() && "the scrubber-BE should be empty");

  // RRR perform in decode_received_map() ?
  for (const auto& i : m_acting_but_me) {
    logger().debug("scrubber: {}: replica {} has {} items", __func__, i,
		   this_chunk->m_received_maps[i].objects.size());
    this_chunk->maps[i] = &this_chunk->m_received_maps[i];
  }

  // Construct the master set of objects
  for (const auto& map : this_chunk->maps) {
    std::transform(map.second->objects.begin(), map.second->objects.end(),
		   std::inserter(this_chunk->master_set, this_chunk->master_set.end()),
		   [](const auto& i) { return i.first; });
  }
}


void ScrubBackend::decode_received_map(pg_shard_t from,
				     const MOSDRepScrubMap& msg,
				     int64_t pool)
{
  logger().debug("scrubber: {}: ({}) decoding map from {}", __func__, m_pg_whoami, from);
  auto p = const_cast<bufferlist&>(msg.get_data()).cbegin();
  this_chunk->m_received_maps[from].decode(p, pool);

  logger().debug("scrubber: {}: decoded map from {}: versions: {}/{}", __func__, from,
		 this_chunk->m_received_maps[from].valid_through, msg.get_map_epoch());
}


// /////////////////////////////////////////////////////////////////////////////
//
//
//
// /////////////////////////////////////////////////////////////////////////////

seastar::future<> ScrubBackend::scrub_compare_maps(
  bool max_reached)  // == scrubber::scrub_compare_maps()
{
  logger().debug("scrubber: {}: analyzing maps", __func__);

  // construct authoritative scrub map for type-specific scrubbing

  m_cleaned_meta_map.insert(*m_primary_map);
  merge_to_master_set();

  // collect some omap statistics

  stringstream ss;
  m_pgbe.omap_checks(this_chunk->maps, this_chunk->master_set, m_omap_stats, ss);
  if (!ss.str().empty()) {
    logger().warn("scrubber: {}: {}", __func__, ss.str());
#ifdef NO_CLUSTER_LOGGER_YET
    m_osds.clog->warn(ss);
#endif
  }

  return seastar::do_with(std::move(ss), [this, max_reached](auto&& ss)  mutable -> seastar::future<> {

    return update_authoritative(ss)
      .then([max_reached, this]() mutable -> seastar::future<> {
	auto for_meta_scrub = clean_meta_map(m_cleaned_meta_map, max_reached);

	// ok, do the pg-type specific scrubbing

	// (Validates consistency of the object info and snap sets)
	scrub_snapshot_metadata(for_meta_scrub, this_chunk->missing_digest);
	// Called here on the primary can use an authoritative map if it isn't the
	// primary
	// RRR _scan_snaps(for_meta_scrub);


#ifdef NOT_YET
	if (!m_scrubber.m_store->empty()) {

		  if (state_test(PG_STATE_REPAIR)) {
		    logger().debug("{}: discarding scrub results", __func__);
		    return m_scrubber.m_store->flush(nullptr);
		  } else {
		    logger().debug("{}: updating scrub object", __func__);
		    ObjectStore::Transaction t;	 // this doesn't make sense
		    return m_scrubber.m_store->flush(&t).then([this, &t] {
		      return m_osds.get_store().do_transaction(m_pg->get_collection_ref(),
							       std::move(t));
		    });
		  }
		} else {
		  return seastar::make_ready_future<>();
		}
#endif

	return seastar::make_ready_future<>();
      });

  });
}


seastar::future<> ScrubBackend::update_authoritative(stringstream& ss)
{
  if (m_acting_but_me.empty()) {
    // no replicas
    return seastar::make_ready_future<>();
  }

  logger().debug("scrubber: {}", __func__);

  ss.str("");	 // RRR?
  ss.clear();

  return compare_smaps(ss).then([this, &ss]() mutable {

    logger().debug(
      "scrubber: ScrubBackend::scrub_compare_maps(): from compare_smaps(): {}", ss.str());

#ifdef NO_CLUSTER_LOGGER_YET
    if (!ss.str().empty()) {
	m_osds.clog->error(ss);
      }
#endif

    for (auto& [obj, peers] : this_chunk->authoritative) {

      list<pair<ScrubMap::object, pg_shard_t>> good_peers;

      for (auto& peer : peers) {
	good_peers.emplace_back(this_chunk->maps[peer]->objects[obj], peer);
      }

      m_scrubber.m_authoritative.emplace(obj, good_peers);
    }

    for (auto i = this_chunk->authoritative.begin();
	 i != this_chunk->authoritative.end(); ++i) {
      m_cleaned_meta_map.objects.erase(i->first);
      m_cleaned_meta_map.objects.insert(
	*(this_chunk->maps[i->second.back()]->objects.find(i->first)));
    }

    return seastar::make_ready_future<>();
  });


}


// /////////////////////////////////////////////////////////////////////////////
//
// components of PGBackend::be_compare_scrubmaps()
//
// /////////////////////////////////////////////////////////////////////////////


// implementation of PGBackend::be_compare_scrubmaps()
seastar::future<> ScrubBackend::compare_smaps(ostream& errstream)
{
  // return ::crimson::do_for_each(this_chunk->master_set, [&, this](const auto& ho) {

  logger().debug("{}: master-set #:{}", __func__, this_chunk->master_set.size());
  return seastar::do_for_each(
    this_chunk->master_set.begin(), this_chunk->master_set.end(),
    [this, &errstream](const auto& ho) { return compare_obj_in_maps(ho, errstream); });
}


std::optional<ScrubBackend::AuthAndObjErrors> ScrubBackend::empty_auth_list(
  std::list<pg_shard_t>&& auths,
  std::set<pg_shard_t>&& obj_errors,
  IterToSMap auth,
  ostream& errstream)
{
  if (auths.empty()) {
    if (obj_errors.empty()) {
      // errstream << pgid.pgid << " soid " << ho
      //	  << " : failed to pick suitable auth object\n";
      return std::nullopt;
    }
    // Object errors exist and nothing in auth_list
    // Prefer the auth shard otherwise take first from list.
    pg_shard_t shard;
    if (obj_errors.count(auth->first)) {
      shard = auth->first;
    } else {
      shard = *(obj_errors.begin());
    }
    auths.push_back(shard);
    obj_errors.erase(shard);
  }

  return std::optional<ScrubBackend::AuthAndObjErrors>{
    ScrubBackend::AuthAndObjErrors{std::move(auths), std::move(obj_errors)}};
}


// called from the refactored
// PGBackend::be_compare_scrubmaps()/PgScrubber::compare_smaps()
seastar::future<> ScrubBackend::compare_obj_in_maps(const hobject_t& ho, ostream& errstream)
{
  // clear per-object data:
  this_chunk->cur_inconsistent.clear();
  this_chunk->cur_missing.clear();
  this_chunk->fix_digest = false;

  return seastar::do_with(
    object_info_t{},			    // TBdoc
    map<pg_shard_t, shard_info_wrapper>{},  // TBdoc
    inconsistent_obj_wrapper{ho},	    // TBdoc
    bool{},  // digest_match: true if all (existing) digests match
    [this, &ho, &errstream](auto&& auth_oi, auto&& shard_map, auto&& object_error,
			    auto&& digest_match) mutable -> seastar::future<> {
      // select_auth_object() returns a an iter into one of this_chunk->maps.
      return m_pgbe
	.select_auth_object(ho, this_chunk->maps, &auth_oi, shard_map, digest_match,
			    m_pg_id, errstream)
	.safe_then(
	  [&, this](auto&& auth) mutable -> seastar::future<> {

	    if (auth == this_chunk->maps.end()) {
	      // no auth selected
	      object_error.set_version(0);
	      object_error.set_auth_missing(ho, this_chunk->maps, shard_map,
					    m_scrubber.m_shallow_errors,
					    m_scrubber.m_deep_errors, m_pg_whoami);
	      if (object_error.has_deep_errors())
		++m_scrubber.m_deep_errors;
	      else if (object_error.has_shallow_errors())
		++m_scrubber.m_shallow_errors;
	      m_scrubber.m_store->add_object_error(ho.pool, object_error);
	      errstream << m_scrubber.m_pg_id.pgid << " soid " << ho
			<< " : failed to pick suitable object info\n";
	      return seastar::make_ready_future<>();
	    }

	    // an auth source was selected

	    object_error.set_version(auth_oi.user_version);
	    ScrubMap::object& auth_object = auth->second->objects[ho];
	    this_chunk->fix_digest = false;  // RRR needed?

	    auto [auths, objs] = match_in_shards(auth, ho, auth_oi, object_error,
						 shard_map, digest_match, errstream);
	    auto opt_ers =
	      empty_auth_list(std::move(auths), std::move(objs), auth, errstream);

	    if (opt_ers.has_value()) {

	      // At this point auth_list is populated, so we add the object error
	      // shards as inconsistent.
	      return inconsistents(ho, auth_object, auth_oi, std::move(*opt_ers),
				   errstream);

	    } else {
	      // both the auth & errs containers are empty
	      errstream << m_scrubber.m_pg_id.pgid << " soid " << ho
			<< " : failed to pick suitable authoritative object\n";
	      return seastar::make_ready_future<>();
	    }
	  },
	  PGBackend::ll_read_errorator::all_same_way([]() {
	    // RRR complete the error handling here

	    logger().debug("ScrubBackend::compare_obj_in_maps() in error handling");
	    return seastar::make_ready_future<>();
	  }));
    });
}


// RRR no need to return a future, it seems.
seastar::future<> ScrubBackend::inconsistents(
  const hobject_t& ho,
  ScrubMap::object& auth_object,
  object_info_t& auth_oi,  // RRR move to object?
  AuthAndObjErrors&& auth_n_errs,
  ostream& errstream)
{
  auto& object_errors = get<1>(auth_n_errs);
  auto& auth_list = get<0>(auth_n_errs);

  this_chunk->cur_inconsistent.insert(object_errors.begin(), object_errors.end());
  if (!this_chunk->cur_missing.empty()) {
    m_missing[ho] = this_chunk->cur_missing;
  }
  if (!this_chunk->cur_inconsistent.empty()) {
    m_inconsistent[ho] = this_chunk->cur_inconsistent;
  }

  if (this_chunk->fix_digest) {

    ceph_assert(auth_object.digest_present);
    std::optional<uint32_t> data_digest{auth_object.digest};

    std::optional<uint32_t> omap_digest;
    if (auth_object.omap_digest_present) {
      omap_digest = auth_object.omap_digest;
    }
    this_chunk->missing_digest[ho] = make_pair(data_digest, omap_digest);
  }

  if (!this_chunk->cur_inconsistent.empty() || !this_chunk->cur_missing.empty()) {

    this_chunk->authoritative[ho] = auth_list;

  } else if (!this_chunk->fix_digest && m_is_replicated) {

    auto is_to_fix = should_fix_digest(ho, auth_object, auth_oi, m_repair, errstream);

    switch (is_to_fix) {

      case digest_fixing_t::no:
        break;

      case digest_fixing_t::if_aged:
      {
	utime_t age = this_chunk->started - auth_oi.local_mtime;

	// \todo find out 'age_limit' only once
	auto age_limit = m_scrubber.get_pg_cct()->_conf->osd_deep_scrub_update_digest_min_age;

	if (age <= age_limit) {

	  logger().info("{}: missing digest but age ({}) < conf ({})  on {}", __func__, age,
	    age_limit, ho);
	  break;
	}
      }

      [[fallthrough]];

      case digest_fixing_t::force:

	std::optional<uint32_t> data_digest;
	if (auth_object.digest_present) {
	  data_digest = auth_object.digest;
	  logger().info("{}: will update data digest on {}", __func__, ho);
	}

	std::optional<uint32_t> omap_digest;
	if (auth_object.omap_digest_present) {
	  omap_digest = auth_object.omap_digest;
	  logger().info("{}: will update omap digest on {}", __func__, ho);
	}
	this_chunk->missing_digest[ho] = make_pair(data_digest, omap_digest);
	break;
    }
  }

  return seastar::make_ready_future<>();
}

ScrubBackend::digest_fixing_t ScrubBackend::should_fix_digest(const hobject_t& ho, const ScrubMap::object& auth_object,
			   const object_info_t& auth_oi, bool repair_flag, ostream& errstream)
{
  digest_fixing_t update{digest_fixing_t::no};

  if (auth_object.digest_present && !auth_oi.is_data_digest()) {
    logger().info("{}: missing data digest on {}", __func__, ho);
    update = digest_fixing_t::if_aged;
  }

  if (auth_object.omap_digest_present && !auth_oi.is_omap_digest()) {
    logger().info("{}: missing omap digest on {}", __func__, ho);
    update = digest_fixing_t::if_aged;
  }

  // recorded digest != actual digest?
  if (auth_oi.is_data_digest() && auth_object.digest_present &&
      auth_oi.data_digest != auth_object.digest) {
    // ceph_assert(shard_map[auth->first].has_data_digest_mismatch_info());
    errstream << m_pg_id << " recorded data digest 0x" << std::hex
	      << auth_oi.data_digest << " != on disk 0x" << auth_object.digest
	      << std::dec << " on " << auth_oi.soid << "\n";
    if (repair_flag)
      update = digest_fixing_t::force;
  }

  if (auth_oi.is_omap_digest() && auth_object.omap_digest_present &&
      auth_oi.omap_digest != auth_object.omap_digest) {
    // ceph_assert(shard_map[auth->first].has_omap_digest_mismatch_info());
    errstream << m_pg_id << " recorded omap digest 0x" << std::hex
	      << auth_oi.omap_digest << " != on disk 0x" << auth_object.omap_digest
	      << std::dec << " on " << auth_oi.soid << "\n";
    if (repair_flag)
      update = digest_fixing_t::force;
  }

  return update;
}


// seastar::future<ScrubBackend::AuthAndObjErrors>
ScrubBackend::AuthAndObjErrors ScrubBackend::match_in_shards(
  IterToSMap auth,
  const hobject_t& ho,
  object_info_t& auth_oi,
  inconsistent_obj_wrapper& obj_result,
  map<pg_shard_t, shard_info_wrapper>& shard_map,
  bool digest_match,
  ostream& errstream)
{
  std::list<pg_shard_t> auth_list;     // to be returned
  std::set<pg_shard_t> object_errors;  // to be returned

  for (auto j = this_chunk->maps.cbegin(); j != this_chunk->maps.cend(); ++j) {

    if (j == auth) { // why can't I compare the shard # instead of comparing the iter? RRR to test in Classic
      shard_map[auth->first].selected_oi = true;
    }

    if (j->second->objects.count(ho)) {

      shard_map[j->first].set_object(j->second->objects[ho]);

      logger().debug("{}: ho: {} ^^^^ {} ^^^^ {}", __func__, ho.to_str(), ho.is_head(), ho.has_snapset());

      // Compare
      stringstream ss;
      auto& auth_object = auth->second->objects[ho];
      const bool discrep_found =
	compare_obj_details(auth->first, auth_object, auth_oi, j->second->objects[ho],
			    shard_map[j->first], obj_result, ss, false && ho.has_snapset());

      logger().debug("{}: {} {} {} shards {}{}{}", __func__,
      		(m_repair?"repair":" "),
      		(m_is_replicated ? "replicated" : ""),
		(j == auth ? "auth" : ""),
		shard_map.size(),
		(digest_match ? " digest_match " : " "),
		(shard_map[j->first].only_data_digest_mismatch_info() ? "'info mismatch info'" : "")
		);

      // If all replicas match, but they don't match object_info we can
      // repair it by using missing_digest mechanism
      if (m_repair && m_is_replicated && j == auth &&
	  shard_map.size() > 1 && digest_match &&
	  shard_map[j->first].only_data_digest_mismatch_info() &&
	  auth_object.digest_present) {
	// Set in missing_digests
	this_chunk->fix_digest = true;
	// Clear the error
	shard_map[j->first].clear_data_digest_mismatch_info();
	errstream << m_pg_id << " soid " << ho << " : repairing object info data_digest"
		  << "\n";
      }

      // Some errors might have already been set in select_auth_object()
      if (shard_map[j->first].errors != 0) {

	this_chunk->cur_inconsistent.insert(j->first);
	if (shard_map[j->first].has_deep_errors())
	  ++m_scrubber.m_deep_errors;
	else
	  ++m_scrubber.m_shallow_errors;

	if (discrep_found) {
	  // Only true if be_compare_scrub_objects() found errors and put something
	  // in ss.
	  errstream << m_pg_id << " shard " << j->first << " soid " << ho << " : "
		    << ss.str() << "\n";
	}

      } else if (discrep_found) {

	// Track possible shards to use as authoritative, if needed

	// There are errors, without identifying the shard
	object_errors.insert(j->first);
	errstream << m_pg_id << " soid " << ho << " : " << ss.str() << "\n";

      } else {

	// XXX: The auth shard might get here that we don't know
	// that it has the "correct" data.
	auth_list.push_back(j->first);
      }

    } else {

      this_chunk->cur_missing.insert(j->first);
      shard_map[j->first].set_missing();
      // shard_map[j->first].primary = (j->first == get_parent()->whoami_shard());
      shard_map[j->first].primary = (j->first == m_pg_whoami);

      // Can't have any other errors if there is no information available
      ++m_scrubber.m_shallow_errors;
      errstream << m_pg_id << " shard " << j->first << " " << ho << " : missing\n";
    }
    obj_result.add_shard(j->first, shard_map[j->first]);
  }

  logger().debug("{}: auth_list:{} #:{}; obj-errs#:{}", __func__, auth_list, auth_list.size(),
		 object_errors.size());
  return {auth_list, object_errors};
}

/*
 * Process:
 * Building a map of objects suitable for snapshot validation.
 * The data in m_cleaned_meta_map is the leftover partial items that need to
 * be completed before they can be processed.
 *
 * Snapshots in maps precede the head object, which is why we are scanning
 * backwards.
 */
ScrubMap ScrubBackend::clean_meta_map(ScrubMap& cleaned, bool max_reached)
{
  ScrubMap for_meta_scrub;

  if (max_reached || cleaned.objects.empty()) {
    cleaned.swap(for_meta_scrub);
  } else {
    auto iter = cleaned.objects.end();
    --iter;  // not empty, see 'if' clause
    auto begin = cleaned.objects.begin();
    if (iter->first.has_snapset()) {
      ++iter;
    } else {
      while (iter != begin) {
	auto next = iter--;
	if (next->first.get_head() != iter->first.get_head()) {
	  ++iter;
	  break;
	}
      }
    }
    for_meta_scrub.objects.insert(begin, iter);
    cleaned.objects.erase(begin, iter);
  }

  return for_meta_scrub;
}

inline static const char* sep(bool& prev_err)
{
  if (prev_err) {
    return ", ";
  } else {
    prev_err = true;
    return "";
  }
}

// == PGBackend::be_compare_scrub_objects()
bool ScrubBackend::compare_obj_details(pg_shard_t auth_shard,
				     const ScrubMap::object& auth,
				     const object_info_t& auth_oi,
				     const ScrubMap::object& candidate,
				     shard_info_wrapper& shard_result,
				     inconsistent_obj_wrapper& obj_result,
				     ostream& errstream,
				     bool has_snapset)
{
  fmt::memory_buffer out;
  bool error{false};

  // ------------------------------------------------------------------------

  if (auth.digest_present && candidate.digest_present &&
      auth.digest != candidate.digest) {
    format_to(out, "data_digest {:x} != data_digest {:x} from shard {}", candidate.digest,
	      auth.digest, auth_shard);
    error = true;
    obj_result.set_data_digest_mismatch();
  }

  if (auth.omap_digest_present && candidate.omap_digest_present &&
      auth.omap_digest != candidate.omap_digest) {
    format_to(out, "{}omap_digest {:x} != omap_digest {:x} from shard {}", sep(error),
	      candidate.omap_digest, auth.omap_digest, auth_shard);
    obj_result.set_omap_digest_mismatch();
  }

  // for replicated:
  if (auth_oi.is_data_digest() && candidate.digest_present &&
      auth_oi.data_digest != candidate.digest) {
    format_to(out, "{}data_digest {:x} != data_digest {:x} from auth oi {}", sep(error),
	      candidate.digest, auth_oi.data_digest, auth_oi);
    shard_result.set_data_digest_mismatch_info();
  }

  // for replicated:
  if (auth_oi.is_omap_digest() && candidate.omap_digest_present &&
      auth_oi.omap_digest != candidate.omap_digest_present) {
    format_to(out, "{}omap_digest {:x} != omap_digest {:x} from auth oi {}", sep(error),
	      candidate.digest, auth_oi.data_digest, auth_oi);
    shard_result.set_omap_digest_mismatch_info();
  }

  // ------------------------------------------------------------------------

  // RRR why adding a ',' here?

  if (candidate.stat_error) {
    error = true;  // RRR ?????????????????
  }

  // ------------------------------------------------------------------------

  if (!shard_result.has_info_missing() && !shard_result.has_info_corrupted()) {

    auto can_attr = candidate.attrs.find(OI_ATTR);
    ceph_assert(can_attr != candidate.attrs.end());
    bufferlist can_bl;
    can_bl.push_back(can_attr->second);

    auto auth_attr = auth.attrs.find(OI_ATTR);
    ceph_assert(auth_attr != auth.attrs.end());
    bufferlist auth_bl;
    auth_bl.push_back(auth_attr->second);

    if (!can_bl.contents_equal(auth_bl)) {
      format_to(out, "{}object info inconsistent ", sep(error));
      obj_result.set_object_info_inconsistency();
    }
  }

  if (has_snapset) {
    if (!shard_result.has_snapset_missing() && !shard_result.has_snapset_corrupted()) {

      auto can_attr = candidate.attrs.find(SS_ATTR);
      ceph_assert(can_attr != candidate.attrs.end());
      bufferlist can_bl;
      can_bl.push_back(can_attr->second);

      auto auth_attr = auth.attrs.find(SS_ATTR);
      ceph_assert(auth_attr != auth.attrs.end());
      bufferlist auth_bl;
      auth_bl.push_back(auth_attr->second);

      if (!can_bl.contents_equal(auth_bl)) {
	format_to(out, "{}snapset inconsistent ", sep(error));
	obj_result.set_snapset_inconsistency();
      }
    }
  }

  // ------------------------------------------------------------------------

  // \todo handle EC part

#ifdef NOT_YET_EC
  if (parent->get_pool().is_erasure()) {
    if (!shard_result.has_hinfo_missing() && !shard_result.has_hinfo_corrupted()) {
      bufferlist can_bl, auth_bl;
      auto can_hi = candidate.attrs.find(ECUtil::get_hinfo_key());
      auto auth_hi = auth.attrs.find(ECUtil::get_hinfo_key());

      ceph_assert(auth_hi != auth.attrs.end());
      ceph_assert(can_hi != candidate.attrs.end());

      can_bl.push_back(can_hi->second);
      auth_bl.push_back(auth_hi->second);
      if (!can_bl.contents_equal(auth_bl)) {
	if (error != CLEAN)
	  errorstream << ", ";
	error = FOUND_ERROR;
	obj_result.set_hinfo_inconsistency();
	errorstream << "hinfo inconsistent ";
      }
    }
  }
#endif

  // ------------------------------------------------------------------------

  // sizes:

  uint64_t oi_size = auth_oi.size;  // be_get_ondisk_size(auth_oi.size); // RRR find the equivalent
  if (oi_size != candidate.size) {
    format_to(out, "{}size {} != size {} from auth oi {}", sep(error), candidate.size,
	      oi_size, auth_oi);
    shard_result.set_size_mismatch_info();
  }

  if (auth.size != candidate.size) {
    format_to(out, "{}size {} != size {} from shard {}", sep(error), candidate.size,
	      auth.size, auth_shard);
    obj_result.set_size_mismatch();
  }

  // If the replica is too large and we didn't already count it for this object

  // RRR cache the conf value in the object
  if (candidate.size > m_scrubber.get_pg_cct()->_conf->osd_max_object_size &&
      !obj_result.has_size_too_large()) {

    format_to(out, "{}size {} (> {}) is too large", sep(error), candidate.size,
	      m_scrubber.get_pg_cct()->_conf->osd_max_object_size);
    obj_result.set_size_too_large();
  }

  // ------------------------------------------------------------------------

  // comparing the attributes:

  for (const auto& [k, v] : auth.attrs) {
    if (k == OI_ATTR || k[0] != '_') {
      // We check system keys separately
      continue;
    }

    auto cand = candidate.attrs.find(k);
    if (cand == candidate.attrs.end()) {
      format_to(out, "{}attr name mismatch '{}'", sep(error), k);
      obj_result.set_attr_name_mismatch();
    } else if (cand->second.cmp(v)) {
      format_to(out, "{}attr value mismatch '{}'", sep(error), k);
      obj_result.set_attr_value_mismatch();
    }
  }

  for (const auto& [k, v] : candidate.attrs) {
    if (k == OI_ATTR || k[0] != '_') {
      // We check system keys separately
      continue;
    }

    auto in_auth = auth.attrs.find(k);
    if (in_auth == auth.attrs.end()) {
      format_to(out, "{}attr name mismatch '{}'", sep(error), k);
      obj_result.set_attr_name_mismatch();
    }
  }

  errstream << fmt::to_string(out);
  return error;
}


// /////////////////////////////////////////////////////////////////////////////
//
// final checking & fixing - scrub_snapshot_metadata()
//
// /////////////////////////////////////////////////////////////////////////////

/*
 * Validate consistency of the object info and snap sets.
 *
 * We are sort of comparing 2 lists. The main loop is on objmap.objects. But
 * the comparison of the objects is against multiple snapset.clones. There are
 * multiple clone lists and in between lists we expect head.
 *
 * Example
 *
 * objects              expected
 * =======              =======
 * obj1 snap 1          head, unexpected obj1 snap 1
 * obj2 head            head, match
 *              [SnapSet clones 6 4 2 1]
 * obj2 snap 7          obj2 snap 6, unexpected obj2 snap 7
 * obj2 snap 6          obj2 snap 6, match
 * obj2 snap 4          obj2 snap 4, match
 * obj3 head            obj2 snap 2 (expected), obj2 snap 1 (expected), match
 *              [Snapset clones 3 1]
 * obj3 snap 3          obj3 snap 3 match
 * obj3 snap 1          obj3 snap 1 match
 * obj4 head            head, match
 *              [Snapset clones 4]
 * EOL                  obj4 snap 4, (expected)
 */
void ScrubBackend::scrub_snapshot_metadata(ScrubMap& map,
					 const missing_map_t& missing_digest)
{
#ifdef NOT_YET_SNAPS
  // dout(10) << __func__ << " num stat obj " << m_pl_pg->info.stats.stats.sum.num_objects
  //   << dendl;

  auto& info = m_pl_pg->info;
  const PGPool& pool = m_pl_pg->pool;
  bool allow_incomplete_clones = pool.info.allow_incomplete_clones();

  bool repair = state_test(PG_STATE_REPAIR);
  bool deep_scrub = state_test(PG_STATE_DEEP_SCRUB);
  const char* mode = (repair ? "repair" : (deep_scrub ? "deep-scrub" : "scrub"));

  std::optional<snapid_t> all_clones;  // Unspecified snapid_t or std::nullopt

  // traverse in reverse order.
  std::optional<hobject_t> head;
  std::optional<SnapSet> snapset;		// If initialized so will head (above)
  vector<snapid_t>::reverse_iterator curclone;	// Defined only if snapset initialized
  int missing = 0;
  inconsistent_snapset_wrapper soid_error, head_error;
  int soid_error_count = 0;

  for (auto p = scrubmap.objects.rbegin(); p != scrubmap.objects.rend(); ++p) {

    const hobject_t& soid = p->first;
    ceph_assert(!soid.is_snapdir());
    soid_error = inconsistent_snapset_wrapper{soid};
    object_stat_sum_t stat;
    std::optional<object_info_t> oi;

    stat.num_objects++;

    if (soid.nspace == m_pl_pg->cct->_conf->osd_hit_set_namespace)
      stat.num_objects_hit_set_archive++;

    if (soid.is_snap()) {
      // it's a clone
      stat.num_object_clones++;
    }

    // basic checks.
    if (p->second.attrs.count(OI_ATTR) == 0) {
      oi = std::nullopt;
      m_osds->clog->error() << mode << " " << info.pgid << " " << soid << " : no '"
			    << OI_ATTR << "' attr";
      ++m_shallow_errors;
      soid_error.set_info_missing();
    } else {
      bufferlist bv;
      bv.push_back(p->second.attrs[OI_ATTR]);
      try {
	oi = object_info_t(bv);
      } catch (ceph::buffer::error& e) {
	oi = std::nullopt;
	m_osds->clog->error() << mode << " " << info.pgid << " " << soid
			      << " : can't decode '" << OI_ATTR << "' attr " << e.what();
	++m_shallow_errors;
	soid_error.set_info_corrupted();
	soid_error.set_info_missing();	// Not available too
      }
    }

    if (oi) {
      if (m_pl_pg->pgbackend->be_get_ondisk_size(oi->size) != p->second.size) {
	m_osds->clog->error() << mode << " " << info.pgid << " " << soid
			      << " : on disk size (" << p->second.size
			      << ") does not match object info size (" << oi->size
			      << ") adjusted for ondisk to ("
			      << m_pl_pg->pgbackend->be_get_ondisk_size(oi->size) << ")";
	soid_error.set_size_mismatch();
	++m_shallow_errors;
      }

      dout(20) << mode << "  " << soid << " " << *oi << dendl;

      // A clone num_bytes will be added later when we have snapset
      if (!soid.is_snap()) {
	stat.num_bytes += oi->size;
      }
      if (soid.nspace == m_pl_pg->cct->_conf->osd_hit_set_namespace)
	stat.num_bytes_hit_set_archive += oi->size;

      if (oi->is_dirty())
	++stat.num_objects_dirty;
      if (oi->is_whiteout())
	++stat.num_whiteouts;
      if (oi->is_omap())
	++stat.num_objects_omap;
      if (oi->is_cache_pinned())
	++stat.num_objects_pinned;
      if (oi->has_manifest())
	++stat.num_objects_manifest;
    }

    // Check for any problems while processing clones
    if (doing_clones(snapset, curclone)) {
      std::optional<snapid_t> target;
      // Expecting an object with snap for current head
      if (soid.has_snapset() || soid.get_head() != head->get_head()) {

	dout(10) << __func__ << " " << mode << " " << info.pgid << " new object " << soid
		 << " while processing " << *head << dendl;

	target = all_clones;
      } else {
	ceph_assert(soid.is_snap());
	target = soid.snap;
      }

      // Log any clones we were expecting to be there up to target
      // This will set missing, but will be a no-op if snap.soid == *curclone.
      missing +=
	process_clones_to(head, snapset, m_osds->clog, info.pgid, mode,
			  allow_incomplete_clones, target, &curclone, head_error);
    }

    bool expected;
    // Check doing_clones() again in case we ran process_clones_to()
    if (doing_clones(snapset, curclone)) {
      // A head would have processed all clones above
      // or all greater than *curclone.
      ceph_assert(soid.is_snap() && *curclone <= soid.snap);

      // After processing above clone snap should match the expected curclone
      expected = (*curclone == soid.snap);
    } else {
      // If we aren't doing clones any longer, then expecting head
      expected = soid.has_snapset();
    }
    if (!expected) {
      // If we couldn't read the head's snapset, just ignore clones
      if (head && !snapset) {
	m_osds->clog->error() << mode << " " << info.pgid << " " << soid
			      << " : clone ignored due to missing snapset";
      } else {
	m_osds->clog->error() << mode << " " << info.pgid << " " << soid
			      << " : is an unexpected clone";
      }
      ++m_shallow_errors;
      soid_error.set_headless();
      m_store->add_snap_error(pool.id, soid_error);
      ++soid_error_count;
      if (head && soid.get_head() == head->get_head())
	head_error.set_clone(soid.snap);
      continue;
    }

    // new snapset?
    if (soid.has_snapset()) {

      if (missing) {
	log_missing(missing, head, m_osds->clog, info.pgid, __func__, mode,
		    pool.info.allow_incomplete_clones());
      }

      // Save previous head error information
      if (head && (head_error.errors || soid_error_count))
	m_store->add_snap_error(pool.id, head_error);
      // Set this as a new head object
      head = soid;
      missing = 0;
      head_error = soid_error;
      soid_error_count = 0;

      dout(20) << __func__ << " " << mode << " new head " << head << dendl;

      if (p->second.attrs.count(SS_ATTR) == 0) {
	m_osds->clog->error() << mode << " " << info.pgid << " " << soid << " : no '"
			      << SS_ATTR << "' attr";
	++m_shallow_errors;
	snapset = std::nullopt;
	head_error.set_snapset_missing();
      } else {
	bufferlist bl;
	bl.push_back(p->second.attrs[SS_ATTR]);
	auto blp = bl.cbegin();
	try {
	  snapset = SnapSet();	// Initialize optional<> before decoding into it
	  decode(*snapset, blp);
	  head_error.ss_bl.push_back(p->second.attrs[SS_ATTR]);
	} catch (ceph::buffer::error& e) {
	  snapset = std::nullopt;
	  m_osds->clog->error()
	    << mode << " " << info.pgid << " " << soid << " : can't decode '" << SS_ATTR
	    << "' attr " << e.what();
	  ++m_shallow_errors;
	  head_error.set_snapset_corrupted();
	}
      }

      if (snapset) {
	// what will be next?
	curclone = snapset->clones.rbegin();

	if (!snapset->clones.empty()) {
	  dout(20) << "  snapset " << *snapset << dendl;
	  if (snapset->seq == 0) {
	    m_osds->clog->error()
	      << mode << " " << info.pgid << " " << soid << " : snaps.seq not set";
	    ++m_shallow_errors;
	    head_error.set_snapset_error();
	  }
	}
      }
    } else {
      ceph_assert(soid.is_snap());
      ceph_assert(head);
      ceph_assert(snapset);
      ceph_assert(soid.snap == *curclone);

      dout(20) << __func__ << " " << mode << " matched clone " << soid << dendl;

      if (snapset->clone_size.count(soid.snap) == 0) {
	m_osds->clog->error() << mode << " " << info.pgid << " " << soid
			      << " : is missing in clone_size";
	++m_shallow_errors;
	soid_error.set_size_mismatch();
      } else {
	if (oi && oi->size != snapset->clone_size[soid.snap]) {
	  m_osds->clog->error()
	    << mode << " " << info.pgid << " " << soid << " : size " << oi->size
	    << " != clone_size " << snapset->clone_size[*curclone];
	  ++m_shallow_errors;
	  soid_error.set_size_mismatch();
	}

	if (snapset->clone_overlap.count(soid.snap) == 0) {
	  m_osds->clog->error() << mode << " " << info.pgid << " " << soid
				<< " : is missing in clone_overlap";
	  ++m_shallow_errors;
	  soid_error.set_size_mismatch();
	} else {
	  // This checking is based on get_clone_bytes().  The first 2 asserts
	  // can't happen because we know we have a clone_size and
	  // a clone_overlap.  Now we check that the interval_set won't
	  // cause the last assert.
	  uint64_t size = snapset->clone_size.find(soid.snap)->second;
	  const interval_set<uint64_t>& overlap =
	    snapset->clone_overlap.find(soid.snap)->second;
	  bool bad_interval_set = false;
	  for (interval_set<uint64_t>::const_iterator i = overlap.begin();
	       i != overlap.end(); ++i) {
	    if (size < i.get_len()) {
	      bad_interval_set = true;
	      break;
	    }
	    size -= i.get_len();
	  }

	  if (bad_interval_set) {
	    m_osds->clog->error() << mode << " " << info.pgid << " " << soid
				  << " : bad interval_set in clone_overlap";
	    ++m_shallow_errors;
	    soid_error.set_size_mismatch();
	  } else {
	    stat.num_bytes += snapset->get_clone_bytes(soid.snap);
	  }
	}
      }

      // what's next?
      ++curclone;
      if (soid_error.errors) {
	m_store->add_snap_error(pool.id, soid_error);
	++soid_error_count;
      }
    }
    m_scrub_cstat.add(stat);
  }

  if (doing_clones(snapset, curclone)) {
    dout(10) << __func__ << " " << mode << " " << info.pgid
	     << " No more objects while processing " << *head << dendl;

    missing +=
      process_clones_to(head, snapset, m_osds->clog, info.pgid, mode,
			allow_incomplete_clones, all_clones, &curclone, head_error);
  }

  // There could be missing found by the test above or even
  // before dropping out of the loop for the last head.
  if (missing) {
    log_missing(missing, head, m_osds->clog, info.pgid, __func__, mode,
		allow_incomplete_clones);
  }
  if (head && (head_error.errors || soid_error_count))
    m_store->add_snap_error(pool.id, head_error);
#endif

  record_object_digests(0);

  logger().debug("scrubber: {}: (mode:{}) finished", __func__, "XXX" /*mode*/);
}

// missing_digest:   std::map<hobject_t, std::pair<std::optional<uint32_t>,
// std::optional<uint32_t>>>
//  mapping objs into a pair of data & omap numbers.

void ScrubBackend::record_object_digests(int missing)
{
  logger().debug("{}: {} ({})", __func__, missing, this_chunk->missing_digest.size());

  for_each(this_chunk->missing_digest.cbegin(), this_chunk->missing_digest.cend(),
	   [&/*, this*/](const auto& kv) {
	     const auto& [obj, digests] = kv;
	     const auto& data_digest = digests.first;
	     const auto& omap_digest = digests.second;

	     logger().debug(
	       "ScrubBackend::record_object_digests(): recording digests for {} ({}, {})",
	       obj, data_digest, omap_digest);

	     // fetch the object context (from cache or disk)

	     // \todo

	     // create the op-context for the cluster update operation

	     // ctx->at_version = m_pl_pg->get_next_version();

	     // ctx->mtime = utime_t{};  // do not update mtime
	     // if (data_digest) {
	     //   ctx->new_obs.oi.set_data_digest(*data_digest);
	     // } else {
	     //   ctx->new_obs.oi.clear_data_digest();
	     // }

	     // if (omap_digest) {
	     //   ctx->new_obs.oi.set_omap_digest(*omap_digest);
	     // } else {
	     //   ctx->new_obs.oi.clear_omap_digest();
	     // }

	     // pg->finish_ctx(ctx.get(), pg_log_entry_t::MODIFY);

	     // ++m_scrubber.num_digest_updates_pending;

	     // ctx->register_on_success([this]() {
	     //   logger().debug("ScrubBackend::record_object_digests(): updating scrub
	     //   digest ({})",
	     //       m_scrubber.num_digest_updates_pending);
	     // 	 if (--m_scrubber.num_digest_updates_pending <= 0) {
	     //      m_osds->queue_scrub_digest_update(m_pl_pg,
	     //      m_pl_pg->is_scrub_blocking_ops());
	     //   }
	     //	});

	     // m_pl_pg->simple_opc_submit(std::move(ctx));
	   });
}


//}  // namespace crimson::osd::Scrub
