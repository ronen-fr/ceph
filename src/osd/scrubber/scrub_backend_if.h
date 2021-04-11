// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <list>
#include <map>
#include <optional>
#include <set>
#include <utility>

#include "common/scrub_types.h"

class PgScrubber;
class PGBackend;
struct ScrubMap;

/**
 * The scrubber's interface into its ScrubBackend object.
 *
 * ScrubBackend wraps the data required for the back-end part of the scrubbing:
 * comparing the maps and fixing objects.
 */
class ScrubBackendIF {
 public:
  using ObjPeersList =
    std::map<hobject_t, std::list<std::pair<ScrubMap::object, pg_shard_t>>>;

  virtual ~ScrubBackendIF() = default;

  /**
   * reset the per-chunk data structure (ScrubberBeChunk),
   * and attach the m_primary_map to it.
   */
  virtual void new_chunk() = 0;

  // RRR complete doc
  virtual void update_repair_status(bool should_repair) = 0;

  /**
   * decode the arriving MOSDRepScrubMap message, placing the replica's
   * scrub-map into m_received_maps[from].
   *
   * @param from replica
   * @param pool TBD
   */
  virtual void decode_received_map(pg_shard_t from,
                                   const MOSDRepScrubMap& msg,
                                   int64_t pool) = 0;

  virtual void scrub_compare_maps(bool max_point_reached) = 0;

  /**
   * Go over m_authoritative - the list of "objects in distress" (i.e. missing
   * or inconsistent) that we do have an authoritative copy of - and fix them.
   *
   * @return the number of objects fixed
   */
  virtual int scrub_process_inconsistent() = 0;

  virtual void scan_snaps(ScrubMap& smap) = 0;

  // tbd - stats handling

  virtual int get_num_digest_updates_pending() const = 0;

  virtual void repair_oinfo_oid(ScrubMap& smap) = 0;
};
