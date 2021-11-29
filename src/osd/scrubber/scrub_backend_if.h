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
  virtual ~ScrubBackendIF() = default;

  /**
   * reset the per-chunk data structure (ScrubberBeChunk).
   * Create an empty scrub-map for this shard, and place it
   * in the appropriate entry in 'received_maps'.
   *
   * @returns a pointer to the newly created ScrubMap.
   */
  virtual ScrubMap* new_chunk() = 0;

  /**
   * sets Backend's m_repair flag (setting m_mode_desc to a corresponding
   * string)
   */
  virtual void update_repair_status(bool should_repair) = 0;

  /**
   * decode the arriving MOSDRepScrubMap message, placing the replica's
   * scrub-map into received_maps[from].
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

  virtual void replica_clean_meta(ScrubMap& smap,
                                  bool max_reached,
                                  const hobject_t& start) = 0;

  // tbd - stats handling

  virtual int get_num_digest_updates_pending() const = 0;

  virtual void repair_oinfo_oid(ScrubMap& smap) = 0;
};
