// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
 * \file
 * \brief Defines the interface for the snap-mapper used by the scrubber.
 */
#include <set>

#include "common/scrub_types.h"
#include "include/expected.hpp"

namespace Scrub {
/*
 * snaps-related aux structures:
 * the scrub-backend scans the snaps associated with each scrubbed object, and
 * fixes corrupted snap-sets.
 * The actual access to the PG's snap_mapper, and the actual I/O transactions,
 * are performed by the main PgScrubber object.
 * the following aux structures are used to facilitate the required exchanges:
 * - pre-fix snap-sets are accessed by the scrub-backend, and:
 * - a list of fix-orders (either insert or replace operations) are returned
 */

struct mapper_req_t {
  enum class code_t { success, backend_error, not_found, inconsistent };
  code_t code{code_t::success};
  int backend_error{0};	 ///< errno returned by the backend
};
}  // namespace Scrub

struct SnapMapperAccessor {
  using mapper_req_t = Scrub::mapper_req_t;
  /**
   *  get SnapMapper's snap-set for a given object
   *  \returns a set of snaps, or an error code
   *  \attn: only OBJ_ DB entries are consulted
   */
  virtual tl::expected<std::set<snapid_t>, mapper_req_t> get_snaps(
    const hobject_t& hoid) const = 0;

  /**
   *  get SnapMapper's snap-set for a given object.
   *  The snaps gleaned from the OBJ_ entry are verified against the
   *  mapping ('SNA_') entries.
   *  A mismatch between both sets of entries will result in an error.
   *  \returns a set of snaps, or an error code.
   */
  virtual tl::expected<std::set<snapid_t>, mapper_req_t> get_verified_snaps(
    const hobject_t& hoid) const = 0;

  virtual ~SnapMapperAccessor() = default;
};

enum class snap_mapper_op_t {
  add,
  update,
  overwrite,  //< the mapper's data is internally inconsistent. Similar
	      //<  to an 'update' operation, but the logs are different.
};

struct snap_mapper_fix_t {
  snap_mapper_op_t op;
  hobject_t hoid;
  std::set<snapid_t> snaps;
  std::set<snapid_t> wrong_snaps;  // only collected & returned for logging sake
};
