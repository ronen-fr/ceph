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


enum class get_snaps_code_t {  /// \todo RRR move into get_snaps_result_t
  SUCCESS,
  BACKEND_ERROR,
  NOT_FOUND,
  INCONSISTENT
};

struct get_snaps_result_t {
  get_snaps_code_t code{get_snaps_code_t::SUCCESS};
  int backend_error{0};	 ///< errno returned by the backend
};

struct SnapMapperAccessor {
  // deprecated. Used in tests.
  virtual int get_snaps(const hobject_t& hoid,
			std::set<snapid_t>* snaps_set) const = 0;

  virtual tl::expected<std::set<snapid_t>, get_snaps_result_t> get_snaps(
    const hobject_t& hoid) const = 0;

  /// \toto RRR document
  virtual tl::expected<std::set<snapid_t>, get_snaps_result_t>
  get_verified_snaps(const hobject_t& hoid) const = 0;

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
