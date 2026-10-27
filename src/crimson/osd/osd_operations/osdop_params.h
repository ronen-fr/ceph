#pragma once

#include <optional>

#include "messages/MOSDOp.h"
#include "osd/osd_types.h"
#include "crimson/common/type_helpers.h"

// The fields in this struct are parameters that may be needed in multiple
// level of processing. I inclosed all those parameters in this struct to
// avoid passing each of them as a method parameter.
struct osd_op_params_t {
  osd_reqid_t req_id;
  utime_t mtime;
  eversion_t at_version;
  eversion_t pg_trim_to;
  eversion_t pg_committed_to;
  eversion_t last_complete;
  bool user_modify = false;
  /// COPY_FROM: the object takes the source's user_version (unless
  /// another op of the request is a user modification)
  std::optional<version_t> user_version_override;
  /// COPY_FROM: the source's recent reqids, for dup detection
  mempool::osd_pglog::vector<std::pair<osd_reqid_t, version_t>> extra_reqids;
  mempool::osd_pglog::map<uint32_t, int> extra_reqid_return_codes;
  ObjectCleanRegions clean_regions;
  interval_set<uint64_t> modified_ranges;
  //TODO: Move delta_stats to osd_op_params_t
  osd_op_params_t() = default;
};
