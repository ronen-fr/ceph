// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <optional>

#include "crimson/osd/scrubber.h"
#include "crimson/osd/scrubber_common_cr.h"

namespace crimson::osd {

class PG;

class PgScrubSched {
 public:
  explicit PgScrubSched(PG& pg);

  bool sched_scrub();

  int get_scrub_priority();

 private:
  std::optional<requested_scrub_t> verify_scrub_mode() const;

  bool verify_periodic_scrub_mode(bool allow_deep_scrub,
				  bool try_to_auto_repair,
				  bool allow_regular_scrub,
				  bool has_deep_errors,
				  requested_scrub_t& planned) const;

  double next_deepscrub_interval() const;

  bool is_time_for_deep(bool allow_deep_scrub,
			bool allow_scrub,
			bool has_deep_errors,
			const requested_scrub_t& planned) const;

  inline bool test_pool_flag(int fl) const;

  inline bool test_osdmap_flag(
    int fl) const;  // \todo change the 'int' in the  underlying call

  inline CephContext* get_pg_cct() const;

  inline pg_shard_t get_shard_num() const;

  PG& m_pg;
  const spg_t m_pg_id;	///< a local copy of m_pg->pg_id
  ScrubPgIF* m_scrubber;
  ShardServices& m_osds;

  bool scrub_queued{false};
};


}  // namespace crimson::osd
