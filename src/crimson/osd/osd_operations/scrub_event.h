// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include <iostream>

#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operation.h"
//#include "crimson/osd/osd_operations/pg_scrub_event.h"
#include "crimson/osd/scrubber_common_cr.h"
#include "include/mempool.h"
#include "osd/PeeringState.h"
#include "osd/osd_types.h"

namespace crimson::osd {

using namespace ::std::chrono;
using namespace ::std::chrono_literals;


class OSD;
class ShardServices;
class PG;

using ScrubEventFwd = void (ScrubPgIF::*)(epoch_t);

// carrying a scrubber function to call (with epoch data as a parameter)
class ScrubEvent2 : public OperationT<ScrubEvent2> {
 public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_event;

  class PGPipeline {
    OrderedExclusivePhase await_map = {"ScrubEvent::PGPipeline::await_map"};
    OrderedExclusivePhase process = {"ScrubEvent::PGPipeline::process"};
    friend class ScrubEvent2;
  };

  seastar::future<Ref<PG>> get_pg();
  ~ScrubEvent2();

 protected:
  PipelineHandle handle;
  static PGPipeline& pp(PG& pg);

  ShardServices& shard_services;
  PeeringCtx ctx;
  pg_shard_t from;  // to be removed
  spg_t pgid;
  std::chrono::milliseconds delay{0s};

  ScrubEventFwd event_fwd_func;
  epoch_t epoch_queued;
  Ref<PG> pg;

 public:
  std::string_view dbg_desc;

  spg_t get_pgid() const { return pgid; }

  virtual void on_pg_absent();
  virtual seastar::future<> complete_rctx(Ref<PG>);

 public:
  ScrubEvent2(Ref<PG> pg,
              ShardServices& shard_services,
	     const pg_shard_t& from,
	     const spg_t& pgid,
	      ScrubEventFwd func,
	      epoch_t epoch);


  ScrubEvent2(Ref<PG> pg,
	      ShardServices& shard_services,
	      const pg_shard_t& from,
	      const spg_t& pgid,
	      std::chrono::milliseconds delay,
	      ScrubEventFwd func,
	      epoch_t epoch,
	      std::string_view dbg_desc);

  ScrubEvent2(Ref<PG> pg,
	      ShardServices& shard_services,
	      const pg_shard_t& from,
	      const spg_t& pgid,
	      std::chrono::milliseconds delay,
	      ScrubEventFwd func,
	      epoch_t epoch);

  void print(std::ostream&) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();
};

}  // namespace crimson::osd
