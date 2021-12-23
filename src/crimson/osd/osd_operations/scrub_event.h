// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include <iostream>

#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operation.h"
//#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"
#include "osd/osd_types.h"
//#include "crimson/osd/osd_operation.h"
//#include "crimson/osd/osd_operations/internal_client_request.h"
#include "crimson/osd/scrubber_common_cr.h"
//#include "include/mempool.h"
//#include "osd/PeeringState.h"
//#include "osd/osd_types.h"
//#include "common/hobject_fmt.h"
//#include "crimson/common/type_helpers.h"
//#include "crimson/osd/osd_operation.h"
//#include "crimson/osd/osd_operations/client_request_common.h"
//#include "crimson/osd/pg.h"
#include "osd/osd_types_fmt.h"


namespace crimson::osd {
class ScrubEvent;
class LocalScrubEvent;
}  // namespace crimson::osd
namespace fmt {
template <>
struct formatter<crimson::osd::LocalScrubEvent>;
template <>
struct formatter<crimson::osd::ScrubEvent>;
}  // namespace fmt

namespace crimson::osd {

using namespace ::std::chrono;
using namespace ::std::chrono_literals;


class OSD;
class ShardServices;
class PG;

using ScrubEventFwd = void (ScrubPgIF::*)(epoch_t);

#if 0

class ScrubInternalOp final : public InternalClientRequest {
 public:
  ScrubInternalOp(Ref<PG> pg,
		  ShardServices& shard_services,
		  const pg_shard_t& from,
		  const spg_t& pgid,
		  ScrubEventFwd func,
		  epoch_t epoch,
		  /* add the token */ int32_t temptkn)
      : InternalClientRequest(std::move(pg))
  {
    // RRR
  }

  const hobject_t& get_target_oid() const final { return fake_oid; }
  PG::do_osd_ops_params_t get_do_osd_ops_params() const final;
  std::vector<OSDOp> create_osd_ops() final;

  inline static const hobject_t fake_oid{};
};

#endif

// let's squash LocalScrubEvent into ScrubEvent

// to create two derived classes, one for local events (carrying a function
// pointer) and one for remote (osd to osd) ones.
class ScrubEvent : public OperationT<ScrubEvent> {
 public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_event;

  class PGPipeline {
    OrderedExclusivePhase await_map = {"ScrubEvent::PGPipeline::await_map"};
    //  do we need a pipe phase to lock the PG against other
    // types of client operations?
    OrderedExclusivePhase process = {"ScrubEvent::PGPipeline::process"};
    friend class ScrubEvent;
    friend class LocalScrubEvent;
    // friend class PGAdvanceMap;
  };

 private:
  Ref<PG> pg;
  ScrubEventFwd event_fwd_func;
  Scrub::act_token_t act_token;

  PipelineHandle handle;
  PGPipeline& pp(PG& pg);  // should this one be static?

  ShardServices& shard_services;
  // PeeringCtx ctx;
  // pg_shard_t from;
  spg_t pgid;
  epoch_t epoch_queued;
  std::chrono::milliseconds delay{0s};
  // not sure we need an event class. PGPeeringEvent evt;

  //   const pg_shard_t get_from() const {
  //     return from;
  //   }

  const spg_t get_pgid() const { return pgid; }

  //   const PGPeeringEvent &get_event() const {
  //     return evt;
  //   }

  virtual void on_pg_absent();
  virtual ScrubEvent::interruptible_future<> complete_rctx(Ref<PG>);
  virtual seastar::future<Ref<PG>> get_pg() final;

 public:
  std::string dbg_desc;
  ~ScrubEvent() override;


 public:
  ScrubEvent(Ref<PG> pg,
             ShardServices& shard_services,
             const spg_t& pgid,
             ScrubEventFwd func,
             epoch_t epoch_queued,
             Scrub::act_token_t tkn,
             std::chrono::milliseconds delay);

  ScrubEvent(Ref<PG> pg,
             ShardServices& shard_services,
             const spg_t& pgid,
             ScrubEventFwd func,
             epoch_t epoch_queued,
             Scrub::act_token_t tkn);

  void print(std::ostream&) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();
  seastar::future<> start0();

  friend fmt::formatter<ScrubEvent>;
};

#if 0
// -----------------------------------------------  ScrubEventLocal

class LocalScrubEvent final : public ScrubEvent {
 private:
  // seastar::future<Ref<PG>> get_pg() final;

  Ref<PG> pg;
  ScrubEventFwd event_fwd_func;
  Scrub::act_token_t act_token;

 public:
  LocalScrubEvent(Ref<PG> pg,
                  ShardServices& shard_services,
                  // const pg_shard_t& from,
                  const spg_t& pgid,
                  ScrubEventFwd func,
                  epoch_t epoch_queued,
                  Scrub::act_token_t tkn)
      : ScrubEvent{shard_services, pgid, epoch_queued}
      , pg{std::move(pg)}
      , event_fwd_func{func}
      , act_token{tkn}
  {}

  // overriden
  void print(std::ostream&) const final;
  void dump_detail(ceph::Formatter* f) const final;
  ScrubEvent::interruptible_future<> complete_rctx(Ref<PG>) final;

  seastar::future<> start();

  void on_pg_absent() override;
  seastar::future<Ref<PG>> get_pg() final;

  friend fmt::formatter<LocalScrubEvent>;
};
#endif

}  // namespace crimson::osd

template <>
struct fmt::formatter<crimson::osd::ScrubEvent> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const crimson::osd::ScrubEvent& levt, FormatContext& ctx)
  {
    return format_to(ctx.out(), "ScrubEvent(pgid={}, epoch={}, delay={}, token={}, dbg_desc={})",
                     levt.get_pgid(), levt.epoch_queued, levt.delay, levt.act_token, levt.dbg_desc);
  }
};

#if 0

template <>
struct fmt::formatter<crimson::osd::LocalScrubEvent> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const crimson::osd::LocalScrubEvent& levt, FormatContext& ctx)
  {
    return format_to(ctx.out(), "LocalScrubEvent(pgid={}, epoch={})",
                     levt.get_pgid(), levt.epoch_queued);
  }
};
#endif

namespace crimson::osd {

#if 0
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
#endif
}  // namespace crimson::osd
