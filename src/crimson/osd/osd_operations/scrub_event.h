// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include <iostream>
#include <variant>

#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/scrubber_common_cr.h"
#include "osd/PeeringState.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"


namespace crimson::osd {
class ScrubEvent;
}  // namespace crimson::osd

namespace fmt {
template <>
struct formatter<crimson::osd::ScrubEvent>;
}  // namespace fmt

namespace crimson::osd {

using namespace ::std::chrono;
using namespace ::std::chrono_literals;

class OSD;
class ShardServices;
class PG;


// to create two derived classes, one for local events (carrying a function
// pointer) and one for remote (osd to osd) ones.
class ScrubEvent : public OperationT<ScrubEvent> {
 public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_event;

  template <typename T = void>
  using interruptible_future = ::crimson::interruptible::
    interruptible_future<::crimson::osd::IOInterruptCondition, T>;

  using ScrubEventFwdFut = interruptible_future<> (ScrubPgIF::*)(epoch_t);
  using ScrubEventFwdImm = void (ScrubPgIF::*)(epoch_t);
  using ScrubEventFwd = std::variant<ScrubEventFwdFut, ScrubEventFwdImm>;


  class PGPipeline {
    OrderedExclusivePhase await_map = {"ScrubEvent::PGPipeline::await_map"};
    //  do we need a pipe phase to lock the PG against other
    //  types of client operations?

    // a local synchronier, to enable us to finish an operation after creating
    // a new event.
    OrderedExclusivePhase local_sync = {"ScrubEvent::PGPipeline::local_sync"};

    OrderedExclusivePhase process = {"ScrubEvent::PGPipeline::process"};
    friend class ScrubEvent;
  };

 private:
  Ref<PG> pg;
  ScrubEventFwd event_fwd_func;
  Scrub::act_token_t act_token;

  PipelineHandle handle;
  static PGPipeline& pp(PG& pg);  // should this one be static?

 public:
  // using rett = decltype(std::declval<ScrubEventFwd>().index());
  using rett = decltype(handle.enter(pp(*pg).local_sync));
  struct nullevent_tag_t {
  };


 private:
  ShardServices& shard_services;
  spg_t pgid;
  epoch_t epoch_queued;
  std::chrono::milliseconds delay{0s};

  const spg_t get_pgid() const { return pgid; }

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

  // until I learn how to enter the pipeline w/o creating a new event
  ScrubEvent(nullevent_tag_t,
             Ref<PG> pg,
             ShardServices& shard_services,
             const spg_t& pgid,
             ScrubEventFwd func);

  void print(std::ostream&) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();

  // static seastar::future<PipelineHandle> lockout(PG& pg);
  // static void unlock(PG& pg, PipelineHandle&& handle);
  rett lockout();
  void unlock();

  friend fmt::formatter<ScrubEvent>;
};


}  // namespace crimson::osd

template <>
struct fmt::formatter<crimson::osd::ScrubEvent> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }
  template <typename FormatContext>
  auto format(const crimson::osd::ScrubEvent& levt, FormatContext& ctx)
  {
    return format_to(
      ctx.out(),
      "ScrubEvent(pgid={}, epoch={}, delay={}, token={}, dbg_desc={})",
      levt.get_pgid(), levt.epoch_queued, levt.delay, levt.act_token,
      levt.dbg_desc);
  }
};
