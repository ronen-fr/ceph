// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <seastar/core/future.hh>

#include "include/mempool.h"
#include <iostream>
#include "crimson/common/type_helpers.h"
#include "crimson/osd/osd_operation.h"
#include "crimson/osd/osd_operations/pg_scrub_event.h"

//#include "crimson/osd/osd_operations/peering_event.h"
#include "osd/osd_types.h"
//#include "osd/PGPeeringEvent.h"
#include "osd/PeeringState.h"
#include "crimson/osd/scrubber_common_cr.h"

namespace crimson::osd {

class OSD;
class ShardServices;
class PG;

class ScrubEvent : public OperationT<ScrubEvent> {
 public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_event;

  class PGPipeline {
    OrderedPipelinePhase await_map = {"ScrubEvent::PGPipeline::await_map"};
    OrderedPipelinePhase process = {"ScrubEvent::PGPipeline::process"};
    friend class ScrubEvent;
    //friend class PeeringEvent;
    //friend class PGAdvanceMap;
  };

 protected:
  OrderedPipelinePhase::Handle handle;
  PGPipeline& pp(PG& pg);

  ShardServices& shard_services;
  PeeringCtx ctx;
  pg_shard_t from;
  spg_t pgid;
  float delay = 0;
  PgScrubEvent evt;

 public:
  pg_shard_t get_from() const { return from; }

  spg_t get_pgid() const { return pgid; }

  const PgScrubEvent& get_event() const { return evt; }

  virtual void on_pg_absent();
  virtual seastar::future<> complete_rctx(Ref<PG>);
  virtual seastar::future<Ref<PG>> get_pg() = 0;

 public:
  template <typename... Args>
  ScrubEvent(ShardServices& shard_services,
	     const pg_shard_t& from,
	     const spg_t& pgid,
	     Args&&... args)
      : shard_services(shard_services)
      , ctx{ceph_release_t::octopus}
      , from(from)
      , pgid(pgid)
      , evt(std::forward<Args>(args)...)
  {}
  template <typename... Args>
  ScrubEvent(ShardServices& shard_services,
	     const pg_shard_t& from,
	     const spg_t& pgid,
	     float delay,
	     Args&&... args)
      : shard_services(shard_services)
      , ctx{ceph_release_t::octopus}
      , from(from)
      , pgid(pgid)
      , delay(delay)
      , evt(std::forward<Args>(args)...)
  {}

  void print(std::ostream&) const final;
  void dump_detail(ceph::Formatter* f) const final;
  seastar::future<> start();
};


class RemoteScrubEvent : public ScrubEvent {
 protected:
  OSD &osd;
  crimson::net::ConnectionRef conn;

  void on_pg_absent() final;
  seastar::future<> complete_rctx(Ref<PG> pg) override;
  seastar::future<Ref<PG>> get_pg() final;

 public:
  class ConnectionPipeline {
    OrderedPipelinePhase await_map = {
      "ScrubRequest::ConnectionPipeline::await_map"
    };
    OrderedPipelinePhase get_pg = {
      "ScrubRequest::ConnectionPipeline::get_pg"
    };
    friend class RemoteScrubEvent;
  };

  template <typename... Args>
  RemoteScrubEvent(OSD &osd, crimson::net::ConnectionRef conn, Args&&... args) :
    ScrubEvent(std::forward<Args>(args)...),
    osd(osd),
    conn(conn)
  {}

 private:
  ConnectionPipeline& cp();
};


class LocalScrubEvent final : public ScrubEvent {
 protected:
  seastar::future<Ref<PG>> get_pg() final;

  Ref<PG> pg;

 public:
  template <typename... Args>
  LocalScrubEvent(Ref<PG> pg, Args&&... args) :
    ScrubEvent(std::forward<Args>(args)...),
    pg(pg)
  {}

  virtual ~LocalScrubEvent();
};


// //////////////////////////////////////////////////////////////////////////////// //

using ScrubEventFwd = void (ScrubPgIF::*)(epoch_t);

// carrying a scrubber function to call with epoch data
class ScrubEvent2 : public OperationT<ScrubEvent2> {
 public:
  static constexpr OperationTypeCode type = OperationTypeCode::scrub_event;

  class PGPipeline {
    OrderedPipelinePhase await_map = {"ScrubEvent::PGPipeline::await_map"};
    OrderedPipelinePhase process = {"ScrubEvent::PGPipeline::process"};
    friend class ScrubEvent2;
  };

  seastar::future<Ref<PG>> get_pg();
  ~ScrubEvent2();

 protected:
  OrderedPipelinePhase::Handle handle;
  PGPipeline& pp(PG& pg);

  ShardServices& shard_services;
  PeeringCtx ctx;
  pg_shard_t from;  // to be removed
  spg_t pgid;
  std::chrono::milliseconds delay{0s};
  //PgScrubEvent evt;
  ScrubEventFwd event_fwd_func;
  epoch_t epoch_queued;
  Ref<PG> pg;

 public:
  //pg_shard_t get_from() const { return from; }

  spg_t get_pgid() const { return pgid; }

  //const PgScrubEvent& get_event() const { return evt; }

  virtual void on_pg_absent();
  virtual seastar::future<> complete_rctx(Ref<PG>);
  //virtual seastar::future<Ref<PG>> get_pg() = 0;

 public:
  //template <typename... Args>
  ScrubEvent2(Ref<PG> pg,
              ShardServices& shard_services,
	     const pg_shard_t& from,
	     const spg_t& pgid,
	      ScrubEventFwd func,
	      epoch_t epoch);
	     //Args&&... args)


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
