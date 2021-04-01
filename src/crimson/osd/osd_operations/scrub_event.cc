// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/osd_operations/scrub_event.h"

#include <seastar/core/future.hh>

#include <boost/smart_ptr/local_shared_ptr.hpp>

#include "common/Formatter.h"
#include "crimson/osd/osd.h"
//#include "crimson/osd/osd_connection_priv.h"
#include "crimson/osd/pg.h"
#include "messages/MOSDPGLog.h"

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace


namespace crimson::osd {

void ScrubEvent2::print(std::ostream& lhs) const
{
  lhs << "ScrubEvent2("
      << "from=" << from << " pgid=" << pgid << " sent=" << epoch_queued
      << " requested=" << epoch_queued << " evt="
      << "<<no desc yet>>"
      << ")";
}

void ScrubEvent2::dump_detail(Formatter* f) const
{
  f->open_object_section("ScrubEvent2");
  f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  // f->dump_int("sent", evt.get_epoch_sent());
  // f->dump_int("requested", evt.get_epoch_requested());
  // f->dump_string("evt", evt.get_desc());
  f->close_section();
}


ScrubEvent2::PGPipeline& ScrubEvent2::pp(PG& pg)
{
  return pg.scrub_event2_pg_pipeline;
}


ScrubEvent2::ScrubEvent2(Ref<PG> pg,
			 ShardServices& shard_services,
			 const pg_shard_t& from,
			 const spg_t& pgid,
			 ScrubEventFwd func,
			 epoch_t epoch)
    // Args&&... args)
    : shard_services{shard_services}
    , ctx{ceph_release_t::octopus}
    , from{from}
    , pgid{pgid}
    , event_fwd_func{func}
    , epoch_queued{epoch}
    , pg{pg}
    , dbg_desc{"<no-dbg-desc>"}
{
  logger().debug("ScrubEvent2: 1st ctor {:p}", (void*)this);
}

ScrubEvent2::ScrubEvent2(Ref<PG> pg,
			 ShardServices& shard_services,
			 const pg_shard_t& from,
			 const spg_t& pgid,
			 std::chrono::milliseconds delay,
			 ScrubEventFwd func,
			 epoch_t epoch)
    // Args&&... args)
    : shard_services{shard_services}
    , ctx{ceph_release_t::octopus}
    , from{from}
    , pgid{pgid}
    , delay{delay}
    , event_fwd_func{func}
    , epoch_queued{epoch}
    , pg{pg}
    , dbg_desc{"<no dbg desc>"}
{
  logger().debug("ScrubEvent2: 2nd ctor {:p} {}", (void*)this, dbg_desc);
}

ScrubEvent2::ScrubEvent2(Ref<PG> pg,
			 ShardServices& shard_services,
			 const pg_shard_t& from,
			 const spg_t& pgid,
			 std::chrono::milliseconds delay,
			 ScrubEventFwd func,
			 epoch_t epoch,
			 std::string_view dbg_desc)
    // Args&&... args)
    : shard_services{shard_services}
    , ctx{ceph_release_t::octopus}
    , from{from}
    , pgid{pgid}
    , delay{delay}
    , event_fwd_func{func}
    , epoch_queued{epoch}
    , pg{pg}
    , dbg_desc{dbg_desc}
{
  logger().debug("ScrubEvent2: 3rd ctor {:p} {}", (void*)this, dbg_desc);
}

seastar::future<> ScrubEvent2::start()
{
  logger().debug("scrubber: ScrubEvent2::start(): {}: start (delay: {}) pg:{:p}", *this,
		 delay, (void*)&(*pg));

  IRef ref = this;
  return
    [this] {
      if (delay > 0ms) {
	return seastar::sleep(delay);
      } else {
	return seastar::now();
      }
    }()
      .then([this] {
	logger().debug("{}: after delay ", *this);
	return with_blocking_future(handle.enter(pp(*pg).await_map))
	  .then([this] {
	    return with_blocking_future(pg->osdmap_gate.wait_for_map(
	      epoch_queued));  // should we wait? we are internal. RRR
	  })
	  //	.then([this](auto) {
	  //	  return with_blocking_future(handle.enter(pp(*pg).process));
	  //	  /*}).then([this, pg] {
	  //	    // TODO: likely we should synchronize also with the pg log-based
	  //	    // recovery.
	  //	    return with_blocking_future(
	  //	      handle.enter(BackfillRecovery::bp(*pg).process));*/
	  //	})
	  .then([this](auto) {
	    logger().debug("{}: executing ", *this);
	    try {
	      ((*pg->m_scrubber).*event_fwd_func)(epoch_queued);
	    } catch (...) {
	    }
	    logger().debug("{}: executed ", *this);
	    handle.exit();
	    return complete_rctx(pg);
	  })
	  .then([/* not yet this*/] {
	    return seastar::now();
	    //	  return pg->get_need_up_thru()
	    //		 ? shard_services.send_alive(pg->get_same_interval_since())
	    //		 : seastar::now();
	  })
	  .then(
	    [this /*, ref = std::move(ref)*/] { logger().debug("{}: complete", *this); });
      });
}


void ScrubEvent2::on_pg_absent()
{
  logger().warn("{}: pg absent, dropping", *this);
}

seastar::future<> ScrubEvent2::complete_rctx(Ref<PG> pg)
{
  logger().debug("{}: submitting ctx", *this);
  if (pg) {
    return shard_services.dispatch_context(pg->get_collection_ref(), std::move(ctx));
  } else {
    return shard_services.dispatch_context_messages(std::move(ctx));
  }
}


ScrubEvent2::~ScrubEvent2() = default;


seastar::future<Ref<PG>> ScrubEvent2::get_pg()
{
  return seastar::make_ready_future<Ref<PG>>(pg);
}

}  // namespace crimson::osd
