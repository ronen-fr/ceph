// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "crimson/osd/osd_operations/scrub_event.h"
#include <boost/smart_ptr/local_shared_ptr.hpp>

#include <seastar/core/future.hh>

#include "common/Formatter.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/osd_connection_priv.h"
//#include "crimson/osd/osd_operations/peering_event.h"  // to be removed
#include "crimson/osd/pg.h"
#include "messages/MOSDPGLog.h"

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

namespace crimson::osd {

void ScrubEvent::print(std::ostream& lhs) const
{
  lhs << "ScrubEvent("
      << "from=" << from << " pgid=" << pgid << " sent=" << evt.get_epoch_sent()
      << " requested=" << evt.get_epoch_requested() << " evt=" << evt.get_desc() << ")";
}

void ScrubEvent::dump_detail(Formatter* f) const
{
  f->open_object_section("ScrubEvent");
  f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  f->dump_int("sent", evt.get_epoch_sent());
  f->dump_int("requested", evt.get_epoch_requested());
  f->dump_string("evt", evt.get_desc());
  f->close_section();
}


ScrubEvent::PGPipeline& ScrubEvent::pp(PG& pg)
{
  return pg.scrub_event_pg_pipeline;
}

seastar::future<> ScrubEvent::start()
{

  logger().debug("{}: start", *this);

  IRef ref = this;
  return [this] {
    if (delay > 0.001f) {
      return seastar::sleep(std::chrono::milliseconds(std::lround(delay * 1000)));
    } else {
      return seastar::now();
    }
  }().then([this] { return get_pg(); } // RRR handle formatting
	 ).then([this](Ref<PG> pg) {
	     if (!pg) {
	       logger().warn("{}: pg absent, did not create", *this);
	       on_pg_absent();
	       handle.exit();
	       return complete_rctx(pg);
	     } else {
	       logger().debug("{}: pg present", *this);
	       return with_blocking_future(handle.enter(pp(*pg).await_map))
		 .then([this, pg] {
		   return with_blocking_future(
		     pg->osdmap_gate.wait_for_map(evt.get_epoch_sent()));
		 })
		 .then([this, pg](auto) {
		   return with_blocking_future(handle.enter(pp(*pg).process));
		   /*}).then([this, pg] {
		     // TODO: likely we should synchronize also with the pg log-based
		     // recovery.
		     return with_blocking_future(
		       handle.enter(BackfillRecovery::bp(*pg).process));*/
		 })
		 .then([this, pg] {
		   pg->do_scrub_event(evt, ctx);
		   handle.exit();
		   return complete_rctx(pg);
		 })
		 .then([this, pg] {
		   return pg->get_need_up_thru()
			    ? shard_services.send_alive(pg->get_same_interval_since())
			    : seastar::now();
		 });
	     }
	   })
	   .then([this] { return shard_services.send_pg_temp(); })
	   .then([this, ref = std::move(ref)] { logger().debug("{}: complete", *this); });
}

void ScrubEvent::on_pg_absent()
{
  logger().warn("{}: pg absent, dropping", *this);
}

seastar::future<> ScrubEvent::complete_rctx(Ref<PG> pg)
{
  logger().debug("{}: submitting ctx", *this);
  return shard_services.dispatch_context(pg->get_collection_ref(), std::move(ctx));
}

crimson::osd::RemoteScrubEvent::ConnectionPipeline& RemoteScrubEvent::cp()
{
  return get_osd_priv(conn.get()).scrub_event_conn_pipeline;
}

//RemoteScrubEvent::ConnectionPipeline& RemoteScrubEvent::cp()
//{
//  return get_osd_priv(conn.get()).peering_request_conn_pipeline;
//}


void RemoteScrubEvent::on_pg_absent()
{
#ifdef NOT_YET
  if (auto& e = get_event().get_event(); e.dynamic_type() == MQuery::static_type()) {
    const auto map_epoch = shard_services.get_osdmap_service().get_map()->get_epoch();
    const auto& q = static_cast<const MQuery&>(e);
    const pg_info_t empty{spg_t{pgid.pgid, q.query.to}};
    if (q.query.type == q.query.LOG || q.query.type == q.query.FULLLOG) {
      auto m = ceph::make_message<MOSDPGLog>(q.query.from, q.query.to, map_epoch, empty,
					     q.query.epoch_sent);
      ctx.send_osd_message(q.from.osd, std::move(m));
    } else {
      ctx.send_notify(q.from.osd, {q.query.from, q.query.to, q.query.epoch_sent,
				   map_epoch, empty, PastIntervals{}});
    }
  }
#endif
}

seastar::future<> RemoteScrubEvent::complete_rctx(Ref<PG> pg)
{
  if (pg) {
    return ScrubEvent::complete_rctx(pg);
  } else {
    return shard_services.dispatch_context_messages(std::move(ctx));
  }
}

seastar::future<Ref<PG>> RemoteScrubEvent::get_pg()
{
#ifdef NOT_YET
  return with_blocking_future(handle.enter(cp().await_map))
    .then([this] {
      return with_blocking_future(osd.osdmap_gate.wait_for_map(evt.get_epoch_sent()));
    })
    .then([this](auto epoch) {
      logger().debug("{}: got map {}", *this, epoch);
      return with_blocking_future(handle.enter(cp().get_pg));
    })
    .then([this] {
      return with_blocking_future(
	osd.get_or_create_pg(pgid, evt.get_epoch_sent(), std::move(evt.create_info)));
    });
#endif
  Ref<PG> pg;
  return seastar::make_ready_future<Ref<PG>>(pg);
}



LocalScrubEvent::~LocalScrubEvent() {}


seastar::future<Ref<PG>> LocalScrubEvent::get_pg()
{
  return seastar::make_ready_future<Ref<PG>>(pg);
}

}  // namespace crimson::osd


// event2


namespace crimson::osd {

void ScrubEvent2::print(std::ostream& lhs) const
{
  lhs << "ScrubEvent2("
      << "from=" << from << " pgid=" << pgid << " sent=" << epoch_queued
      << " requested=" << epoch_queued << " evt=" << "<<no desc yet>>" << ")";
}

void ScrubEvent2::dump_detail(Formatter* f) const
{
  f->open_object_section("ScrubEvent2");
  f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  //f->dump_int("sent", evt.get_epoch_sent());
  //f->dump_int("requested", evt.get_epoch_requested());
  //f->dump_string("evt", evt.get_desc());
  f->close_section();
}


ScrubEvent2::PGPipeline& ScrubEvent2::pp(PG& pg)
{
  return pg.scrub_event2_pg_pipeline;
}

#ifdef OLD_VER_1
seastar::future<> ScrubEvent2::start()
{

  logger().debug("{}: start", *this);

  IRef ref = this;
  return [this] {
    if (delay > 0.001f) {
      return seastar::sleep(std::chrono::milliseconds(std::lround(delay * 1000)));
    } else {
      return seastar::now();
    }
  }().then([this] { return get_pg(); } // RRR handle formatting
  ).then([this](Ref<PG> pg) {
    if (!pg) {
      logger().warn("{}: pg absent, did not create", *this);
      on_pg_absent();
      handle.exit();
      return seastar::make_ready_future<>();
      // RRR return complete_rctx(pg); // RRR ask why OK to call on the null ref
    } else {
      logger().debug("{}: pg present", *this);
      return with_blocking_future(handle.enter(pp(*pg).await_map))
	.then([this, pg] {
	  return with_blocking_future(
	    pg->osdmap_gate.wait_for_map(epoch_queued)); // should we wait? we are internal. RRR
	})
	.then([this, pg](auto) {
	  return with_blocking_future(handle.enter(pp(*pg).process));
	  /*}).then([this, pg] {
	    // TODO: likely we should synchronize also with the pg log-based
	    // recovery.
	    return with_blocking_future(
	      handle.enter(BackfillRecovery::bp(*pg).process));*/
	})
	.then([this, pg] {
	  ((*pg->m_scrubber).*event_fwd_func)(epoch_queued);
	  //pg->do_scrub_event(evt, ctx);
	  handle.exit();
	  return complete_rctx(pg);
	})
	.then([this, pg] {
	  return pg->get_need_up_thru()
		 ? shard_services.send_alive(pg->get_same_interval_since())
		 : seastar::now();
	});
    }
  })
    .then([this] { return shard_services.send_pg_temp(); })
    .then([this, ref = std::move(ref)] { logger().debug("{}: complete", *this); });
}
#endif

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
//, event_fwd_func(std::forward<Args>(args)...)
{}

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
//, event_fwd_func(std::forward<Args>(args)...)
{}

seastar::future<> ScrubEvent2::start()
{
  logger().debug("scrubber: ScrubEvent2::start(): {}: start (delay: {})", *this, delay);

  IRef ref = this;
  return [this] {
    if (delay > 0ms) {
      //return seastar::sleep(std::chrono::milliseconds(std::lround(delay * 1000)));
      return seastar::sleep(delay);
    } else {
      return seastar::now();
    }
  }().then([this] {
      logger().debug("{}: after delay ", *this);
      return with_blocking_future(handle.enter(pp(*pg).await_map))
	.then([this] {
	  return with_blocking_future(
	    pg->osdmap_gate.wait_for_map(epoch_queued)); // should we wait? we are internal. RRR
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
	  ((*pg->m_scrubber).*event_fwd_func)(epoch_queued);
	  //pg->do_scrub_event(evt, ctx);
	  handle.exit();
	  return complete_rctx(pg);
	})
	.then([this] {
	  return seastar::now();
//	  return pg->get_need_up_thru()
//		 ? shard_services.send_alive(pg->get_same_interval_since())
//		 : seastar::now();
	})
    //.then([this] { return shard_services.send_pg_temp(); })
    .then([this /*, ref = std::move(ref)*/] { logger().debug("{}: complete", *this); });
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


ScrubEvent2::~ScrubEvent2() {}


seastar::future<Ref<PG>> ScrubEvent2::get_pg()
{
  return seastar::make_ready_future<Ref<PG>>(pg);
}

}  // namespace crimson::osd
