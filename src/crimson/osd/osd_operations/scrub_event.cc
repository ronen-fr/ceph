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

// --------------------------- ScrubEvent ---------------------------


ScrubEvent::ScrubEvent(Ref<PG> pg,
                       ShardServices& shard_services,
                       const spg_t& pgid,
                       ScrubEventFwd func,
                       epoch_t epoch_queued,
                       Scrub::act_token_t tkn,
                       std::chrono::milliseconds delay)
    : pg{std::move(pg)}
    , event_fwd_func{func}
    , act_token{tkn}
    , shard_services{shard_services}
    , pgid{pgid}
    , epoch_queued{epoch_queued}
    , delay{delay}
    , dbg_desc{"<ScrubEvent>"}
{
  logger().debug("ScrubEvent: 1'st ctor {:p} {} delay:{}", (void*)this, dbg_desc,
                 delay);
}

ScrubEvent::ScrubEvent(Ref<PG> pg,
                       ShardServices& shard_services,
                       const spg_t& pgid,
                       ScrubEventFwd func,
                       epoch_t epoch_queued,
                       Scrub::act_token_t tkn)
: ScrubEvent{std::move(pg), shard_services, pgid, func, epoch_queued, tkn,
             std::chrono::milliseconds{0}}
{
  logger().debug("ScrubEvent: 2'nd ctor {:p} {}", (void*)this, dbg_desc);
}

void ScrubEvent::print(std::ostream& lhs) const
{
  lhs << fmt::format("{}", *this);
}

void ScrubEvent::dump_detail(Formatter* f) const
{
  f->open_object_section("ScrubEvent");
  //f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  // f->dump_int("sent", evt.get_epoch_sent());
  // f->dump_int("requested", evt.get_epoch_requested());
  // f->dump_string("evt", evt.get_desc());
  f->close_section();
}

void ScrubEvent::on_pg_absent()
{
  logger().warn("{}: pg absent, dropping", *this);
}

seastar::future<Ref<PG>> ScrubEvent::get_pg() {
  return seastar::make_ready_future<Ref<PG>>(pg);
}

ScrubEvent::interruptible_future<> ScrubEvent::complete_rctx(Ref<PG> pg)
{
  logger().debug("{}: no ctx for now to submit", *this);
//   if (pg) {
//     return shard_services.dispatch_context(pg->get_collection_ref(), std::move(ctx));
//   } else {
//     return shard_services.dispatch_context_messages(std::move(ctx));
//   }
  return seastar::make_ready_future<>();
}

ScrubEvent::PGPipeline &ScrubEvent::pp(PG &pg)
{
  return pg.scrub_event_pg_pipeline;
}


ScrubEvent::~ScrubEvent() = default;


// clang-format off
seastar::future<> ScrubEvent::start()
{
  logger().debug(
    "scrubber: ScrubEvent::start(): {}: start (delay: {}) pg:{:p}", *this,
    delay, (void*)&(*pg));

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (delay.count() > 0) {
    maybe_delay = seastar::sleep(delay);
  }

  return maybe_delay.then([this] {
    return get_pg();
  }).then([this](Ref<PG> pg) {
    return interruptor::with_interruption([this, pg]() -> ScrubEvent::interruptible_future<> {
      if (!pg) {
        logger().warn("scrubber: ScrubEvent::start(): {}: pg absent, did not create", *this);
        on_pg_absent();
        handle.exit();
        return complete_rctx(pg);
      }
      logger().debug("scrubber: ScrubEvent::start(): {}: pg present", *this);
      return with_blocking_future_interruptible<interruptor::condition>(
        handle.enter(pp(*pg).await_map)
      ).then_interruptible([this, pg] {
        return with_blocking_future_interruptible<interruptor::condition>(
          pg->osdmap_gate.wait_for_map(epoch_queued));
      }).then_interruptible([this, pg](auto) {
        return with_blocking_future_interruptible<interruptor::condition>(
          handle.enter(pp(*pg).process));
      }).then_interruptible([this, pg]() mutable -> ScrubEvent::interruptible_future<>  {

        logger().info("ScrubEvent::start() {} executing...", *this);
        if (std::holds_alternative<ScrubEvent::ScrubEventFwdImm>(event_fwd_func)) {
          (*(pg->get_scrubber(Scrub::ScrubberPasskey{})).*std::get<ScrubEvent::ScrubEventFwdImm>(event_fwd_func))(epoch_queued);
          return seastar::make_ready_future<>();
        } else {
          return (*(pg->get_scrubber(Scrub::ScrubberPasskey{})).*std::get<ScrubEvent::ScrubEventFwdFut>(event_fwd_func))(epoch_queued);
        }
        //return (*(pg->get_scrubber(Scrub::ScrubberPasskey{})).*event_fwd_func)(epoch_queued);

      }).then_interruptible([this, pg]() mutable {
        logger().info("ScrubEvent::start() {} after calling fwder", *this);
        handle.exit();
        logger().info("ScrubEvent::start() {} executing... exited", *this);
        return complete_rctx(pg);
      }).then_interruptible([pg]() -> ScrubEvent::interruptible_future<> {
        return seastar::now();
      });
    },
    [this](std::exception_ptr ep) {
      logger().debug("ScrubEvent::start(): {} interrupted with {}", *this, ep);
      return seastar::now();
    },
    pg);
  }).finally([this, ref=std::move(ref)] {
    logger().debug("ScrubEvent::start(): {} complete", *this /*, *ref*/);
  });
}
// clang-format on

#if 0
seastar::future<> ScrubEvent::start0()
{
  logger().debug(
    "scrubber: ScrubEvent::start(): {}: start (delay: {}) pg:{:p}", *this,
    delay, (void*)&(*pg));

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (delay.count() > 0) {
    maybe_delay = seastar::sleep(delay);
  }

  return maybe_delay.then([this] {
    return get_pg();
  }).then([this](Ref<PG> pg) {
    return interruptor::with_interruption([this, pg]() -> ScrubEvent::interruptible_future<> {
      if (!pg) {
        logger().warn("scrubber: ScrubEvent::start(): {}: pg absent, did not create", *this);
        on_pg_absent();
        handle.exit();
        return complete_rctx(pg);
      }
      logger().debug("scrubber: ScrubEvent::start(): {}: pg present", *this);
      return with_blocking_future_interruptible<interruptor::condition>(
        handle.enter(pp(*pg).await_map)
      ).then_interruptible([this, pg] {
        return with_blocking_future_interruptible<interruptor::condition>(
          pg->osdmap_gate.wait_for_map(epoch_queued));
      //#ifdef NOT_YET
      }).then_interruptible([this, pg](auto) {
        return with_blocking_future_interruptible<interruptor::condition>(
          handle.enter(pp(*pg).process));
      //}).then_interruptible([this, pg] {
      //  return with_blocking_future_interruptible<interruptor::condition>(
      //    handle.enter(BackfillRecovery::bp(*pg).process));
      //}).then_interruptible([this, pg] {
      //  return with_blocking_future_interruptible<interruptor::condition>(
      //    handle.enter(Scrub::scrub(*pg).process));
      //#endif
      }).then_interruptible([this, pg]() mutable -> ScrubEvent::interruptible_future<>  {

        logger().info("ScrubEvent::start() {} executing...", *this);
        return (*(pg->get_scrubber(Scrub::ScrubberPasskey{})).*event_fwd_func)(epoch_queued);

      }).then_interruptible([this, pg]() mutable {
        logger().info("ScrubEvent::start() {} after calling fwder", *this);
        handle.exit();
        logger().info("ScrubEvent::start() {} executing... exited", *this);
        return complete_rctx(pg);
      }).then_interruptible([pg]() -> ScrubEvent::interruptible_future<> {
        return seastar::now();
      });
    },
    [this](std::exception_ptr ep) {
      logger().debug("ScrubEvent::start(): {} interrupted with {}", *this, ep);
      return seastar::now();
    },
    pg);
  }).finally([this, ref=std::move(ref)] {
    logger().debug("ScrubEvent::start(): {} complete", *this /*, *ref*/);
  });
}

#endif
#if 0
// --------------------------- LocalScrubEvent -----------------------

void LocalScrubEvent::print(std::ostream& lhs) const
{
  lhs << fmt::format("{}", *this);
//   lhs << "LocalScrubEvent("
//       << "from=" << from << " pgid=" << pgid << " sent=" << epoch_queued
//       << " requested=" << epoch_queued << " evt="
//       << "<<no desc yet>>"
//       << ")";
}

void LocalScrubEvent::dump_detail(Formatter* f) const
{
  f->open_object_section("LocalScrubEvent");
  //f->dump_stream("from") << from;
  f->dump_stream("pgid") << pgid;
  // f->dump_int("sent", evt.get_epoch_sent());
  // f->dump_int("requested", evt.get_epoch_requested());
  // f->dump_string("evt", evt.get_desc());
  f->close_section();
}



// LocalScrubEvent::PGPipeline& LocalScrubEvent::pp(PG& pg)
// {
//   return pg.scrub_event2_pg_pipeline;
// }

void LocalScrubEvent::on_pg_absent()
{
  logger().warn("{}: pg absent, dropping", *this);
}

// clang-format off
seastar::future<> LocalScrubEvent::start()
{
  logger().debug(
    "scrubber: ScrubEvent2::start(): {}: start (delay: {}) pg:{:p}", *this,
    delay, (void*)&(*pg));

  IRef ref = this;
  auto maybe_delay = seastar::now();
  if (delay.count() > 0) {
    maybe_delay = seastar::sleep(delay);
  }

  return maybe_delay.then([this] {
    return get_pg();
  }).then([this](Ref<PG> pg) {
    return interruptor::with_interruption([this, pg] {
      if (!pg) {
        logger().warn("{}: pg absent, did not create", *this);
        on_pg_absent();
        handle.exit();
        return complete_rctx(pg);
      }
      logger().debug("{}: pg present", *this);
      return with_blocking_future_interruptible<interruptor::condition>(
        handle.enter(pp(*pg).await_map)
      ).then_interruptible([this, pg] {
        return with_blocking_future_interruptible<interruptor::condition>(
          pg->osdmap_gate.wait_for_map(epoch_queued));
      #ifdef NOT_YET
      }).then_interruptible([this, pg](auto) {
        return with_blocking_future_interruptible<interruptor::condition>(
          handle.enter(pp(*pg).process));
      }).then_interruptible([this, pg] {
        return with_blocking_future_interruptible<interruptor::condition>(
          handle.enter(BackfillRecovery::bp(*pg).process));
      }).then_interruptible([this, pg] {
        return with_blocking_future_interruptible<interruptor::condition>(
          handle.enter(Scrub::scrub(*pg).process));
      #endif
      }).then_interruptible([this, pg]() mutable -> ScrubEvent::interruptible_future<> {
        logger().info("LocalScrubEvent::start() executing...");
        (*(pg->get_scrubber(Scrub::ScrubberPasskey{})).*event_fwd_func)(epoch_queued);
        handle.exit();
        return complete_rctx(pg);
      }).then_interruptible([pg]() -> ScrubEvent::interruptible_future<> {
        return seastar::now();
      });
    },
    [this](std::exception_ptr ep) {
      logger().debug("{}: interrupted with {}", *this, ep);
      return seastar::now();
    },
    pg);
  }).finally([ref=std::move(ref)] {
    logger().debug("{}: complete", *ref);
  });
}
// clang-format on


// 
// 
//         //	.then([this](auto) {
//         //	  return with_blocking_future(handle.enter(pp(*pg).process));
//         //	  /*}).then([this, pg] {
//         //	    // TODO: likely we should synchronize also with the pg
//         //log-based
//         //	    // recovery.
//         //	    return with_blocking_future(
//         //	      handle.enter(BackfillRecovery::bp(*pg).process));*/
//         //	})
//         .then([this](auto) {
//           logger().debug("{}: executing ", *this);
//           try {
//             ((*pg->m_scrubber).*event_fwd_func)(epoch_queued);
//           } catch (...) {
//           }
//           logger().debug("{}: executed ", *this);
//           handle.exit();
//           return complete_rctx(pg);
//         })
//         .then([/* not yet this*/] {
//           return seastar::now();
//           //	  return pg->get_need_up_thru()
//           //		 ?
//           //shard_services.send_alive(pg->get_same_interval_since()) 		 :
//           //seastar::now();
//         })
//         .then([this /*, ref = std::move(ref)*/] {
//           logger().debug("{}: complete", *this);
//         });
//     });
// }

#endif

}


#if 0

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

#endif

/*

handle_recovery_subreq

seastar::future<> OSD::handle_recovery_subreq(crimson::net::ConnectionRef conn,
				   Ref<MOSDFastDispatchOp> m)
{
  (void) shard_services.start_operation<RecoverySubRequest>(
    *this,
    conn,
    std::move(m));
  return seastar::now();
}

std::optional<seastar::future<>>
OSD::ms_dispatch(crimson::net::ConnectionRef conn, MessageRef m)
{
...
    case MSG_OSD_PG_SCAN:
      [[fallthrough]];
    case MSG_OSD_PG_BACKFILL:
      [[fallthrough]];
    case MSG_OSD_PG_BACKFILL_REMOVE:
      return handle_recovery_subreq(conn, boost::static_pointer_cast<MOSDFastDispatchOp>(m));
 ...
}

  seastar::future<> handle_recovery_subreq(crimson::net::ConnectionRef conn,
					   Ref<MOSDFastDispatchOp> m);
  seastar::future<> handle_scrub(crimson::net::ConnectionRef conn,
				 Ref<MOSDScrub2> m);





MSG_OSD_PG_SCAN:


recovery bckend:
RecoveryBackend::interruptible_future<>
RecoveryBackend::handle_recovery_op(
  Ref<MOSDFastDispatchOp> m)
{
  switch (m->get_header().type) {
  case MSG_OSD_PG_BACKFILL:
    return handle_backfill(*boost::static_pointer_cast<MOSDPGBackfill>(m));
  case MSG_OSD_PG_BACKFILL_REMOVE:
    return handle_backfill_remove(*boost::static_pointer_cast<MOSDPGBackfillRemove>(m));
  case MSG_OSD_PG_SCAN:
    return handle_scan(*boost::static_pointer_cast<MOSDPGScan>(m));
  default:
    return seastar::make_exception_future<>(
	std::invalid_argument(fmt::format("invalid request type: {}",
					  m->get_header().type)));
  }
}

  MOSDPGScan()
    : MOSDFastDispatchOp{MSG_OSD_PG_SCAN, HEAD_VERSION, COMPAT_VERSION} {}
  MOSDPGScan(__u32 o, pg_shard_t from,
	     epoch_t e, epoch_t qe, spg_t p, hobject_t be, hobject_t en)
    : MOSDFastDispatchOp{MSG_OSD_PG_SCAN, HEAD_VERSION, COMPAT_VERSION},
      op(o),
      map_epoch(e), query_epoch(qe),
      from(from),
      pgid(p),
      begin(be), end(en) {
  }


MOSDPGScan

void PGRecovery::request_replica_scan(
  const pg_shard_t& target,
  const hobject_t& begin,
  const hobject_t& end)
{
  logger().debug("{}: target.osd={}", __func__, target.osd);
  auto msg = crimson::make_message<MOSDPGScan>(
    MOSDPGScan::OP_SCAN_GET_DIGEST,
    pg->get_pg_whoami(),
    pg->get_osdmap_epoch(),
    pg->get_last_peering_reset(),
    spg_t(pg->get_pgid().pgid, target.shard),
    begin,
    end);
  std::ignore = pg->get_shard_services().send_to_osd(
    target.osd,
    std::move(msg),
    pg->get_osdmap_epoch());
}


---- are there messages that do not need an M part?








*/
