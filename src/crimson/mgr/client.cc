#include "client.h"

#include "crimson/common/log.h"
#include "crimson/net/Connection.h"
#include "crimson/net/Messenger.h"
#include "messages/MMgrConfigure.h"
#include "messages/MMgrMap.h"
#include "messages/MMgrOpen.h"

namespace {
  seastar::logger& logger()
  {
    return ceph::get_logger(ceph_subsys_mgrc);
  }
  template<typename Message, typename... Args>
  Ref<Message> make_message(Args&&... args)
  {
    // Message inherits from RefCountedObject, whose nref is 1 when it is
    // constructed, so we pass "add_ref = false" to intrusive_ptr's ctor
    return {new Message{std::forward<Args>(args)...}, false};
  }
}

using ceph::common::local_conf;

namespace ceph::mgr
{

// /////////////////////////////////////////////
// BackoffImp

BackoffImp::BackoffImp(std::chrono::milliseconds initial_bo, std::chrono::milliseconds max_bo) :
  max_backoff{max_bo},
  initial_backoff{initial_bo}
{}

bool BackoffImp::is_during_backoff()
{
  if (!backing_off)
    return false;
  if (seastar::lowres_clock::now() > backingoff_till) {
    backing_off = false;
    current_backoff = initial_backoff;
    return false;
  }
  logger().info("Mgr conn. delayed (duration: {})", current_backoff.count());
  return true;
}  

void BackoffImp::extend_backoff()
{
  if (backing_off) {
    current_backoff = std::min(2*current_backoff, max_backoff);
  } else {
    current_backoff = std::min(initial_backoff, max_backoff);
  }

  // make sure backoff is not disabled with a '0' max
  if (current_backoff.count()) {
    backing_off = true;
    backingoff_till = seastar::lowres_clock::now() + current_backoff;
  } else {
    // disabled by 0 max in the configuration
    backing_off = false;
  }

  logger().info("Mgr conn. backoff duration: {}", current_backoff.count());
}

// /////////////////////////////////////////////
// Client

Client::Client(ceph::net::Messenger& msgr,
                 WithStats& with_stats)
  : msgr{msgr},
    with_stats{with_stats},
    report_timer{[this] {report();}}
{}

seastar::future<> Client::start()
{
  return seastar::now();
}

seastar::future<> Client::stop()
{
  return gate.close().then([this] {
    backer.cancel_backoff();
    if (conn) {
      return conn->close();
    } else {
      return seastar::now();
    }
  });
}

seastar::future<> Client::ms_dispatch(ceph::net::Connection* conn,
                                      MessageRef m)
{
  switch(m->get_type()) {
  case MSG_MGR_MAP:
    return handle_mgr_map(conn, boost::static_pointer_cast<MMgrMap>(m));
  case MSG_MGR_CONFIGURE:
    return handle_mgr_conf(conn, boost::static_pointer_cast<MMgrConfigure>(m));
  default:
    return seastar::now();
  }
}

seastar::future<> Client::ms_handle_reset(ceph::net::ConnectionRef c)
{
  if (conn == c) {
    return reconnect();
  } else {
    return seastar::now();
  }
}

/* RRR dnm
  Assuming we will implement the pull-back reconnect strategy here. How will we handle a
  "not enough time has passed since last try" response?

  - the internal timer that caused us to request that connection should be reset/canceled.
  - anything else?

  How is it handled in the original code?

 */
seastar::future<> Client::reconnect_old()
{
  return (conn ? conn->close() : seastar::now()).then([this] {
    if (!mgrmap.get_available()) {
      logger().warn("No active mgr available yet");
      return seastar::now();
    }
    auto peer = mgrmap.get_active_addrs().front();
    return msgr.connect(peer, CEPH_ENTITY_TYPE_MGR).then(
      [this](auto xconn) {
        conn = xconn->release();
        // ask for the mgrconfigure message
        auto m = make_message<MMgrOpen>();
        m->daemon_name = local_conf()->name.get_id();
        return conn->send(std::move(m));
      });
  });
}


seastar::future<> Client::reconnect()
{
  logger().info("Mgr client reconnecting (prev-conn:{} bo:{} mgrmap:{})",
                 (conn?'+':'-'),
                 (backer.is_during_backoff()?'+':'-'),
                 (mgrmap.get_available()?"available":"none"));

  return (conn ? conn->close() : seastar::now()).then([this] {
    if (!mgrmap.get_available()) {
      if (!backer.is_during_backoff()) {
        // prevent multiple log entries
        logger().warn("No active mgr available yet");
      }
      //  no need for a backoff to be set here. The problem is not with the MGR. And -
      //  no penalty for rechecking for the map.
      return seastar::now();
    }
    if (backer.is_during_backoff()) {
      return seastar::now();
    }

    auto peer = mgrmap.get_active_addrs().front();

    return msgr.connect_wcatch(peer, CEPH_ENTITY_TYPE_MGR)
      .handle_exception([&backoff=backer](auto ep) mutable {
        // done below backoff.extend_backoff();
        return seastar::make_ready_future<ceph::net::XConnOrFault>(std::make_error_code(std::errc::connection_refused));
      }) // RRR or should we re-throw?
      .then([this](auto maybe_conn) {
        if (maybe_conn.has_error()) {
          backer.extend_backoff();
          return seastar::now();
        }

        conn = maybe_conn.value()->release();
        backer.cancel_backoff();
        // ask for the mgrconfigure message
        auto m = make_message<MMgrOpen>();
        m->daemon_name = local_conf()->name.get_id();
        return conn->send(std::move(m));
      });
  });
}

seastar::future<> Client::handle_mgr_map(ceph::net::Connection*,
                                         Ref<MMgrMap> m)
{
  logger().debug("{} {}", __func__, *m);
  mgrmap = m->get_map();
  
  if (!conn) {
    return reconnect();
  }
  if (conn->get_peer_addr() != mgrmap.get_active_addrs().legacy_addr()) {
    backer.cancel_backoff();
    return reconnect();
  }

  return seastar::now();
}

seastar::future<> Client::handle_mgr_conf(ceph::net::Connection*,
                                          Ref<MMgrConfigure> m)
{
  logger().info("{} {}", __func__, *m);
  report_period = std::chrono::seconds{m->stats_period};

  //  must probably prevent a 'report()' call if we are in the backoff period  
  if (report_period.count() && !report_timer.armed() ) {
    report();
  }
  return seastar::now();
}

void Client::report()
{
  seastar::with_gate(gate, [this] {
    auto pg_stats = with_stats.get_stats();
    return conn->send(std::move(pg_stats)).finally([this] {  // RRR How do we know we have a connection? note that this is not what was passed to ms_handle_...
      if (report_period.count()) {
        report_timer.arm(report_period);
      }
    });
  });
}

}
