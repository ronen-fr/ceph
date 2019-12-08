// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/ceph_context.h"

#include <iostream>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include "seastar/core/future.hh"
#include "seastar/core/thread.hh"
#include "crimson/admin/admin_socket.h"
#include "crimson/admin/osd_admin.h"
#include "crimson/osd/osd.h"
#include "crimson/osd/exceptions.h"
#include "common/config.h"
#include "crimson/common/log.h"
#include <iostream>

#ifndef WITH_SEASTAR
#error "this is a Crimson-specific implementation of some OSD APIs"
#endif

using crimson::osd::OSD;

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

namespace crimson::admin {

/**
  the hooks and states needed to handle OSD asok requests
*/
class OsdAdminImp {
  friend class OsdAdmin;
  friend class OsdAdminHookBase;

  OSD*                          m_osd;
  CephContext*                  m_cct;
  crimson::common::ConfigProxy& m_conf;

  //  shared-ownership of the socket server itself, to guarantee its existence
  //  until we have a chance to remove our registration:
  AsokRegistrationRes m_socket_server;

  /**
       Common code for all OSD admin hooks.
       Adds access to the owning OSD.
  */
  class OsdAdminHookBase : public AdminSocketHook {
   protected:
    OsdAdminImp& m_osd_admin;

    /// the specific command implementation
    virtual seastar::future<> exec_command(
      [[maybe_unused]] Formatter*       formatter,
      [[maybe_unused]] std::string_view command,
      [[maybe_unused]] const cmdmap_t&  cmdmap,
      [[maybe_unused]] std::string_view format,
      [[maybe_unused]] bufferlist&      out) const = 0;

    explicit OsdAdminHookBase(OsdAdminImp& master) : m_osd_admin{ master } {}
  };

  /**
       An OSD admin hook: OSD status
   */
  class OsdStatusHook : public OsdAdminHookBase {
   public:
    explicit OsdStatusHook(OsdAdminImp& master) : OsdAdminHookBase(master){};
    seastar::future<> exec_command(Formatter*                        f,
                                   [[maybe_unused]] std::string_view command,
                                   [[maybe_unused]] const cmdmap_t&  cmdmap,
                                   std::string_view                  format,
                                   [[maybe_unused]] bufferlist& out) const final
    {

      f->dump_stream("cluster_fsid")
        << m_osd_admin.osd_superblock().cluster_fsid;
      f->dump_stream("osd_fsid") << m_osd_admin.osd_superblock().osd_fsid;
      f->dump_unsigned("whoami", m_osd_admin.osd_superblock().whoami);
      f->dump_string("state", m_osd_admin.osd_state_name());
      f->dump_unsigned("oldest_map", m_osd_admin.osd_superblock().oldest_map);
      f->dump_unsigned("newest_map", m_osd_admin.osd_superblock().newest_map);

      f->dump_unsigned("num_pgs", m_osd_admin.m_osd->pg_map.get_pgs().size());
      return seastar::now();
    }
  };

  /**
       An OSD admin hook: send beacon
   */
  class SendBeaconHook : public OsdAdminHookBase {
   public:
    explicit SendBeaconHook(OsdAdminImp& master) : OsdAdminHookBase(master){};
    seastar::future<> exec_command(Formatter*                        f,
                                   [[maybe_unused]] std::string_view command,
                                   [[maybe_unused]] const cmdmap_t&  cmdmap,
                                   [[maybe_unused]] std::string_view format,
                                   [[maybe_unused]] bufferlist& out) const final
    {
      if (m_osd_admin.get_osd_state().is_active()) {
        return m_osd_admin.m_osd->send_beacon();
      } else {
        return seastar::now();
      }
    }
  };

  /**
       A test hook that throws or returns an exceptional future
   */
  class TestThrowHook : public OsdAdminHookBase {
   public:
    explicit TestThrowHook(OsdAdminImp& master) : OsdAdminHookBase(master){};
    seastar::future<> exec_command(Formatter*                        f,
                                   std::string_view                  command,
                                   [[maybe_unused]] const cmdmap_t&  cmdmap,
                                   [[maybe_unused]] std::string_view format,
                                   [[maybe_unused]] bufferlist& out) const final
    {
      if (command == "fthrow") {
        return seastar::make_exception_future<>(
          std::errc::no_message_available);
      }

      if (command == "stoposd") {
        // test admin-socket termination
        std::ignore =
          seastar::async([this] { m_osd_admin.m_osd->stop().get(); });
        return seastar::now();
      }

      throw(std::invalid_argument("TestThrowHook"));
    }
  };

  /**
       provide the hooks with access to OSD internals
  */
  const OSDSuperblock& osd_superblock() const
  {
    return m_osd->superblock;
  }

  OSDState get_osd_state() const
  {
    return m_osd->state;
  }

  string_view osd_state_name() const
  {
    return m_osd->state.to_string();
  }

  OsdStatusHook  osd_status_hook;
  SendBeaconHook send_beacon_hook;
  TestThrowHook  osd_test_throw_hook;

 public:
  OsdAdminImp(OSD* osd, CephContext* cct, crimson::common::ConfigProxy& conf)
      : m_osd{ osd }
      , m_cct{ cct }
      , m_conf{ conf }
      , osd_status_hook{ *this }
      , send_beacon_hook{ *this }
      , osd_test_throw_hook{ *this }
  {}

  ~OsdAdminImp()
  {
    // our registration with the admin_socket server was already removed by
    // 'OsdAdmin' - our 'pimpl' owner. Thus no need for:
    //   unregister_admin_commands();
  }

  seastar::future<> register_admin_commands()
  {
    static const std::vector<AsokServiceDef> hooks_tbl{
      // clang-format off
        AsokServiceDef{"status",      "status",        &osd_status_hook,
                      "OSD status"}
      , AsokServiceDef{"send_beacon", "send_beacon",   &send_beacon_hook,
                      "send OSD beacon to mon immediately"}
      , AsokServiceDef{"throw",       "throw",         &osd_test_throw_hook,
                      ""}  // dev tool
      , AsokServiceDef{"stoposd",     "stoposd",       &osd_test_throw_hook,
                      ""}  // dev tool
      , AsokServiceDef{"fthrow",      "fthrow",        &osd_test_throw_hook,
                      ""}  // dev tool
      // clang-format on
    };

    return m_cct->get_admin_socket()
      ->register_server(AdminSocket::hook_server_tag{ this }, hooks_tbl)
      .then([this](AsokRegistrationRes res) { m_socket_server = res; });
  }

  seastar::future<> unregister_admin_commands()
  {
    if (!m_socket_server.has_value()) {
      logger().warn("{}: OSD asok APIs already removed", __func__);
      return seastar::now();
    }

    AdminSocketRef srv{ std::move(m_socket_server.value()) };
    m_socket_server.reset();

    auto admin_if = m_cct->get_admin_socket();
    if (admin_if) {
      return admin_if->unregister_server(AdminSocket::hook_server_tag{ this },
                                         std::move(srv));
    } else {
      return seastar::now();
    }
  }
};

//
//  some PIMPL details:
//
OsdAdmin::OsdAdmin(OSD*                          osd,
                   CephContext*                  cct,
                   crimson::common::ConfigProxy& conf)
    : m_imp{ std::make_unique<crimson::admin::OsdAdminImp>(osd, cct, conf) }
{}

seastar::future<> OsdAdmin::register_admin_commands()
{
  return m_imp->register_admin_commands();
}

seastar::future<> OsdAdmin::unregister_admin_commands()
{
  if (m_imp) {
    auto moved_m_imp{ std::move(m_imp) };
    return moved_m_imp->unregister_admin_commands();
  } else
    return seastar::now();
}

OsdAdmin::~OsdAdmin() = default;

}  // namespace crimson::admin
