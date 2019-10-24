// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#include "common/ceph_context.h"

#include <iostream>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include "crimson/admin/admin_socket.h"
#include "crimson/admin/osd_admin.h"
#include "crimson/osd/osd.h"
#include "common/config.h"
//#include "common/errno.h"
//#include "common/Graylog.h"

#include "log/Log.h"
//#include "common/valgrind.h"
//#include "include/spinlock.h"

//using ceph::HeartbeatMap;

// for CINIT_FLAGS
//#include "common/common_init.h"

#include <iostream>
//#include <pthread.h>

#ifndef WITH_SEASTAR
#error "this is a Crimson-specific implementation of some OSD APIs"
#endif

using ceph::bufferlist;
using ceph::common::local_conf;
using ceph::osd::OSD;
//using AdminSocket::hook_client_tag;

namespace ceph::osd {

/*!
  the hooks and states needed to handle OSD asok requests
*/
class OsdAdminImp {
  friend class OsdAdmin;
  friend class OsdAdminHookBase;

  OSD* m_osd;
  CephContext* m_cct;
  ceph::common::ConfigProxy& m_conf;

  ///
  ///  common code for all CephContext admin hooks
  ///
  class OsdAdminHookBase : public AdminSocketHook {
  protected:
    OsdAdminImp& m_osd_admin;

    /// the specific command implementation (would have used template specification, but can't yet use
    /// the command string as the template parameter).
    virtual seastar::future<> exec_command(Formatter* formatter, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) = 0;

    explicit OsdAdminHookBase(OsdAdminImp& master) : 
      m_osd_admin{master}
    {}

    // the high-level section is an array (affects the formatting)
    virtual bool format_as_array() const {
      return false;
    }

  public:
    /*!
        \retval 'false' for hook execution errors
     */
    seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
	                       std::string_view format, bufferlist& out) override {
      //std::cerr << "OSDADH call  1" << std::endl;
      try {
        //
        //  some preliminary (common) parsing:
        //
        unique_ptr<Formatter> f{Formatter::create(format, "json-pretty"sv, "json-pretty"s)}; // RRR consider destructing in 'catch'
        std::string section(command);
        boost::replace_all(section, " ", "_");
        if (format_as_array()) {
          f->open_array_section(section.c_str());
        } else {
          f->open_object_section(section.c_str());
        }

	//  call the command-specific hook.
	//  A note re error handling:
	//	- will be modified to use the new 'erroretor'. For now:
	//	- exec_command() may throw or return an exceptional future. We return a message starting
	//	  with "error" on both failure scenarios.
        try {
          (void)exec_command(f.get(), command, cmdmap, format, out).then_wrapped([&f](auto p) {
            try {
              (void)p.get();
              //auto resp = p.get();
            } catch (std::exception& ex) {
              f->dump_string("error", ex.what());
              //std::cout << "request error: " << ex.what() << std::endl;
            }
          });
        } catch ( ... ) {
          f->dump_string("error", std::string(command) + " failed");
          //std::cout << "\n\nexecution throwed\n\n";
        }
        f->close_section();
        f->flush(out);
      } catch (const bad_cmd_get& e) {
        return seastar::make_ready_future<bool>(false);
      } catch ( ... ) {
        return seastar::make_ready_future<bool>(false);
      }
      std::cerr << "OSDADH call  111" << std::endl;

      return seastar::make_ready_future<bool>(true);
    }
  };


  ///
  ///  An Osd admin hook: OSD status
  ///
  class OsdStatusHook : public OsdAdminHookBase {
  public:
    explicit OsdStatusHook(OsdAdminImp& master) : OsdAdminHookBase(master) {};
    seastar::future<> exec_command(Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) final {

      //std::cerr << "OsdStatusHook 1" << std::endl;
      // wrong? f->open_object_section("status");
      try {
        f->dump_stream("cluster_fsid") << m_osd_admin.osd_superblock().cluster_fsid;
      } catch (...) {

      }
      f->dump_stream("osd_fsid") << m_osd_admin.osd_superblock().osd_fsid;
      f->dump_unsigned("whoami", m_osd_admin.osd_superblock().whoami);
      // \todo f->dump_string("state", get_state_name(get_state()));
      //std::cerr << "OsdStatusHook 11" << std::endl;
      f->dump_unsigned("oldest_map", m_osd_admin.osd_superblock().oldest_map);
      f->dump_unsigned("newest_map", m_osd_admin.osd_superblock().newest_map);
      // \todo f->dump_unsigned("num_pgs", num_pgs);
      //std::cerr << "OsdStatusHook 111" << std::endl;
      return seastar::now();
    }
  };

  ///
  ///  provide the hooks with access to OSD internals 
  ///
  const OSDSuperblock& osd_superblock() {
    return m_osd->superblock;
  }

  OsdStatusHook   osd_status_hook;

  std::atomic_flag  m_no_registrations{false}; // 'double negative' as that matches 'atomic_flag' "direction"

public:

  OsdAdminImp(OSD* osd, CephContext* cct, ceph::common::ConfigProxy& conf)
    : m_osd{osd}
    , m_cct{cct}
    , m_conf{conf}
    , osd_status_hook{*this}
  {
    register_admin_commands();
  }

  ~OsdAdminImp() {
    unregister_admin_commands();
  }

  void register_admin_commands() {  // should probably be a future<void>

    auto admin_if = m_cct->get_admin_socket();

    (void)seastar::when_all_succeed(
            [this, admin_if](){ return admin_if->register_promise(AdminSocket::hook_client_tag{this}, "status",   "status",  &osd_status_hook,      "OSD status"); },
            [this, admin_if](){ return admin_if->register_promise(AdminSocket::hook_client_tag{this}, "status2",  "status 2",  &osd_status_hook,      "OSD status"); }
          );
    //admin_if->register_command(AdminSocket::hook_client_tag{this}, "status",    "status",  &osd_status_hook,      "OSD status");
    //admin_if->register_command(AdminSocket::hook_client_tag{this}, "ZZ_ZZ_ZZ_ZZ",    "ZZ_ZZ_ZZ_ZZ",  &osd_status_hook,      "OSD status");
  }

  void unregister_admin_commands() {
    if (m_no_registrations.test_and_set()) {
      //  already un-registered
      return;
    }
    //  unregister all our hooks. \todo add an API to AdminSocket hooks, to have a common identifying tag
    //  (probably the address of the registering object)

    auto admin_if = m_cct->get_admin_socket();
    if (admin_if) {
      // guarding against possible (?) destruction order problems
      try {
        (void)admin_if->unregister_client(AdminSocket::hook_client_tag{this}).finally([]{}).discard_result();
      } catch (...) {
        std::cerr << " failed unregistering" << std::endl;
      }
    }
  }
};

//
//  some Pimpl details:
//
OsdAdmin::OsdAdmin(OSD* osd, CephContext* cct, ceph::common::ConfigProxy& conf)
  : m_imp{ std::make_unique<ceph::osd::OsdAdminImp>(osd, cct, conf) }
{}

void OsdAdmin::unregister_admin_commands()
{
  m_imp->unregister_admin_commands();
}

OsdAdmin::~OsdAdmin() = default;

} // namespace
