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
/*!
  \brief implementation of the 'admin_socket' API of (Crimson) Ceph Context

  Main functionality:
  - manipulating Context-level configuraion
  - process-wide commands ('abort', 'assert')
  - ...
 */
#include "common/ceph_context.h"

#include <iostream>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include "crimson/admin/admin_socket.h"
#include "common/config.h"
#include "common/errno.h"
#include "common/Graylog.h"

#include "crimson/common/log.h"
#include "common/valgrind.h"



// for CINIT_FLAGS
#include "common/common_init.h"

#include <iostream>
//#include <pthread.h>

#ifndef WITH_SEASTAR
#error "this is a Crimson-specific implementation of some CephContext APIs"
#endif


using ceph::bufferlist;
using ceph::HeartbeatMap;
using ceph::common::local_conf;

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

/*!
  the hooks and states needed to handle configuration requests
*/
class ContextConfigAdminImp {
  friend class ContextConfigAdmin;
  ///
  ///  ContextConfigAdminImp objects are held by CephContext objects. m_cct points back to our master.
  ///
  CephContext* m_cct;
  ceph::common::ConfigProxy& m_conf;
  friend class CephContextHookBase;
  friend class ConfigGetHook;

  ///
  ///  common code for all CephContext admin hooks.
  ///  Adds access to the configuration object and to the
  //   parent Context.
  ///
  class CephContextHookBase : public AdminSocketHook {
  protected:
    ContextConfigAdminImp& m_config_admin;

    /// the specific command implementation
    seastar::future<> exec_command(Formatter* formatter, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) const override = 0;

    explicit CephContextHookBase(ContextConfigAdminImp& master) : m_config_admin{master} {}
  };

  ///
  ///  A CephContext admin hook: listing the configuration values
  ///
  class ConfigShowHook : public CephContextHookBase {
  public:
    explicit ConfigShowHook(ContextConfigAdminImp& master) : CephContextHookBase(master) {};
    seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) const final {

      std::vector<std::string> k_list;
      return m_config_admin.m_conf.show_config(k_list).
        then([&k_list, &f]() {
          for (const auto& k : k_list) {
            f->dump_string("conf-item", k);
            logger().warn("---> {}\n", k);
          }
          return seastar::now();
        }).
        finally([&k_list,f](){return seastar::now();});
    }
  };

  /*!
       A CephContext admin hook: fetching the value of a specific configuration item
   */
  class ConfigGetHook : public CephContextHookBase {
  public:
    explicit ConfigGetHook(ContextConfigAdminImp& master) : CephContextHookBase(master) {};
    seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) const final {
      std::string var;
      if (!cmd_getval<std::string>(nullptr, cmdmap, "var", var)) {
	f->dump_string("error", "syntax error: 'config get <var>'");

      } else {

        try {
          std::string conf_val = m_config_admin.m_conf.get_val<std::string>(var.c_str());
          f->dump_string(var.c_str(), conf_val.c_str());
        } catch ( ... ) {
          f->dump_string("error", "unrecognized configuration item " + var);
        }
      }
      return seastar::now();
    }
  };

  /*!
       A CephContext admin hook: setting the value of a specific configuration item
       (a real example: {"prefix": "config set", "var":"debug_osd", "val": ["30/20"]} )
   */
  class ConfigSetHook : public CephContextHookBase {
  public:
    explicit ConfigSetHook(ContextConfigAdminImp& master) : CephContextHookBase(master) {};

    seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) const final {
      std::string var;
      std::vector<std::string> new_val;
      if (!cmd_getval<std::string>(nullptr, cmdmap, "var", var) ||
          !cmd_getval(nullptr, cmdmap, "val", new_val)) {

	f->dump_string("error", "syntax error: 'config set <var> <value>'");
        return seastar::now();

      } else {
        // val may be multiple words
	string valstr = str_join(new_val, " ");

        return m_config_admin.m_conf.set_val(var, valstr).then_wrapped([=](auto p) {
          if (p.failed()) {
            f->dump_string("error setting", var.c_str());
          } else {
            f->dump_string("success", command);
          }
          return seastar::now();
        });
      }
      #if 0
          try {({
              (void)p.get();
              f->dump_string("success", command);
            } catch (std::exception& ex) {
              f->dump_string("error", ex.what());
            }
            return seastar::now();
        });
      #endif
    }
  };

  ///
  ///  A CephContext admin hook: calling assert (if allowed by 'debug_asok_assert_abort')
  ///
  class AssertAlwaysHook : public CephContextHookBase {
  public:
    explicit AssertAlwaysHook(ContextConfigAdminImp& master) : CephContextHookBase(master) {};
    seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) const final {
      bool assert_conf = m_config_admin.m_conf.get_val<bool>("debug_asok_assert_abort") || /*for now*/ true;
      if (!assert_conf) {
	f->dump_string("error", "configuration set to disallow asok assert");
	return seastar::now();
      }
      ceph_assert_always(0);
      return seastar::now();
     }
  };

  ///
  ///  A test hook that throws or returns an exceptional future
  ///
  class TestThrowHook : public CephContextHookBase {
  public:
    explicit TestThrowHook(ContextConfigAdminImp& master) : CephContextHookBase(master) {};
    seastar::future<> exec_command(Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
                              std::string_view format, bufferlist& out) const final {

      if (command == "fthrowCtx")
        return seastar::make_exception_future<>(std::system_error{1, std::system_category()});
      throw(std::invalid_argument("TestThrowHook"));
    }
  };

  ConfigShowHook   config_show_hook;
  ConfigGetHook    config_get_hook;
  ConfigSetHook    config_set_hook;
  AssertAlwaysHook assert_hook;
  TestThrowHook    ctx_test_throw_hook;  // for development testing

  std::atomic_flag  m_no_registrations{false}; // 'double negative' as that matches 'atomic_flag' "direction"

public:

  ContextConfigAdminImp(CephContext* cct, ceph::common::ConfigProxy& conf)
    : m_cct{cct}
    , m_conf{conf}
    , config_show_hook{*this}
    , config_get_hook{*this}
    , config_set_hook{*this}
    , assert_hook{*this}
    , ctx_test_throw_hook{*this}
  {
    register_admin_commands();
  }

  ~ContextConfigAdminImp() {
    //std::unique_ptr<ContextConfigAdmin> moved_context_admin{std::move(m_cct->asok_config_admin)};
    //m_cct->asok_config_admin = nullptr;
    std::ignore = seastar::do_with(std::move(m_cct)/*, std::move(m_cct->asok_config_admin)*/,
                                 [this] (auto& cct/*, auto& cxadmin*/) mutable {
      cct->get();
      return unregister_admin_commands().then([cct](){
        //cct->put();
      }).then([this](){
        std::cerr << "~ContextConfigAdminImp() " << (uint64_t)(this) << std::endl;

      });
    });
  }

  void register_admin_commands() {  // should probably be a future<void>

    //std::cerr << "ContextConfigAdminImp registering\n";
    static const std::vector<AsokServiceDef> hooks_tbl{
        AsokServiceDef{"config show",    "config show",  &config_show_hook,      "lists all conf items"}
      , AsokServiceDef{"config get",     "config get",   &config_get_hook,       "fetches a conf value"}
      , AsokServiceDef{"config set",     "config set",   &config_set_hook,       "sets a conf value"}
      , AsokServiceDef{"assert",         "assert",       &assert_hook,           "asserts"}
      , AsokServiceDef{"throwCtx",       "throwCtx",     &ctx_test_throw_hook,   "dev throw"}
      , AsokServiceDef{"fthrowCtx",      "fthrowCtx",    &ctx_test_throw_hook,   "dev throw"}
    };

    std::ignore = m_cct->get_admin_socket()->server_registration(AdminSocket::hook_server_tag{this}, hooks_tbl);

    //admin_socket->register_command("config unset", "config unset name=var,type=CephString",  _admin_hook, "config unset <field>: unset a config variable");
  }

  seastar::future<> unregister_admin_commands() {
    if (m_no_registrations.test_and_set()) {
      //  already un-registered
      return seastar::now();
    }

    // note that unregister_server() closes a seastar::gate (i.e. - it blocks)
    auto admin_if = m_cct->get_admin_socket();
    return admin_if->unregister_server(AdminSocket::hook_server_tag{this});
  }
};

//
//  some Pimpl details:
//
ContextConfigAdmin::ContextConfigAdmin(CephContext* cct, ceph::common::ConfigProxy& conf)
  : m_imp{ std::make_unique<ContextConfigAdminImp>(cct, conf) }
{}

seastar::future<>  ContextConfigAdmin::unregister_admin_commands()
{
  return m_imp->unregister_admin_commands();
}

ContextConfigAdmin::~ContextConfigAdmin() = default;
