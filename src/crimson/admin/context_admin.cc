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
#ifndef WITH_SEASTAR
#error "this is a Crimson-specific implementation of some CephContext APIs"
#endif

#include <iostream>
#include <atomic>
#include <boost/algorithm/string.hpp>
#include "seastar/core/sleep.hh"
#include "common/config.h"
#include "common/errno.h"
#include "common/Graylog.h"

#include "crimson/common/log.h"
#include "common/valgrind.h"

// for CINIT_FLAGS
#include "common/common_init.h"
#include <iostream>

#include "common/ceph_context.h"

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
  //
  //  ContextConfigAdminImp objects are held by CephContext objects. m_cct points back to our master.
  //
  CephContext* m_cct;
  ceph::common::ConfigProxy& m_conf;

  //  shared-ownership of the socket server itself, to guarantee its existence until we have
  //  a chance to remove our registration:
  AsokRegistrationRes m_socket_server;

  friend class CephContextHookBase;
  friend class ConfigGetHook;

  //
  //  common code for all CephContext admin hooks.
  //  Adds access to the configuration object and to the
  //   parent Context.
  //
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

      return seastar::do_with(std::vector<std::string>(), 
        [this, f](std::vector<std::string>& k_list) {
          return m_config_admin.m_conf.show_config(k_list).
            then([&k_list, f]() {
              for (const auto& k : k_list) {
                f->dump_string("conf-item", k);
                //logger().warn("---> {}\n", k);
              }
              return seastar::now();
            });
          }).
          finally([/*&k_list,f*/]() {
            return seastar::now();
          });
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
    }
  };

  /*!
       A CephContext admin hook: calling assert (if allowed by 'debug_asok_assert_abort')
   */
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

  /*!
       A test hook that throws or returns an exceptional future
   */
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
            logger().warn("{}: {} {}", __func__, (int)getpid(), (uint64_t)(this));

          return;
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
    logger().warn("{}: {} {} (size:xx)", __func__, (int)getpid(), (uint64_t)(this));

    //std::cerr << "ContextConfigAdminImp registering\n";
    static const std::vector<AsokServiceDef> hooks_tbl{
        AsokServiceDef{"config show",    "config show",  &config_show_hook,      "dump current config settings"}
      , AsokServiceDef{"config get",     "onfig get name=var,type=CephString",
                                                         &config_get_hook,       "config get <field>: get the config value"}
      , AsokServiceDef{"config set",     "config set name=var,type=CephString name=val,type=CephString,n=N",
                                                         &config_set_hook,       "config set <field> <val> [<val> ...]: set a config variable"}
      , AsokServiceDef{"assert",         "assert",       &assert_hook,           "asserts"}
      , AsokServiceDef{"throwCtx",       "throwCtx",     &ctx_test_throw_hook,   "dev throw"}
      , AsokServiceDef{"fthrowCtx",      "fthrowCtx",    &ctx_test_throw_hook,   "dev throw"}
    };

    m_socket_server = m_cct->get_admin_socket()->server_registration(AdminSocket::hook_server_tag{this}, hooks_tbl);
    //logger().warn("{}: {} {} (size:{})", __func__, (int)getpid(), (uint64_t)(this), servers.size());

    //admin_socket->register_command("config unset", "config unset name=var,type=CephString",  _admin_hook, "config unset <field>: unset a config variable");
  }

  seastar::future<> unregister_admin_commands() {

    logger().warn("{}:Z {} this:{} {} cct:{} adif:{}", __func__, (int)getpid(), (uint64_t)(this),
                (m_socket_server?"+":"-"),
                (uint64_t)(m_cct),
                (uint64_t)(m_cct->get_admin_socket()));

    if (!m_socket_server) {
       logger().warn("{} m_s", __func__);
      return seastar::now();
    }

    auto admin_if = m_cct->get_admin_socket();
    if (!admin_if) {
      logger().warn("{}:Z no admin_if", __func__);
      return seastar::now();
    }

    logger().warn("{}:Z5 {}", __func__, (uint64_t)(& (*(*m_socket_server))));

    //  we are holding a shared-ownership of the admin socket server, just so that we
    //  can keep it alive until after our de-registration.
    //AdminSocketRef srv{std::move(*m_socket_server)};
    AdminSocketRef srv = *m_socket_server;
    m_socket_server = std::nullopt; // should be redundant

    // note that unregister_server() closes a seastar::gate (i.e. - it blocks)
    logger().warn("cad imp unreg {}", (uint64_t)(&(*srv)));
    return admin_if->unregister_server(AdminSocket::hook_server_tag{this}, std::move(srv));
  }
};

//
//  some Pimpl details:
//
ContextConfigAdmin::ContextConfigAdmin(CephContext* cct, ceph::common::ConfigProxy& conf)
  : m_imp{ std::make_unique<ContextConfigAdminImp>(cct, conf) }
  , m_cct{cct}
{}

seastar::future<>  ContextConfigAdmin::unregister_admin_commands()
{
  return m_imp->unregister_admin_commands();
}

ContextConfigAdmin::~ContextConfigAdmin()
{
  logger().warn("{}: ~ContextConfigAdmin {} {} {}", __func__,
                (int)getpid(), (uint64_t)(this), (m_imp ? "++" : "--"));

  // relinquish control over the actual implementation object, as that one should only be
  // destructed after the relevant seastar::gate closes
  //std::ignore = seastar::do_with(std::move(m_imp), [](auto& imp) {
    // test using sleep()
    seastar::sleep(1s).
    //then([imp_ptr = imp.get()]() {
    then([imp_ptr = std::move(m_imp)]() {
       logger().warn("{} step 2: {}", __func__, (int)getpid());
       if (!imp_ptr) {// RRR
         logger().warn("{} step s: {}", __func__, (int)getpid());
         return seastar::now();
       }
       //assert(imp_ptr);
       return imp_ptr->unregister_admin_commands();
    }).discard_result();
  //});
}