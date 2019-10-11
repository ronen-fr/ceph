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
#include "common/admin_socket.h"
//#include "common/code_environment.h"
//#include "common/ceph_mutex.h"
//#include "common/debug.h"
#include "common/config.h"
//#include "common/ceph_crypto.h"
//#include "common/lockdep.h"
//#include "common/HeartbeatMap.h"
#include "common/errno.h"
#include "common/Graylog.h"

#include "log/Log.h"

//#include "auth/Crypto.h"
//#include "include/str_list.h"
//#include "common/config.h"
//#include "common/config_obs.h"
//#include "common/PluginRegistry.h"
#include "common/valgrind.h"
//#include "include/spinlock.h"

using ceph::bufferlist;
using ceph::HeartbeatMap;

// for CINIT_FLAGS
#include "common/common_init.h"

#include <iostream>
//#include <pthread.h>

#ifndef WITH_SEASTAR
#error "this is a Crimson-specific implementation of some CephContext APIs"
#endif



using ceph::common::local_conf;

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
  ///  common code for all CephContext admin hooks
  ///
  class CephContextHookBase : public AdminSocketHook {
  protected:
    ContextConfigAdminImp& m_config_admin;

    /// the specific command implementation (would have used template specification, but can't yet use
    /// the command string as the template parameter).
    virtual seastar::future<> exec_command(Formatter* formatter, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) = 0;

  public: // RRR why public?
    explicit CephContextHookBase(ContextConfigAdminImp& master) : m_config_admin{master} {}

    /*!
        \retval 'false' for hook execution errors
     */
    bool call(std::string_view command, const cmdmap_t& cmdmap,
	      std::string_view format, bufferlist& out) override {
      try {
        //
        //  some preliminary (common) parsing:
        //

        // RRR to do check that creating a formatter does not block
        unique_ptr<Formatter> f{Formatter::create(format, "json-pretty"sv, "json-pretty"s)}; // RRR consider destructing in 'catch'
        std::string section(command);
        boost::replace_all(section, " ", "_");
        f->open_object_section(section.c_str());

	//  call the command-specific hook.
	//  A note re error handling:
	//	- will be modified to use the new 'erroretor'. For now:
	//	- exec_command() may throw or return an exceptional future. We return a message starting
	//	  with "error" on both failure scenarios.
        try {
          (void)exec_command(f.get(), command, cmdmap, format, out).then_wrapped([&f](auto p) {
            try {
              (void)p.get();
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
        return false;
      } catch ( ... ) {
        return false;
      }

      return true;
    }
  };

  ///
  ///  A CephContext admin hook: listing the configuration values
  ///
  class ConfigShowHook : public CephContextHookBase {
  public:
    explicit ConfigShowHook(ContextConfigAdminImp& master) : CephContextHookBase(master) {};
    seastar::future<> exec_command(Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) final {
      std::vector<std::string> listing;
      m_config_admin.m_conf.get_all_sections(listing);
      for (const auto& s : listing) {
        f->dump_string("section", s);
      }

      // does not exist: m_config_admin.m_conf.get_config().show_config(f);
      return seastar::now();
    }
  };

  ///
  ///  A CephContext admin hook: fetching the value of a specific configuration item
  ///
  class ConfigGetHook : public CephContextHookBase {
  public:
    explicit ConfigGetHook(ContextConfigAdminImp& master) : CephContextHookBase(master) {};
    seastar::future<> exec_command(Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) final {
      std::string var;
      if (!cmd_getval<std::string>(nullptr, cmdmap, "var", var)) {
	f->dump_string("error", "syntax error: 'config get <var>'");

      } else {

        try {
          std::string conf_val = m_config_admin.m_conf.get_val<std::string>(var.c_str());
          f->dump_string(var.c_str(), conf_val.c_str());
        } catch ( ... ) {
          f->dump_string("error", "unrecognized configuration item " + std::string(command));
        }
      }
      return seastar::now();
    }
  };

  ///
  ///  A CephContext admin hook: setting the value of a specific configuration item
  ///  (a real example: {"prefix": "config set", "var":"debug_osd", "val": ["30/20"]} )
  ///
  class ConfigSetHook : public CephContextHookBase {
  public:
    explicit ConfigSetHook(ContextConfigAdminImp& master) : CephContextHookBase(master) {};
    seastar::future<> exec_command(Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                      std::string_view format, bufferlist& out) final {
      std::string var;
      std::vector<std::string> new_val;
      if (!cmd_getval<std::string>(nullptr, cmdmap, "var", var) ||
          !cmd_getval(nullptr, cmdmap, "val", new_val)) {

	f->dump_string("error", "syntax error: 'config set <var> <value>'");
        return seastar::now();

      } else {
        // val may be multiple words
	string valstr = str_join(new_val, " ");

        return m_config_admin.m_conf.set_val(var, valstr).then_wrapped([&f, &command](auto p) {
          try {
              (void)p.get();
              f->dump_string("success", "command");
            } catch (std::exception& ex) {
              f->dump_string("error", ex.what());
            }
            return seastar::now();
        });
      }
    }
  };

  ConfigShowHook config_show_hook;
  ConfigGetHook  config_get_hook;
  ConfigSetHook  config_set_hook;

  std::atomic_flag  m_no_registrations{false}; // 'double negative' as that matches 'atomic_flag' "direction"


public:

  ContextConfigAdminImp(CephContext* cct, ceph::common::ConfigProxy& conf)
    : m_cct{cct}
    , m_conf{conf}
    , config_show_hook{*this}
    , config_get_hook{*this}
    , config_set_hook{*this}
  {
    register_admin_commands();
  }

  ~ContextConfigAdminImp() {
    unregister_admin_commands();
  }

  void register_admin_commands() {  // should probably be a future<void>

    auto admin_if = m_cct->get_admin_socket();

    admin_if->register_command("config show",    "config show",  &config_show_hook,      "lists all conf items");
    admin_if->register_command("config get",     "config get",   &config_get_hook,       "fetches a conf value");
    admin_if->register_command("config set",     "config set",   &config_set_hook,       "sets a conf value");
    //admin_socket->register_command("config unset", "config unset name=var,type=CephString",  _admin_hook, "config unset <field>: unset a config variable");
  }

  void unregister_admin_commands() {
    if (m_no_registrations.test_and_set()) {
      //  already un-registered
      return;
    }
    //  unregister all our hooks. \todo add an API to AdminSocket hooks, to have a common identifying tag
    //  (probably the address of the registering object)

    // ... \todo
  }
};

//
//  some Pimpl details:
//
ContextConfigAdmin::ContextConfigAdmin(CephContext* cct, ceph::common::ConfigProxy& conf)
  : m_imp{ std::make_unique<ContextConfigAdminImp>(cct, conf) }
{}

void ContextConfigAdmin::unregister_admin_commands()
{
  m_imp->unregister_admin_commands();
}

ContextConfigAdmin::~ContextConfigAdmin() = default;
