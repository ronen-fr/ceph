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
#pragma once

/*!
  A Crimson-wise version of the src/common/admin_socket.h

  Running on a single core:
  - the hooks database is only manipulated on that main core. Hook-servers running on other cores
    dispatch the register/unregister requests to that main core.
  - incoming requests arriving on the admin socket are only received on that specific core. The actual
    operation is delegated to the relevant core if needed.
 */
#include <string>
#include <string_view>
#include <map>
#include "seastar/core/future.hh"
#include "seastar/core/gate.hh"
#include "seastar/core/iostream.hh"
#include "common/cmdparse.h"

class AdminSocket;
class CephContext;

using namespace std::literals;

inline constexpr auto CEPH_ADMIN_SOCK_VERSION = "2"sv;

/*!
  A specific hook must implement exactly one of the two interfaces:
  (1) call(command, cmdmap, format, out) 
  or
  (2) exec_command(formatter, command, cmdmap, format, out)

  The default implementation of (1) above calls exec_command() after handling most
  of the boiler-plate choirs:
  - setting up the formmater, with an appropiate 'section' already opened;
  - handling possible failures (exceptions or future_exceptions) returned by (2)
  - flushing the output to the outgoing bufferlist.
*/
class AdminSocketHook {
public:
  /*!
      \retval 'false' for hook execution errors
  */
  virtual seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
		                     std::string_view format, ceph::buffer::list& out) const;

  virtual ~AdminSocketHook() {}

protected:
  virtual seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const {
    return seastar::now();
  }

  // the high-level section is an array (affects the formatting)
  virtual bool format_as_array() const {
    return false;
  }
};

struct AsokServiceDef {
  //AsokServiceDefinition(const std::string& command, //!< the sequence of words that should be used
  //                      const std::string& desc,    //!< the syntax
  //                      const AdminSocketHook* hook,
  //                      const std::string help_message)
  //  : command{command}
  //  , cmddesc{desc}
  //  , hook{hook}
  //  , help{help_message}
  //{}
  const std::string command;
  const std::string cmddesc;
  const AdminSocketHook* hook;
  const std::string help;
};

struct AsokRegistrationRes {
  //seastar::gate* server_gate;         // a pointer &! a ref, as the registration might fail
  bool           registration_ok;     // failure: if server ID already taken
  bool           double_registration; //!< leaving it to the registering server to
                                     //   decide whether this is a bug
};


class AdminSocket {
public:
  AdminSocket(CephContext* cct);
  ~AdminSocket();

  AdminSocket(const AdminSocket&) = delete;
  AdminSocket& operator =(const AdminSocket&) = delete;
  AdminSocket(AdminSocket&&) = delete;
  AdminSocket& operator =(AdminSocket&&) = delete;

  using hook_server_tag = const void*;

  seastar::future<> init(const std::string& path);

  /*!
   * register an admin socket command
   *
   * The command is registered under a command string. Incoming
   * commands are split by space and matched against the longest
   * registered command. For example, if 'foo' and 'foo bar' are
   * registered, and an incoming command is 'foo bar baz', it is
   * matched with 'foo bar', while 'foo fud' will match 'foo'.
   *
   * The entire incoming command string is passed to the registered
   * hook.
   *
   * @server_tag a tag identifying the server registering the hook
   * @param command command string
   * @param cmddesc command syntax descriptor
   * @param hook implementation
   * @param help help text.  if empty, command will not be included in 'help' output.
   *
   * @return 'true' for success, 'false' if command already registered.
   */
  //seastar::future<bool> register_command(hook_server_tag  server_tag,
  //                                       std::string command,
  //                                       std::string cmddesc,
  //                                       AdminSocketHook *hook,
  //                                       std::string help);

  //seastar::future<AsokRegistrationRes>
  AsokRegistrationRes
  server_registration(hook_server_tag  server_tag,
                      const std::vector<AsokServiceDef>& hv); 

 /* bool register_immediate(hook_server_tag  server_tag,
                        std::string command,
                        std::string cmddesc,
                        AdminSocketHook* hook,
                        std::string help);
*/

  // no single-command unregistration, as en-bulk per server unregistration is the pref method.
  // I will consider adding specific API for those cases where the client needs to disable
  // one specific service. It will be clearly named, to mark the fact that it would not replace
  // deregistration. Something like disable_command() 
  //seastar::future<> unregister_command(std::string_view command);

  /// unregister all hooks registered by this client
  seastar::future<> unregister_server(hook_server_tag  server_tag);

private:

  //seastar::future<bool> handle_registration(hook_server_tag  server_tag,
  //                                          std::string command,
  //                                          std::string cmddesc,
  //                                          AdminSocketHook* hook,
  //                                          std::string help);

  //seastar::future<> delayed_unregistration(std::string command);

  void internal_hooks();

  seastar::future<> init_async(const std::string& path);

  seastar::future<> handle_client(seastar::input_stream<char>&& inp, seastar::output_stream<char>&& out);

  seastar::future<> execute_line(std::string cmdline, seastar::output_stream<char>& out);

#if 0
  bool validate(const std::string& command,
		const cmdmap_t& cmdmap,
		ceph::buffer::list& out) const;
#endif
  
  CephContext* m_cct;
  bool do_die{false};  // RRR check if needed

  // seems like multiple Context objects are created when calling vstart, and that
  // translates to multiple AdminSocket objects being created. But only the "real" one
  // (the OSD's) is actually initialized by a call to 'init()'.
  // Thus, we will try to discourage the "other" objects from registering hooks.
  bool setup_done{false}; // RRR check if needed

  std::unique_ptr<AdminSocketHook> version_hook;
  std::unique_ptr<AdminSocketHook> help_hook;
  std::unique_ptr<AdminSocketHook> getdescs_hook;
  std::unique_ptr<AdminSocketHook> test_throw_hook;  // for dev unit-tests

  //inline static std::vector<AsokServiceDef> InternalHooks{
  //AsokServiceDef{"0","0",version_hook.get(),     ""

  //};

  struct hook_info {
    std::string cmd;
    bool is_valid{true}; //!< cleared with 'disable_command()'
    AdminSocketHook* hook;
    std::string desc;
    std::string help;
    hook_server_tag server_tag; //!< for when we un-register all client's requests en bulk

    //hook_info(hook_server_tag tag, AdminSocketHook* hook, std::string_view desc,
    //      std::string_view help)
    //  : hook{hook}, desc{desc}, help{help}, server_tag{tag} {}
    hook_info(std::string cmd, hook_server_tag tag, AdminSocketHook* hook, std::string_view desc,
          std::string_view help)
      : cmd{cmd}, hook{hook}, desc{desc}, help{help}, server_tag{tag} {}
  };

  struct parsed_command_t {
    std::string            m_cmd;
    cmdmap_t               m_parameters;
    std::string            m_format;
    const AdminSocketHook* m_hook;
    //AsokServiceDef*        m_hook;
    ::seastar::gate*       m_gate;

  };
  std::optional<parsed_command_t> parse_cmd(const std::string& command_text);

  //
  //  the original code uses std::map. As we wish to discard entries without erasing
  //  them, I'd rather use a container that supports key modifications. And as the number
  //  of entries is in the tens, not the thousands, I expect std::vector to perform
  //  better.
  //std::map<std::string, hook_info, std::less<>> hooks;
  std::vector<hook_info> hooks;

  struct server_block {
    //server_block(hook_server_tag tag, const std::vector<AsokServiceDef>& hooks)
    //  : m_server_id{tag}
    //  , m_hooks{hooks}
    //{}
    //server_block(server_block&& f) = default;
    //server_block(const server_block& f) = default;
    //hook_server_tag m_server_id;

    server_block(const std::vector<AsokServiceDef>& hooks)
      : m_hooks{hooks}
    {}
    const std::vector<AsokServiceDef>& m_hooks;
    ::seastar::gate m_gate;
  };

  // \todo cache all available commands, from all servers, in one vector.
  //  Recreate the list every register/unreg request.

  std::map<hook_server_tag, server_block> servers;

  //using GateAndHook = std::pair<::seastar::gate*, const AsokServiceDef*>;
  struct GateAndHook {
    //GateAndHook& operator=(GateAndHook&& f) {
    //  m_gate = std::move(f.m_gate);
    //  api = f.api;
    //  return *this;
    //}
    ::seastar::gate* m_gate;
    const AsokServiceDef* api;
  };

  /*!
    locate_command() will search all servers' control blocks. If found, the
    relevant gate is entered. Returns the AsokServiceDef, and the "activated" gate.
   */
  GateAndHook locate_command(std::string_view cmd);

  friend class AdminSocketTest;
  friend class HelpHook;
  friend class GetdescsHook;
};




