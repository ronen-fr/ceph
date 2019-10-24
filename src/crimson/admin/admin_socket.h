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

  Keeping existing interfaces whenever possible.
  Running on a single core:
  - the hooks database is only manipulated on that main core. Clients running on other cores
    dispatch the register/unregister requests to that main core.
  - incoming requests arriving on the admin socket are only received on that specific core. The actual
    operation is delegated to the relevant core if needed.
 */
#include <string>
#include <string_view>
#include <map>




#if 0
#include <condition_variable>
#include <mutex>

#include "include/buffer.h"
#endif

#include "seastar/core/future.hh"
#include "seastar/core/iostream.hh"

#include "common/cmdparse.h"

class AdminSocket;
class CephContext;

using namespace std::literals;

inline constexpr auto CEPH_ADMIN_SOCK_VERSION = "2"sv;

class AdminSocketHook {
public:
  virtual seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
		                     std::string_view format, ceph::buffer::list& out) = 0;
  virtual ~AdminSocketHook() {}
};

class AdminSocket {
public:
  AdminSocket(CephContext *cct);
  ~AdminSocket();

  AdminSocket(const AdminSocket&) = delete;
  AdminSocket& operator =(const AdminSocket&) = delete;
  AdminSocket(AdminSocket&&) = delete;
  AdminSocket& operator =(AdminSocket&&) = delete;

  using hook_client_tag = const void*;

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
   * @param command command string
   * @param cmddesc command syntax descriptor
   * @param hook implementation
   * @param help help text.  if empty, command will not be included in 'help' output.
   *
   * @return 'true' for success, 'false' if command already registered.
   */
  seastar::future<bool> register_promise(hook_client_tag  client_tag,
                                         std::string command,
		                         std::string cmddesc,
		                         AdminSocketHook *hook,
		                         std::string help);

  bool register_command(hook_client_tag  client_tag,
                        std::string command,
                        std::string cmddesc,
                        AdminSocketHook* hook,
                        std::string help);


  seastar::future<> unregister_command(std::string_view command);

  /// unregister all hooks registered by this client
  seastar::future<> unregister_client(hook_client_tag  client_tag);

private:

  seastar::future<bool> handle_registration(hook_client_tag  client_tag,
                                            std::string command,
			                    std::string cmddesc,
		                            AdminSocketHook* hook,
			                    std::string help);

  seastar::future<> delayed_unregistration(std::string command);

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

  struct hook_info {
    std::string cmd;
    bool is_valid{true}; //!< cleared with 'unregister_command()'
    AdminSocketHook* hook;
    std::string desc;
    std::string help;
    hook_client_tag client_tag; //!< for when we un-register all client's requests en bulk

    //hook_info(hook_client_tag tag, AdminSocketHook* hook, std::string_view desc,
    //      std::string_view help)
    //  : hook{hook}, desc{desc}, help{help}, client_tag{tag} {}
    hook_info(std::string cmd, hook_client_tag tag, AdminSocketHook* hook, std::string_view desc,
          std::string_view help)
      : cmd{cmd}, hook{hook}, desc{desc}, help{help}, client_tag{tag} {}
  };

  struct parsed_command_t {
    std::string            m_cmd;
    cmdmap_t               m_parameters;
    std::string            m_format;
    AdminSocketHook*       m_hook;
  };
  std::optional<parsed_command_t> parse_cmd(const std::string& command_text);

  //
  //  the original code uses std::map. As we wish to discard entries without erasing
  //  them, I'd rather use a container that supports key modifications. And as the number
  //  of entries is in the tens, not the thousands, I expect std::vector to perform
  //  better.
  //std::map<std::string, hook_info, std::less<>> hooks;
  std::vector<hook_info> hooks;

  friend class AdminSocketTest;
  friend class HelpHook;
  friend class GetdescsHook;
};




