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

//using outstream_t = seastar::output_stream<char>;

class AdminSocketHook {
public:
  virtual bool call(std::string_view command, const cmdmap_t& cmdmap,
		    std::string_view format, ceph::buffer::list& out) = 0;
  virtual ~AdminSocketHook() {}
};



class AdminSocket
{
public:
  AdminSocket(CephContext *cct);
  ~AdminSocket();

  AdminSocket(const AdminSocket&) = delete;
  AdminSocket& operator =(const AdminSocket&) = delete;
  AdminSocket(AdminSocket&&) = delete;
  AdminSocket& operator =(AdminSocket&&) = delete;

  seastar::future<> init(const std::string& path);

  /**
   * register an admin socket command
   *
   * The command is registered under a command string.  Incoming
   * commands are split by space and matched against the longest
   * registered command.  For example, if 'foo' and 'foo bar' are
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
   * @return 0 for success, -EEXIST if command already registered.
   */
  int register_command(std::string_view command,
		       std::string_view cmddesc,
		       AdminSocketHook *hook,
		       std::string_view help);

  seastar::future<int> unregister_command(std::string_view command);

private:

  int handle_registeration(std::string_view command,
			   std::string_view cmddesc,
		           AdminSocketHook* hook,
			   std::string_view help);


  seastar::future<int> delayed_unregistration(std::string command);



  void internal_hooks();

  seastar::future<> init_async(const std::string& path);

  seastar::future<> handle_client(seastar::input_stream<char>&& inp, seastar::output_stream<char>&& out);

  seastar::future<seastar::stop_iteration> execute_line(std::string cmdline, seastar::output_stream<char>& out);

#if 0

  void shutdown();

  std::string create_shutdown_pipe(int *pipe_rd, int *pipe_wr);
  std::string destroy_shutdown_pipe();
  std::string bind_and_listen(const std::string &sock_path, int *fd);

  std::thread th;
  void entry() noexcept;
  bool do_accept();
  bool validate(const std::string& command,
		const cmdmap_t& cmdmap,
		ceph::buffer::list& out) const;
#endif
  
  CephContext *m_cct;
  bool do_die{false};

#if 0

  std::string m_path;
  int m_sock_fd = -1;
  int m_shutdown_rd_fd = -1;
  int m_shutdown_wr_fd = -1;

  bool in_hook = false;
  std::condition_variable in_hook_cond;
  std::mutex lock;  // protects `hooks`
#endif
  std::unique_ptr<AdminSocketHook> version_hook;
  std::unique_ptr<AdminSocketHook> help_hook;
  std::unique_ptr<AdminSocketHook> getdescs_hook;

  struct hook_info {
    AdminSocketHook* hook;
    std::string desc;
    std::string help;
    bool is_valid{true}; //!< cleared with 'unregister_command()'

    hook_info(AdminSocketHook* hook, std::string_view desc,
	      std::string_view help)
      : hook(hook), desc(desc), help(help) {}
  };

  struct parsed_command_t {
    std::string            m_cmd;
    cmdmap_t               m_parameters;
    std::string            m_format;
    AdminSocketHook*       m_hook;
  };
  std::optional<parsed_command_t> parse_cmd(const std::string& command_text);

  std::map<std::string, hook_info, std::less<>> hooks;

  friend class AdminSocketTest;
  friend class HelpHook;
  friend class GetdescsHook;
};




