// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
  A Crimson-wise version of the src/common/admin_socket.h

  Note: assumed to be running on a single core.
*/
#include <string>
#include <string_view>
#include <map>
#include "seastar/core/future.hh"
#include "seastar/core/shared_ptr.hh"
#include "seastar/core/shared_mutex.hh"
#include "seastar/core/gate.hh"
#include "seastar/core/iostream.hh"
#include "seastar/net/api.hh"
#include "common/cmdparse.h"

class CephContext;

using namespace std::literals;

inline constexpr auto CEPH_ADMIN_SOCK_VERSION = "2"sv;

namespace crimson::admin {

class AdminSocket;

/**
  A specific hook must implement exactly one of the two interfaces:
  (1) call(command, cmdmap, format, out)
  or
  (2) exec_command(formatter, command, cmdmap, format, out)

  The default implementation of (1) above calls exec_command() after handling
  most of the boiler-plate choirs:
  - setting up the formatter, with an appropiate 'section' already opened;
  - handling possible failures (exceptions or future_exceptions) returned by (2)
  - flushing the output to the outgoing bufferlist.
*/
class AdminSocketHook {
 public:
  /**
      \retval 'false' for hook execution errors
  */
  virtual seastar::future<bool> call(std::string_view command,
                                     const cmdmap_t&  cmdmap,
                                     std::string_view format,
                                     bufferlist&      out) const;

  virtual ~AdminSocketHook() {}

 protected:
  virtual seastar::future<> exec_command(ceph::Formatter* f,
                                         std::string_view command,
                                         const cmdmap_t&  cmdmap,
                                         std::string_view format,
                                         bufferlist&      out) const
  {
    return seastar::now();
  }

  /**
    (customization point) controlling whether the high-level section of the
    output is an array (affects the JSON formatting)
  */
  virtual bool format_as_array() const
  {
    return false;
  }

  /**
    customization point (as some commands expect non-standard response header)
  */
  virtual std::string section_name(std::string_view command) const
  {
    return std::string{ command };
  }
};

/**
  The details of a single API in a server's hooks block
*/
struct AsokServiceDef {
  const std::string command;  ///< the sequence of words that should be used
  const std::string cmddesc;  ///< the command syntax
  const AdminSocketHook* hook;
  const std::string      help;  ///< help message
};

class AdminHooksIter;  ///< an iterator over all APIs in all server blocks

/// a ref-count owner of the AdminSocket, used to guarantee its existence until
/// all server-blocks are unregistered
using AdminSocketRef = seastar::lw_shared_ptr<AdminSocket>;

using AsokRegistrationRes =
  std::optional<AdminSocketRef>;  // holding the server alive until after our
                                  // unregistration

class AdminSocket : public seastar::enable_lw_shared_from_this<AdminSocket> {
 public:
  AdminSocket(CephContext* cct);
  ~AdminSocket();

  AdminSocket(const AdminSocket&) = delete;
  AdminSocket& operator=(const AdminSocket&) = delete;
  AdminSocket(AdminSocket&&)                 = delete;
  AdminSocket& operator=(AdminSocket&&) = delete;

  using hook_server_tag = const void*;

  /**
     create the async Seastar thread that handles asok commands arriving
     over the socket.
  */
  seastar::future<> init(const std::string& path);

  seastar::future<> stop();

  /**
     register an admin socket hooks server

     The server registers a set of APIs under a common hook_server_tag.

     Commands (APIs) are registered under a command string. Incoming
     commands are split by spaces and matched against the longest
     registered command. For example, if 'foo' and 'foo bar' are
     registered, and an incoming command is 'foo bar baz', it is
     matched with 'foo bar', while 'foo fud' will match 'foo'.

     The entire incoming command string is passed to the registered
     hook.

     \param server_tag  a tag identifying the server registering the hook
     \param apis_served a vector of the commands served by this server. Each
            command registration includes its identifying command string, the
            expected call syntax, and some help text.
            A note re the help text: if empty, command will not be included in
            'help' output.

     \retval a shared ptr to the asok server itself, or nullopt if
             a block with same tag is already registered.
  */
  seastar::future<AsokRegistrationRes> register_server(
    hook_server_tag server_tag, const std::vector<AsokServiceDef>& apis_served);

  /**
     unregister all hooks registered by this hooks-server. The caller
     gives up on its shared-ownership of the asok server once the deregistration
     is complete.
  */
  seastar::future<> unregister_server(hook_server_tag  server_tag,
                                      AdminSocketRef&& server_ref);

 private:
  /**
     the result of analyzing an incoming command, and locating it in
     the registered APIs collection.
  */
  struct parsed_command_t {
    std::string            m_cmd;
    cmdmap_t               m_parameters;
    std::string            m_format;
    const AsokServiceDef*  m_api;
    const AdminSocketHook* m_hook;
    /**
        the length of the whole command-sequence under the 'prefix' header
     */
    std::size_t m_cmd_seq_len;
  };
  // and the shorthand:
  using Maybe_parsed = std::optional<AdminSocket::parsed_command_t>;

  /**
    server_registration() is called by register_server() after acquiring the
    table lock.
  */
  AsokRegistrationRes server_registration(
    hook_server_tag server_tag, const std::vector<AsokServiceDef>& apis_served);

  /**
    Registering the APIs that are served directly by the admin_socket server.
  */
  seastar::future<AsokRegistrationRes> internal_hooks();

  /**
     unregister all hooks registered by this hooks-server
   */
  seastar::future<> unregister_server(hook_server_tag server_tag);

  seastar::future<> handle_client(seastar::input_stream<char>&  inp,
                                  seastar::output_stream<char>& out);

  seastar::future<> execute_line(std::string                   cmdline,
                                 seastar::output_stream<char>& out);

  seastar::future<> finalyze_response(seastar::output_stream<char>& out,
                                      ceph::bufferlist&&            msgs);

  bool validate_command(const parsed_command_t& parsed,
                        const std::string&      command_text,
                        ceph::bufferlist&       out) const;

  CephContext* m_cct;

  /**
    Non-owning ptr to the UNIX-domain "server-socket".
    Named here to allow a call to abort_accept().
  */
  seastar::api_v2::server_socket* m_server_sock{ nullptr };

  /*
    stopping incoming ASOK requests at shutdown
  */
  seastar::gate arrivals_gate;

  std::unique_ptr<AdminSocketHook> version_hook;
  std::unique_ptr<AdminSocketHook> git_ver_hook;
  std::unique_ptr<AdminSocketHook> the0_hook;
  std::unique_ptr<AdminSocketHook> help_hook;
  std::unique_ptr<AdminSocketHook> getdescs_hook;
  std::unique_ptr<AdminSocketHook> test_throw_hook;  // for dev unit-tests

  /**
    parse the incoming command line into the sequence of words that identifies
    the API, and into its arguments. Locate the command string in the registered
    blocks.
  */
  Maybe_parsed parse_cmd(std::string command_text, bufferlist& out);

  struct server_block {
    server_block(const std::vector<AsokServiceDef>& hooks) : m_hooks{ hooks } {}
    const std::vector<AsokServiceDef>& m_hooks;
  };

  // \todo possible improvement: cache all available commands, from all servers,
  // in one vector.
  //  Recreate the list every register/unreg request.

  /**
    The servers table is protected by a rw-lock, to be acquired exclusively only
    when registering or removing a server.
    The lock is locked-shared when executing any hook.
   */
  seastar::shared_mutex                   servers_tbl_rwlock;
  std::map<hook_server_tag, server_block> servers;

  using maybe_service_def_t = std::optional<const AsokServiceDef*>;

  /**
    Find the longest subset of words in 'match' that is a registered API (in any
    of the servers' control blocks).
    locate_subcmd() is expected to be called with the servers table RW-lock
    held.
  */
  maybe_service_def_t locate_subcmd(std::string match) const;

 public:
  /**
    iterator support
   */
  AdminHooksIter begin();
  AdminHooksIter end();

  using ServersListIt = std::map<hook_server_tag, server_block>::iterator;
  using ServerApiIt   = std::vector<AsokServiceDef>::const_iterator;

  friend class AdminSocketTest;
  friend class HelpHook;
  friend class GetdescsHook;
  friend class AdminHooksIter;
};

/**
  An iterator over all registered APIs.
*/
struct AdminHooksIter
    : public std::iterator<std::output_iterator_tag, AsokServiceDef> {
 public:
  explicit AdminHooksIter(AdminSocket& master, bool end_flag = false);

  ~AdminHooksIter() = default;

  const AsokServiceDef* operator*() const
  {
    return &(*m_siter);
  }

  /**
    The (in)equality test is only used to compare to 'end'.
   */
  bool operator!=(const AdminHooksIter& other) const
  {
    return m_end_marker != other.m_end_marker;
  }

  AdminHooksIter& operator++();

 private:
  AdminSocket&               m_master;
  AdminSocket::ServersListIt m_miter;
  AdminSocket::ServerApiIt   m_siter;
  bool                       m_end_marker;

  friend class AdminSocket;
};

}  // namespace crimson::admin
