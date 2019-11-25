// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "common/version.h"
#include "crimson/net/Socket.h"
#include "crimson/admin/admin_socket.h"
#include "crimson/common/log.h"
//#include "seastar/testing/test_case.hh"
#include "seastar/net/api.hh"
#include "seastar/net/inet_address.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/thread.hh"
//#include "seastar/util/log.hh"
#include "seastar/util/std-compat.hh"
#include <boost/algorithm/string.hpp>

/*!
  A Crimson-wise version of the admin socket - implementation file

  \todo handle the unlinking of the admin socket. Note that 'cleanup_files' at-exit functionality is not yet
        implemented in Crimson
*/


namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

namespace asok_unit_testing {

  seastar::future<> utest_run_1(AdminSocket* asok);

}

/*!
  Hooks table - iterator support

  Note: Each server-block is locked (via its gate member) before
        iterating over its entries.
 */

AdminHooksIter AdminSocket::begin() {
  AdminHooksIter it{*this};
  it.m_miter->second.m_gate.enter();
  it.m_in_gate = true;
  return it;
}

AdminHooksIter AdminSocket::end() {
  AdminHooksIter it{*this, true};
  return it;
}

/*!
  note that 'end-flag' is an optional parameter, and is only used internally (to
  signal the end() state).
 */ 
AdminHooksIter::AdminHooksIter(AdminSocket& master, bool end_flag)
    : m_master{master}
{
  if (end_flag) {
    // create the equivalent of servers.end();
    m_miter = m_master.servers.end();
  } else {
    m_miter = m_master.servers.begin();
    m_siter = m_miter->second.m_hooks.begin();
  }
}

AdminHooksIter& AdminHooksIter::operator++() 
{
  ++m_siter;
  if (m_siter == m_miter->second.m_hooks.end()) {
    // move to the next server-block
    m_miter->second.m_gate.leave();
    m_in_gate = false;
    
    while (true) {
      m_miter++;
      if (m_miter == m_master.servers.end())
        return *this;

      try {
        m_miter->second.m_gate.enter(); // will throw if gate was already closed
        m_siter = m_miter->second.m_hooks.begin();
        m_in_gate = true;
        return *this;
      } catch (...) {
        // this server-block is being torn-down, and cannot be used
        continue;
      }
    }
  }
  return *this;
}

AdminHooksIter::~AdminHooksIter()
{
  //logger().info("{} {}", __func__, (m_in_gate?"+":"-")); 
  if (m_in_gate) {
    m_miter->second.m_gate.leave();
    logger().info("{}", __func__);
  } 
}

/*!
  the defaut implementation of the hook API

  Note that we never throw or return a failed future.
 */
seastar::future<bool> AdminSocketHook::call(std::string_view command, const cmdmap_t& cmdmap,
                                            std::string_view format, ceph::bufferlist& out) const
{
  unique_ptr<Formatter> f{Formatter::create(format, "json-pretty"sv, "json-pretty"s)};

  // customize the output section, as required by a couple of hook APIs:
  std::string section{section_name(command)};
  boost::replace_all(section, " ", "_");
  if (format_as_array()) {
    f->open_array_section(section.c_str());
  } else {
    f->open_object_section(section.c_str());
  }
  logger().info("{} cmd={} out section={}", __func__, command, section); 

  bool bres{false};
  /*!
      call the command-specific hook.
      A note re error handling:
        - will be modified to use the new 'erroretor'. For now:
        - exec_command() may throw or return an exceptional future. We return a message that starts
          with "error" on both failure scenarios.
   */
  return seastar::do_with(std::move(f), std::move(bres), [this, &command, &cmdmap, &format, &out](unique_ptr<Formatter>& ftr, bool& br) {

    return seastar::futurize_apply([this, &command, &cmdmap, &format, &out, f=ftr.get()] {
      return exec_command(f, command, cmdmap, format, out);
    }).
      // get us to a resolved state:
      then_wrapped([&ftr,&command,&br](seastar::future<> res) -> seastar::future<bool> {

      //  we now have a ready future (or a failure)
      if (res.failed()) {
        std::cerr << "exec throw" << std::endl;
        br = false;
      } else {
        br = true;
      }
      res.ignore_ready_future();
      return seastar::make_ready_future<bool>(br);
    }).then([this, &ftr, &out](auto res) -> seastar::future<bool> {
      ftr->close_section();
      ftr->enable_line_break();
      ftr->flush(out);
      return seastar::make_ready_future<bool>(res);
    });
  });
}


AdminSocket::AdminSocket(CephContext *cct)
  : m_cct(cct)
{
  std::cout << "######### a new AdminSocket " << (uint64_t)(this) <<
        " -> " << (uint64_t)(m_cct) << std::endl;
}

AdminSocket::~AdminSocket()
{
  std::cout << "######### XXXXX AdminSocket " << (uint64_t)(this) << std::endl;
  logger().warn("{}: {} {}", __func__, (int)getpid(), (uint64_t)(this));
}

/*!
  Note re context: running in the asok core. No need to lock the table. (RRR rethink this point)
  And no futurization until required to support multiple cores.
*/
AsokRegistrationRes AdminSocket::server_registration(AdminSocket::hook_server_tag  server_tag,
                                 const std::vector<AsokServiceDef>& apis_served)
{
  auto ne = servers.try_emplace(
                                server_tag,
                                apis_served);

  //  is this server tag already registered?
  if (!ne.second) {
    return std::nullopt;
  }

  logger().warn("{}: {} server registration (tag: {})", __func__, (int)getpid(), (uint64_t)(server_tag));
  logger().warn("{}: {} server registration (tbl size: {})", __func__, (int)getpid(), servers.size());
  return this->shared_from_this();
}

seastar::future<AsokRegistrationRes>
AdminSocket::register_server(hook_server_tag  server_tag, const std::vector<AsokServiceDef>& apis_served)
{
  return seastar::with_lock(servers_tbl_rwlock, [this, server_tag, &apis_served]() {
    return server_registration(server_tag, apis_served);
  });
}


/*!
  Called by the server implementing the hook. The gate will be closed, and the function
  will block until all execution of commands within the gate are done.
 */
seastar::future<> AdminSocket::unregister_server(hook_server_tag server_tag)
{
  logger().debug("{}: {} server un-reg (tag: {}) ({} prev cnt: {})",
                __func__, (int)getpid(), (uint64_t)(server_tag), (uint64_t)(this), servers.size());

  //  locate the server registration
  return seastar::with_shared(servers_tbl_rwlock, [this, server_tag]() {

    auto srv_itr = servers.find(server_tag);
    return srv_itr;

  }).then([this, server_tag](auto srv_itr) {

    if (srv_itr == servers.end()) {
      logger().warn("{}: unregistering a non-existing registration (tag: {})", __func__, (uint64_t)(server_tag));
      // list what servers *are* there
      for (auto& [k, d] : servers) {
        logger().debug("---> server: {} {}", (uint64_t)(k), d.m_hooks.front().command);
      }
      return seastar::now();

    } else {

      // note: usually we would have liked to maintain the shared lock, and just upgrade it here
      // to an exclusive one. But there is no chance of anyone else removing our specific server-block
      // before we re-acquire the lock

      return (srv_itr->second.m_gate.close()).then([this, srv_itr]() {
        return with_lock(servers_tbl_rwlock, [this, srv_itr]() {
          servers.erase(srv_itr);
          return seastar::now();
        });
      });
    }
  }).finally([server_tag]() {
    logger().info("{}: done removing server block {}", __func__, (uint64_t)(server_tag));
  });
}

seastar::future<> AdminSocket::unregister_server(hook_server_tag server_tag, AdminSocketRef&& server_ref)
{
   // reducing the ref-count on us (the asok server) by discarding server_ref:
   return seastar::do_with(std::move(server_ref), [this, server_tag](auto& srv) {
     return unregister_server(server_tag).finally([this,server_tag,&srv]() {
       logger().warn("{} - {}", (uint64_t)(server_tag), srv->servers.size());
       //return seastar::now();
     });
   });
}

seastar::future<AdminSocket::GateAndHook> AdminSocket::locate_command(const std::string_view cmd)
{
  return seastar::with_shared(servers_tbl_rwlock, [this, cmd]() {

    for (auto& [tag, srv] : servers) {

      // "lock" the server's control block before searching for the command string
      try {
        srv.m_gate.enter();
      } catch (...) {
        // probable error: gate is already closed
        continue;
      }

      for (auto& api : srv.m_hooks) {
        if (api.command == cmd) {
          logger().info("{}: located {} w/ server {}", __func__, cmd, tag);
          // note that the gate was entered!
          return AdminSocket::GateAndHook{&srv.m_gate, &api};
        }
      }

      // not registered by this server. Close this server's gate.
      srv.m_gate.leave();
    }

    return AdminSocket::GateAndHook{nullptr, nullptr};
  });
}

/*!
    the incoming command text, which is in JSON or XML, is parsed down
    to its separate op-codes and arguments. The hook registered to handle the
    specific command is located.
  */
std::optional<AdminSocket::parsed_command_t> AdminSocket::parse_cmd(const std::string command_text)
{
  cmdmap_t cmdmap;
  vector<string> cmdvec;
  stringstream errss;

  //  note that cmdmap_from_json() may throw on syntax issues
  cmdvec.push_back(command_text); // as cmdmap_from_json() likes the input in this format
  try {
    if (!cmdmap_from_json(cmdvec, &cmdmap, errss)) {
      logger().error("{}: incoming command error: {}", __func__, errss.str()); // RRR verify errrss
      return std::nullopt;
    }
  } catch ( ... ) {
    logger().error("{}: incoming command syntax: {}", __func__, command_text);
    return std::nullopt;
  }

  string format;
  string match;
  std::size_t full_command_seq;
  try {
    cmd_getval(m_cct, cmdmap, "format", format);
    cmd_getval(m_cct, cmdmap, "prefix", match);
    full_command_seq = match.length(); // the full sequence, before we start chipping away the end
  } catch (const bad_cmd_get& e) {
    return std::nullopt;
  }
  if (format != "json"s && format != "json-pretty"s &&
      format != "xml"s && format != "xml-pretty"s)
    format = "json-pretty"s;

  // try to match the longest set of strings. Failing - remove the tail part and retry.
  AdminSocket::GateAndHook gh{nullptr, nullptr};
  while (match.size()) {
    try {
      std::cerr << "before get0" << std::endl;
      seastar::future<AdminSocket::GateAndHook> future_gh = locate_command(match);
      future_gh.wait();
      // future_gh is available, but maybe an exceptional one
      if (!future_gh.failed()) {
        gh = future_gh.get0();
        if (gh.api)
          break;
      }

    } catch ( ... ) {
      std::cerr << "failed get0" << std::endl;
       // nothing to do
    }

    // drop right-most word
    size_t pos = match.rfind(' ');
    if (pos == std::string::npos) {
      match.clear();  // we fail
      break;
    } else {
      match.resize(pos);
    }
  }

  if (!gh.api) {
    logger().error("{}: unknown command: {}", __func__, command_text);
    return std::nullopt;
  }

  return parsed_command_t{match, cmdmap, format, gh.api, gh.api->hook, gh.m_gate, full_command_seq};
}

bool AdminSocket::validate_command(const parsed_command_t& parsed,
                                   const std::string& command_text,
                                   ceph::buffer::list& out) const
{
  // did we receive any arguments apart from the command word?
  logger().warn("ct:{} {} origl:{} pmc:{} pmcl:{}", command_text, command_text.length(), parsed.m_cmd_seq_len, parsed.m_cmd, parsed.m_cmd.length());

  if (parsed.m_cmd_seq_len == parsed.m_cmd.length())
    return true;

  logger().warn("{}: in:{} against:{}", __func__, command_text, parsed.m_api->cmddesc);

  stringstream os;
  try {
    // validate throws on some syntax errors
    if (validate_cmd(m_cct, parsed.m_api->cmddesc, parsed.m_parameters, os)) {
      return true;
    }
  } catch( std::exception& e ) {
    logger().error("{}: validation failure ({} : {}) {}", __func__, command_text, parsed.m_cmd, e.what());
  }

  logger().error("{}: (incoming:{}) {}", __func__, command_text, os.str());
  out.append(os);
  return false;
}

bool AdminSocket::validate_command(const parsed_command_t& parsed,
                                   const std::string& command_text,
                                   std::stringstream& outs) const
{
  // did we receive any arguments apart from the command word?
  logger().warn("ct:{} {} origl:{} pmc:{} pmcl:{}", command_text, command_text.length(), parsed.m_cmd_seq_len, parsed.m_cmd, parsed.m_cmd.length());

  if (parsed.m_cmd_seq_len == parsed.m_cmd.length())
    return true;

  logger().warn("{}: in:{} against:{}", __func__, command_text, parsed.m_api->cmddesc);

  try {
    // validate throws on some syntax errors
    if (validate_cmd(m_cct, parsed.m_api->cmddesc, parsed.m_parameters, outs)) {
      return true;
    }
  } catch( std::exception& e ) {
    logger().error("{}: validation failure ({} : {}) {}", __func__, command_text, parsed.m_cmd, e.what());
  }

  logger().error("{}: (incoming:{}) {}", __func__, command_text, outs.str());
  return false;
}

seastar::future<> AdminSocket::execute_line(std::string cmdline, seastar::output_stream<char>& out)
{
  //  find the longest word-sequence for which we have a registered hook to handle
  auto parsed = parse_cmd(cmdline);
  if (!parsed) {
    logger().error("{}: no command in ({})", __func__, cmdline);
    return out.write("command syntax error");
  }
  std::cerr << "eline " << cmdline << std::endl;

  //return seastar::do_with(std::move(parsed), ::ceph::bufferlist(), bool{false},
  //                        [&out, cmdline, this, gatep = parsed->m_gate] (auto& parsed, auto&& out_buf, auto&& dbg_gate_exit) {}
  return seastar::do_with(std::move(parsed), ::ceph::bufferlist(), std::stringstream(),
                          [&out, cmdline, this, gatep = parsed->m_gate] (auto& parsed, auto& out_buf, auto&& out_stream) {

    ceph_assert_always(parsed->m_gate->get_count() > 0); // the server-block containing the identified command should have been locked

    std::cerr << "eline2 " << cmdline << std::endl;
    return (validate_command(*parsed, cmdline, out_stream) ?
              //  (dbg_gate_exit = true), // marking the fact that we must close this gate
              parsed->m_hook->call(parsed->m_cmd, parsed->m_parameters, (*parsed).m_format, out_buf) :
              seastar::make_ready_future<bool>(false) /* failed args syntax validation */
      ).
      then_wrapped([&out, out_buf, gatep, &out_stream](auto fut) {
        std::cerr << "b4 gate "  << std::endl;
        std::cerr << "b4 gate g "  << gatep->get_count() << std::endl;
        std::cerr << "b4 gate "  << out_stream.str() << std::endl;
        gatep->leave();
        std::cerr << "af gate "  << out_stream.str() << std::endl;

        if (fut.failed()) {
          // add 'failed' to the contents of out_buf? not what happens in the old code
          std::cerr << "elinef "  << out_stream.str() << std::endl;

          return seastar::make_ready_future<bool>(false);
        } else {
          return fut;
        }
      }).
      then([&out, out_buf, gatep, &out_stream](auto call_res) {
        string outbuf_cont = out_buf.to_str();
        uint32_t response_length = htonl(outbuf_cont.length());
        logger().info("asok response length: {}", outbuf_cont.length());
        std::cerr << "repln " <<  outbuf_cont.length() << std::endl;

        return out.write((char*)&response_length, sizeof(uint32_t)).then([&out, outbuf_cont]() {
          std::cerr << "fin " <<  outbuf_cont.length() << std::endl;
          if (outbuf_cont.empty())
            return out.write("xxxx");
          else
            return out.write(outbuf_cont.c_str());
        });
      });
  });
}

seastar::future<> AdminSocket::handle_client(seastar::input_stream<char>& inp, seastar::output_stream<char>& out)
{
  //  RRR \todo safe read
  //  RRR \todo handle old protocol (see original code) - is still needed?

  return inp.read().
    then( [&out, this](auto full_cmd) {

      seastar::sstring cmd_line{full_cmd.begin(), full_cmd.end()};
      logger().debug("{}: {}\n", __func__, cmd_line);
      return execute_line(cmd_line, out);

    }).then([&out]() { return out.flush(); }).
    then([&out]() { return out.close(); }).
    then([&inp]() { 
            logger().debug("{}: cn--", __func__);
            return inp.close();
    }).handle_exception([](auto ep) {
      logger().error("exception on {}: {}", __func__, ep);
      return seastar::make_ready_future<>();
    }).discard_result();
}

#if 0
seastar::future<> AdminSocket::handle_client(seastar::input_stream<char>& inp, seastar::output_stream<char>& out)
{
  //  RRR \todo safe read
  //  RRR \todo handle old protocol (see original code) - is still needed?

  seastar::future<> client_fut = inp.read().
    then( [&out, this](auto full_cmd) {

      seastar::sstring cmd_line{full_cmd.begin(), full_cmd.end()};
      logger().debug("{}: {}\n", __func__, cmd_line);
      return execute_line(cmd_line, out);

    }).then([&out]() { return out.flush(); }).
    then([&out]() { return out.close(); }).
    then([&inp]() { 
            logger().debug("{}: cn--", __func__);
            return inp.close();
    }).discard_result();

  //client_fut.wait();
  return client_fut;
}
#endif

seastar::future<> AdminSocket::init(const std::string& path)
{
  //std::cout << "AdminSocket::init() w " << path << " owner: " << (uint64_t)(this) <<
  //      " -> " << (uint64_t)(m_cct) << std::endl;

  std::ignore = seastar::async([this]() {
     asok_unit_testing::utest_run_1(this).wait();
  });

  return seastar::async([this, path] {
    auto serverfut = init_async(path);
  }); 
}


seastar::future<> AdminSocket::init_async(const std::string& path)
{
  //  verify we are the only instance running now RRR

  internal_hooks().get(); // we are executing in an async thread, thus OK to wait()

  logger().debug("{}: path={}", __func__, path);
  auto sock_path = seastar::socket_address{seastar::unix_domain_addr{path}};

  return seastar::do_with(seastar::engine().listen(sock_path), [this](seastar::server_socket& lstn) {

    return seastar::do_until([this](){ return do_die; }, [&lstn,this]() {
      return lstn.accept().
        then([this](seastar::accept_result from_accept) {

          seastar::connected_socket cn    = std::move(from_accept.connection);
          logger().debug("{}: cn++", __func__);

          // can't count on order of evaluting the move(), and need to keep both the 'cn' and its streams alive
          //return do_with(std::move(cn), [this](auto& cn) { 
            return do_with(std::move(cn.input()), std::move(cn.output()), std::move(cn), [this](auto& inp, auto& out, auto& cn) {

              return handle_client(inp, out);
	        //then([]() { return seastar::make_ready_future<>(); });
	    }); //.then([]() { return seastar::make_ready_future<>(); });
        //});
      });
    });
  });
}

// ///////////////////////////////////////
// the internal hooks
// ///////////////////////////////////////

/*!
    The response to the '0' command is not formatted, neither JSON or otherwise.
*/
class The0Hook : public AdminSocketHook {
public:
  seastar::future<bool> call(std::string_view command, const cmdmap_t& ,
	    std::string_view format, bufferlist& out) const override {
    out.append(CEPH_ADMIN_SOCK_VERSION);
    return seastar::make_ready_future<bool>(true);
  }
};

class VersionHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit VersionHook(AdminSocket* as) : m_as{as} {}

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final
  {
    f->dump_string("version", ceph_version_to_str());
    f->dump_string("release", ceph_release_to_str());
    f->dump_string("release_type", ceph_release_type());
    return seastar::now();
  }
};

/*!
  Note that the git_version command is expected to return a 'version' JSON segment.
*/
class GitVersionHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit GitVersionHook(AdminSocket* as) : m_as{as} {}

  std::string section_name(std::string_view command) const final {
    return "version"s;
  }

  /*seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final
  {
    f->dump_string("git_version", git_version_to_str());
    return seastar::now();
  }*/

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final
  {
    return seastar::sleep(5s).then([this, f]() {
      f->dump_string("git_version", git_version_to_str());
      return seastar::now();
    });
  }
};

class HelpHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit HelpHook(AdminSocket* as) : m_as{as} {}

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final {

    for (const auto& hk_info : *m_as) {
      //logger().warn("debug {} loop", __func__);
      if (hk_info->help.length())
	f->dump_string(hk_info->command.c_str(), hk_info->help);
    }
    return seastar::now();
  }
};

class GetdescsHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit GetdescsHook(AdminSocket *as) : m_as{as}
  {}

  std::string section_name(std::string_view command) const final {
    return "command_descriptions"s;
  }

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
                                 std::string_view format, bufferlist& out) const final
  {
    int cmdnum = 0; 

    for (const auto& hk_info : *m_as) {
      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(f,
                                CEPH_FEATURES_ALL,
                                secname.str().c_str(),
                                hk_info->cmddesc,
                                hk_info->help);
      cmdnum++;
    }
    return seastar::now();
  }
};

class TestThrowHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit TestThrowHook(AdminSocket* as) : m_as{as} {}

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final {
    if (command == "fthrowAs")
      return seastar::make_exception_future<>(std::system_error{1, std::system_category()});

    return seastar::sleep(3s).then([this]() {
      throw(std::invalid_argument("As::TestThrowHook"));
      return seastar::now();
    });
  }
};

/// the hooks that are served directly by the admin_socket server
seastar::future<AsokRegistrationRes> AdminSocket::internal_hooks()
{
  version_hook = std::make_unique<VersionHook>(this);
  git_ver_hook = std::make_unique<GitVersionHook>(this);
  the0_hook = std::make_unique<The0Hook>();
  help_hook = std::make_unique<HelpHook>(this);
  getdescs_hook = std::make_unique<GetdescsHook>(this);
  test_throw_hook = std::make_unique<TestThrowHook>(this);

  static const std::vector<AsokServiceDef> internal_hooks_tbl{
      AsokServiceDef{"0",            "0",                    the0_hook.get(),        ""}
    , AsokServiceDef{"version",      "version",              version_hook.get(),     "get ceph version"}
    , AsokServiceDef{"git_version",  "git_version",          git_ver_hook.get(),     "get git sha1"}
    , AsokServiceDef{"help",         "help",                 help_hook.get(),        "list available commands"}
    , AsokServiceDef{"get_command_descriptions", "get_command_descriptions",
                                                             getdescs_hook.get(),    "list available commands"}
    , AsokServiceDef{"throwAs",      "throwAs",              test_throw_hook.get(),  ""}   // dev
    , AsokServiceDef{"fthrowAs",     "fthrowAs",             test_throw_hook.get(),  ""}   // dev
  };

  // server_registration() returns a shared pointer to the AdminSocket server, i.e. to us. As we
  // already have shared ownership of this object, we do not need it. RRR verify
  return register_server(AdminSocket::hook_server_tag{this}, internal_hooks_tbl);
  //std::ignore = server_registration(AdminSocket::hook_server_tag{this}, internal_hooks_tbl);
}


// ///////////////////////////////////////////
// unit-testing servers-block map manipulation
// ///////////////////////////////////////////

/*
  run multiple seastar threads that register/remove/search server blocks, testing
  the locks that protect them.
*/

#include <random>
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace seastar;
//using AdminSocket::hook_server_tag;

namespace asok_unit_testing {

class UTestHook : public AdminSocketHook {
public:
  explicit UTestHook() {}

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final
  {
    return seastar::now();
  }
};

static const UTestHook test_hook;

static const std::vector<AsokServiceDef> test_hooks{
      AsokServiceDef{"3",            "3",                    &test_hook,        "3"}
    , AsokServiceDef{"4",            "4",                    &test_hook,        "4"}
    , AsokServiceDef{"5",            "5",                    &test_hook,        "5"}
    , AsokServiceDef{"6",            "6",                    &test_hook,        "6"}
    , AsokServiceDef{"7",            "7",                    &test_hook,        "7"}
};


static const std::vector<AsokServiceDef> test_hooks10{
      AsokServiceDef{"13",            "13",                    &test_hook,        "13"}
    , AsokServiceDef{"14",            "14",                    &test_hook,        "14"}
    , AsokServiceDef{"15",            "15",                    &test_hook,        "15"}
    , AsokServiceDef{"16",            "16",                    &test_hook,        "16"}
    , AsokServiceDef{"17",            "17",                    &test_hook,        "17"}
};


struct test_reg_st {

  AdminSocket*                       asok;
  AdminSocket::hook_server_tag       tag;
  milliseconds                       period;
  gate&                              g;
  const std::vector<AsokServiceDef>& hks;
  std::random_device                 rd;
  std::default_random_engine         generator{rd()};
  AdminSocketRef                     current_reg; // holds the shared ownership

  test_reg_st(AdminSocket* ask, AdminSocket::hook_server_tag tag, milliseconds period, gate& gt, const std::vector<AsokServiceDef>& hks) :
    asok{ask},
    tag{tag},
    period{period},
    g{gt},
    hks{hks}
  {
    // start an async thread to reg/unreg
    std::ignore = seastar::async([this, hks] {
      auto loop_fut = loop(hks);
    });
  }

  // sleep, register, or fail w/ exception
  seastar::future<> delayed_reg(milliseconds dly) {
    return seastar::sleep(dly).
      then([this, dly]() {

        return with_gate(g, [this, dly]() {
          return asok->register_server(tag, hks);
        }).then([this](AsokRegistrationRes r) {
          current_reg = *r;
          return seastar::now();
        });
      });
  }

  // sleep, un-register, or fail w/ exception
  seastar::future<> delayed_unreg(milliseconds dly) {
    return seastar::sleep(dly).
      then([this]() {

        //return with_gate(g, [this]() {
          ceph::get_logger(ceph_subsys_osd).warn("{}", (uint64_t)(tag));
          return asok->unregister_server(tag, std::move(current_reg));
        //});
      });
  }

  seastar::future<> loop(const std::vector<AsokServiceDef>& hks) {
    std::uniform_int_distribution<int> dist(80, 120);
    auto act_period = milliseconds{period.count() * dist(generator) / 100};

    ceph::get_logger(ceph_subsys_osd).warn("{} starting", (uint64_t)(tag));
    return seastar::keep_doing([this, act_period, hks]() {

      return delayed_reg(act_period).then([this, act_period]() {
        return delayed_unreg(act_period);
      });
    }).handle_exception([this](std::exception_ptr eptr) {
      return seastar::now();
    }).finally([this]() {
      ceph::get_logger(ceph_subsys_osd).warn("{} done", (uint64_t)(tag));
      return seastar::now();
    });
  }
};

struct test_lookup_st {

  AdminSocket*                       asok;
  string                             cmd;
  milliseconds                       period;
  milliseconds                       delaying_inside;
  gate&                              g;
  std::random_device                 rd;
  std::default_random_engine         generator{rd()};

  test_lookup_st(AdminSocket* ask, string cmd, milliseconds period, milliseconds delaying_inside, gate& gt) :
    asok{ask},
    cmd{cmd},
    period{period},
    delaying_inside{delaying_inside},
    g{gt}
  {
    // start an async thread to reg/unreg
    std::ignore = seastar::async([this] {
      auto loop_fut = loop();
    });
  }

  void do_sleep(milliseconds m) {
    seastar::future<> f = seastar::sleep(m);
    f.wait();
    if (!f.failed()) {
      f.get();
    }
  }

  seastar::future<> delayed_lookup_aux(milliseconds dly_inside) {
    //
    //  look for the command, waste some time, then free the server-block
    //

    bool fnd{false};

    if (1) for (const auto& hk : *asok) {

      if (hk->command == cmd) {
        fnd = true;
        ceph::get_logger(ceph_subsys_osd).info("{}: utest located {}", __func__, cmd);
        seastar::sleep(dly_inside).get();
        break;
      }
    }

    if (!fnd)
      ceph::get_logger(ceph_subsys_osd).info("{}: utest nf {}", __func__, cmd);

    return seastar::sleep(500ms/*dly_inside*/).
      then([this]() {
        return seastar::now();
      });
  }

  // sleep, then lookup (or fail when gate closes)
  seastar::future<> delayed_lookup(milliseconds dly, milliseconds dly_inside) {
    return seastar::sleep(dly).
      then([this, dly_inside]() {

        //if (g.is_closed())
        //  return seastar::now();

        return with_gate(g, [this, dly_inside]() {
          return delayed_lookup_aux(dly_inside);
        });
      });
  }

  future<> delayed_lookup_2(milliseconds dly, milliseconds dly_inside) {
    return seastar::sleep(dly).
      then([this, dly_inside]() {
        return with_gate(g, [this, dly_inside]() {
          return delayed_lookup_aux(dly_inside);
        });
      }); //.get();
  }

  seastar::future<> loop() {
    std::uniform_int_distribution<int> dist(80, 120);
    auto act_period = period; //milliseconds{period.count() * dist(generator) / 100};
    return seastar::make_ready_future();
  }
  #if 0
    ceph::get_logger(ceph_subsys_osd).warn("lkup {} starting", period.count()/1000.0);

    return seastar::keep_doing([this, act_period] {

      return seastar::sleep(act_period).
        then([this, act_period]() {
          //return futurize_apply([this, act_period]() {
             delayed_lookup_2(act_period, delaying_inside).get();
             return seastar::make_ready_future();
          //});
        });
    }).then_wrapped([this](auto &r) -> seastar::future<> {
      return seastar::make_ready_future();
    });

  }
  #endif

#if 0
  seastar::future<> loop() {
    std::uniform_int_distribution<int> dist(80, 120);
    auto act_period = period; //milliseconds{period.count() * dist(generator) / 100};

    ceph::get_logger(ceph_subsys_osd).warn("lkup {} starting", period.count()/1000.0);

    return seastar::keep_doing([this, act_period]() {

      auto ft =  [this, act_period]() -> seastar::future<> {
                   return seastar::sleep(act_period).
                   then([this, act_period]() {
                     return delayed_lookup(act_period, delaying_inside);
                   })
                 ;};
      

      //return ft().wait();
      try {
        ft().wait();
        return seastar::now();
      } catch( std::exception& e ) {
        return seastar::make_exception_future<>(e);
      }


      //return seastar::now();

    }).handle_exception([this](std::exception_ptr eptr) {
      return seastar::now();
    }).finally([this]() {
      ceph::get_logger(ceph_subsys_osd).warn("lkup {} {} done", period.count()/100.0, cmd);
      return seastar::now();
    });
  }
  #endif

#if 0
  seastar::future<> loop() {
    std::uniform_int_distribution<int> dist(80, 120);
    auto act_period = milliseconds{period.count() * dist(generator) / 100};

    return seastar::repeat([this, act_period]() {

      return with_gate(g, [this, act_period]() -> seastar::stop_iteration {

        //
        //  look for the command, waste some time, then free the server-block
        //

        for (const auto& hk : *asok) {

          if (hk->command == cmd) {
            //logger().info("{}: utest located {}", __func__, cmd);
            seastar::sleep(act_period).get0();
            break;
          }
        }
        seastar::sleep(act_period).get0();
	return seastar::stop_iteration::no;
      });
    });
    return seastar::now();
  }
  #endif

};

seastar::future<> utest_run_1(AdminSocket* asok)
{
  const seconds run_time{2};
  seastar::gate gt;
 
  return seastar::do_with(std::move(gt), [run_time, asok](auto& gt) {

    test_reg_st* ta1 = new test_reg_st{asok, AdminSocket::hook_server_tag{(void*)(0x17)}, 200ms, gt, test_hooks  };
    test_reg_st ta2{asok, AdminSocket::hook_server_tag{(void*)(0x27)}, 150ms, gt, test_hooks10};

    //test_lookup_st* tlk1 = new test_lookup_st{asok, "0",  255ms, 95ms, gt};
    //test_lookup_st tlk2{asok, "16", 210ms, 90ms, gt};

    return seastar::sleep(run_time).
     then([&gt]() { return gt.close();}).
     then([&gt](){ return seastar::sleep(2000ms); }).
     then([ta1, &ta2/*, &tlk1*/]() { 
       delete ta1;
       //delete tlk1;
       return seastar::now();
     });
  }).then_wrapped([](auto&& x) {
    try {
      x.get();
      return seastar::now();
    } catch (... ) {
      return seastar::now();
    }
  });
}

}
