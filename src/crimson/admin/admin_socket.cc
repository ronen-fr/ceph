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


namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}

  /*!
    iterator support
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
  the defaut implementation of the hook API

  Note that we never throw or return a failed future.
 */
seastar::future<bool> AdminSocketHook::call(std::string_view command, const cmdmap_t& cmdmap,
                                            std::string_view format, ceph::bufferlist& out) const
{
  unique_ptr<Formatter> f{Formatter::create(format, "json-pretty"sv, "json-pretty"s)};
  std::string section(command);
  boost::replace_all(section, " ", "_"); //!< \todo consider saving the '_' version of the command upon registration
  if (format_as_array()) {
    f->open_array_section(section.c_str());
  } else {
    f->open_object_section(section.c_str());
  }

  /*!
      call the command-specific hook.
      A note re error handling:
    	- will be modified to use the new 'erroretor'. For now:
    	- exec_command() may throw or return an exceptional future. We return a message starting
    	  with "error" on both failure scenarios.
   */
  return seastar::do_with(std::move(f), [this, &command, &cmdmap, &format, &out](unique_ptr<Formatter>& ftr) {
    return seastar::futurize_apply([this, &command, &cmdmap, &format, &out, f=ftr.get()] {
      return exec_command(f, command, cmdmap, format, out);
    //}).handle_exception([this](auto eptr) {
    //  // if something failed
    //  std::cerr << "osd_admin::exec:inhex1" << std::endl;
    //  return seastar::make_ready_future<bool>(false);
    }).then_wrapped([&ftr,&command](seastar::future<> res) -> seastar::future<bool> {
      try {
        if (res.failed()) {
          std::cerr << "osd_admin::exec:res failed" << std::endl;
          ftr->dump_string("res_failed", std::string(command) + " failed");
          return res.handle_exception([](auto eptr) {
            std::cerr << "osd_admin::exec:inhex1" << std::endl;
            return seastar::now();
          }).then_wrapped([](seastar::future<> ){ return seastar::make_ready_future<bool>(false); });
        } else {
          //(void)res.get();
          return seastar::make_ready_future<bool>(true);
        }
      } catch ( std::exception& ex ) {
        ftr->dump_string("error", std::string(command) + " failed with " + ex.what());
        std::cerr << "osd_admin::exec:immediate exc8" << std::endl;
        return seastar::make_ready_future<bool>(false);
      } catch ( ... ) {
        ftr->dump_string("error", std::string(command) + " failed with XX");
        std::cerr << "osd_admin::exec:immediate exc2" << std::endl;
        return seastar::make_ready_future<bool>(false);
      }
    }).handle_exception([this](auto eptr) {
      // if something failed
      std::cerr << "osd_admin::exec:inhexneeded???" << std::endl;
      return seastar::make_ready_future<bool>(false);
    }).then([this, &ftr, &out](auto res) -> seastar::future<bool> {
      ftr->close_section();
      ftr->enable_line_break();
      ftr->flush(out);
      return seastar::make_ready_future<bool>(res); //seastar::make_ready_future<bool>(true);
    });
  });
}

AdminSocket::AdminSocket(CephContext *cct)
  : m_cct(cct)
{
  //hooks.reserve(32); // \todo to be made into a constant
  std::cout << "######### a new AdminSocket " << (uint64_t)(this) <<
        " -> " << (uint64_t)(m_cct) << std::endl;
}

AdminSocket::~AdminSocket()
{
  //shutdown();
  std::cout << "######### XXXXX AdminSocket " << (uint64_t)(this) << std::endl;
}

/*!
  Note re context: running in asok core. No need to lock the table.
*/
//seastar::future<AsokRegistrationRes> // to futurize only if spanning cores
AsokRegistrationRes
AdminSocket::server_registration(AdminSocket::hook_server_tag  server_tag,
                                 const std::vector<AsokServiceDef>& hv)
{
  auto ne = servers.try_emplace(
                                server_tag,
                                hv);

  //  is this server ID already registered?
  if (!ne.second) {
    return AsokRegistrationRes{false, false};
  }

  return AsokRegistrationRes{true, false}; // not checking the 3rd value for now
}

/*!
  Called by the server implementing the hook. The gate will be closed, and the function
  will block until all execution of commands within the gate are done.
 */
seastar::future<> AdminSocket::unregister_server(hook_server_tag server_tag)
{
  //  locate the server registration
  auto srv_itr = servers.find(server_tag);
  if (srv_itr == servers.end()) {
    return seastar::now();
  }

  /*srv_itr->second.m_gate.close();
  servers.erase(srv_itr);
  return seastar::now();*/

  std::cerr << "\n~server (AS:" << (uint64_t)(this) <<") " << (uint64_t)(server_tag) << " to be deleted" <<  (uint64_t)(srv_itr->first)<< std::endl;

  return (srv_itr->second.m_gate.close()).
    then([this, srv_itr, server_tag]() {
      std::cerr << "\n~server_2 " << (uint64_t)(srv_itr->first) << " erasure" << std::endl;
      servers.erase(srv_itr);
      return seastar::now();
    });
}

#if 0
//  the internal handling of a registration request, after that request
//  was forwarded from the requesting core.
seastar::future<bool> AdminSocket::handle_registration(hook_server_tag  server_tag,
                                      std::string command,
				      std::string cmddesc,
				      AdminSocketHook* hook,
				      std::string help)
{
  std::string_view cmd{command};
  //std::cerr << "fut_hr " << cmd << std::endl;
  auto h = std::find_if(hooks.begin(), hooks.end(), [this, cmd](const auto& h){
          return h.is_valid && h.cmd == cmd;
  });

  if (h != hooks.end()) {
    std::cerr << "hri failed ###################" << std::endl;
    logger().warn("{}: command {} already registered", __func__, command); 
    return seastar::make_ready_future<bool>(false);
  }

  //std::cerr << "fut_hr to add " << cmd << std::endl;
  hooks.emplace_back(command, server_tag, hook, cmddesc, help);
  //hooks.emplace_back(i,
  //		     std::piecewise_construct,
  //		     std::forward_as_tuple(command),
  //		     std::forward_as_tuple(server_tag, hook, cmddesc, help));
  //std::cerr << "fut_hr done " << cmd << std::endl;
  logger().info("{}: command {} registered", __func__, command); 
  return seastar::make_ready_future<bool>(true);
}

seastar::future<bool> AdminSocket::register_command(hook_server_tag  server_tag,
                                  std::string command,
				  std::string cmddesc,
				  AdminSocketHook* hook,
				  std::string help)
{
  //  are we on the admin-specific core? if not - send to that core.
  //  Not handled for now, as only one core is used for Crimson at this point.
  //  if (core != admin_core) submit_to()...

  // \todo missing multi-core code
  return handle_registration(server_tag, command, cmddesc, hook, help);
}


///  called when we know that we are not executing any hook
seastar::future<> AdminSocket::delayed_unregistration(std::string command)
{
  //auto h = hooks.find(command);
  //if (h != hooks.end()) {
  //  hooks.erase(h);
  //} // the 'else' should not happen, but is not an issue

  return seastar::now();
}
#endif

#if 0
seastar::future<> AdminSocket::unregister_command(std::string_view cmd)
{
  auto valid_match = [cmd](const hook_info& h) -> bool {
                            return h.is_valid && h.cmd == cmd;
                          };

  auto h = std::find_if(hooks.begin(), hooks.end(), valid_match);

  if (h == hooks.end()) {
    logger().warn("{}: {} is not a registered command", __func__, cmd);
  } else {

    h->is_valid = false;

    //  \todo:
    //  Create an unregistration promise, but do not schedule it yet.
    //  Add it to a queue of waiting deletions.
    //  (When can we schedule it?)
  }
  return seastar::now();
}
#endif


class VersionHook : public AdminSocketHook {
public:
  seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format, bufferlist& out) const override {
    if (command == "0"sv) {
      out.append(CEPH_ADMIN_SOCK_VERSION);
    } else {
      // always using a JSON formatter
      JSONFormatter jf;
      jf.open_object_section("version");
      if (command == "version") {
	jf.dump_string("version", ceph_version_to_str());
	jf.dump_string("release", ceph_release_to_str());
	jf.dump_string("release_type", ceph_release_type());
      } else if (command == "git_version") {
	jf.dump_string("git_version", "x" /*git_version_to_str()*/);
      }
      std::ostringstream ss;
      jf.close_section();
      jf.enable_line_break();
      jf.flush(ss);
      out.append(ss.str());
    }
    return seastar::make_ready_future<bool>(true);
  }
};

class HelpHook : public AdminSocketHook {
  AdminSocket* m_as;
public:
  explicit HelpHook(AdminSocket* as) : m_as{as} {}

  seastar::future<> exec_command(ceph::Formatter* f, std::string_view command, const cmdmap_t& cmdmap,
	                                 std::string_view format, bufferlist& out) const final {
    for (const auto& hk_info : *m_as) {
      if (hk_info->help.length())
	f->dump_string(hk_info->command.c_str(), hk_info->help);
    }
    return seastar::now();
  }
};

class GetdescsHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit GetdescsHook(AdminSocket *as) : m_as{as} {}

  seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format, bufferlist& out) const final {
    int cmdnum = 0;
    JSONFormatter jf;
    jf.open_object_section("command_descriptions");

#ifdef JUST_FOR_A_MINUTE


    for (const auto& hk_info : m_as->hooks) {
      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(&jf,
                                CEPH_FEATURES_ALL,
				secname.str().c_str(),
				hk_info.desc,
				hk_info.help);
      cmdnum++;
    }

#endif


    jf.close_section(); // command_descriptions
    jf.enable_line_break();
    ostringstream ss;
    jf.flush(ss);
    out.append(ss.str());
    return seastar::make_ready_future<bool>(true);
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
    throw(std::invalid_argument("As::TestThrowHook"));
  }
};

AdminSocket::GateAndHook AdminSocket::locate_command(const std::string_view cmd)
{
  for (auto& [tag, srv] : servers) {

    // "lock" the server's control block before searching for the command string
    try {
      srv.m_gate.enter();
    } catch (...) {
      // gate is already closed
      continue;
    }

    for (auto& api : srv.m_hooks) {
      if (api.command == cmd) {
        logger().info("{}: located {} w/ server {}", __func__, cmd, tag);
        // note that the gate was entered! to fix RRR
        return AdminSocket::GateAndHook{&srv.m_gate, &api};
      }
    }

    // not found. Close this server's gate.
    srv.m_gate.leave();
  }

  return AdminSocket::GateAndHook{nullptr, nullptr};
}

/// the incoming command text, which is in JSON or XML, is parsed down
/// to its separate op-codes and arguments. The hook registered to handle the
/// specific command is located.
std::optional<AdminSocket::parsed_command_t> AdminSocket::parse_cmd(const std::string& command_text)
{
  cmdmap_t cmdmap;
  vector<string> cmdvec;
  stringstream errss;

  //  note that cmdmap_from_json() may throw on syntax issues
  cmdvec.push_back(command_text); // as cmdmap_from_json() likes the input in this format
  try {
    if (!cmdmap_from_json(cmdvec, &cmdmap, errss)) {
      logger().error("{}: incoming command error: {}", __func__, errss.str());
      return std::nullopt;
    }
  } catch ( ... ) {
    logger().error("{}: incoming command syntax: {}", __func__, command_text);
    return std::nullopt;
  }

  string format;
  string match;
  try {
    cmd_getval(m_cct, cmdmap, "format", format);
    cmd_getval(m_cct, cmdmap, "prefix", match);
  } catch (const bad_cmd_get& e) {
    return std::nullopt;
  }
  if (format != "json" && format != "json-pretty" &&
      format != "xml" && format != "xml-pretty")
    format = "json-pretty";

  // try to match the longest set of strings. Failing - remove the tail part and retry
  //decltype(hooks)::iterator p = hooks.end();
  AdminSocket::GateAndHook gh{nullptr, nullptr};
  while (match.size()) {
    gh = std::move(locate_command(match));
    if (gh.api)
      break;

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


  #ifdef until_used_in_a_call_to_validate
  string args;
  if (match != command_text) {
    args = command_text.substr(match.length() + 1);
  }
  // TODO call validate()
  #endif

  return parsed_command_t{match, cmdmap, format, gh.api->hook, gh.m_gate};
}

seastar::future<> AdminSocket::execute_line(std::string cmdline, seastar::output_stream<char>& out)
{
  //  find the longest word-sequence that we have a hook registered to handle
  auto parsed = parse_cmd(cmdline);
  if (!parsed) {
    return out.write("command syntax error");
    //return seastar::now();
  }

  ::ceph::bufferlist out_buf;

  return seastar::do_with(std::move(parsed), std::move(out_buf), [&out, gatep=parsed->m_gate](auto&& parsed, auto&& out_buf) {
    std::cerr << "gate-cnt " << parsed->m_gate->get_count() << std::endl;
    return parsed->m_hook->call(parsed->m_cmd, parsed->m_parameters, (*parsed).m_format, out_buf).
      then_wrapped([&out, &out_buf](auto fut) {
        if (fut.failed()) {
          // add 'failed' to the contents on out_buf

          return seastar::make_ready_future<bool>(false);
        } else {
          return fut;
        }
      }).
      then([&out, &out_buf, gatep](auto call_res) {
        std::cerr << "gate-leave "  << std::endl;
        gatep->leave();
        uint32_t response_length = htonl(out_buf.length());
        std::cerr << "resp length: " << out_buf.length() << std::endl;
        return out.write((char*)&response_length, sizeof(uint32_t)).then([&out, &out_buf](){
          return out.write(out_buf.to_str());
        });
      });
  });
}

seastar::future<> AdminSocket::handle_client(seastar::input_stream<char>&& inp, seastar::output_stream<char>&& out)
{
  //  RRR \todo safe read
  //  RRR \todo handle old protocol (see original code)

  return inp.read().
    then( [&out, this](auto full_cmd) {

      return execute_line(full_cmd.share().get(), out);

    }).then([&out]() { return out.flush(); }).
    then([&out]() { return out.close(); }).
    then([&inp]() { return inp.close(); }).
    discard_result();
}

seastar::future<> AdminSocket::init(const std::string& path)
{
  std::cout << "AdminSocket::init() w " << path << " owner: " << (uint64_t)(this) <<
        " -> " << (uint64_t)(m_cct) << std::endl;

  return seastar::async([this, path] {
    auto serverfut = init_async(path);
    //(void)serverfut.get();
  }); 
}


/// the hooks that are served directly by the admin_socket server
void AdminSocket::internal_hooks()
{
  version_hook = std::make_unique<VersionHook>();
  help_hook = std::make_unique<HelpHook>(this);
  getdescs_hook = std::make_unique<GetdescsHook>(this);
  test_throw_hook = std::make_unique<TestThrowHook>(this);

  static const std::vector<AsokServiceDef> internal_hooks_tbl{
      AsokServiceDef{"0",            "0",                    version_hook.get(),     ""}
    , AsokServiceDef{"version",      "version",              version_hook.get(),     "get ceph version"}
    , AsokServiceDef{"git_version",  "git_version",          version_hook.get(),     "get git sha1"}
    , AsokServiceDef{"git_version",  "git_version",          version_hook.get(),     "get git sha1"}
    , AsokServiceDef{"help",         "help",                 help_hook.get(),        "list available commands"}
    , AsokServiceDef{"get_command_descriptions", "get_command_descriptions",
                                                             getdescs_hook.get(), "list available commands"}
    , AsokServiceDef{"throwAs",      "throwAs",              test_throw_hook.get(),  "dev throw"}
    , AsokServiceDef{"fthrowAs",     "fthrowAs",             test_throw_hook.get(),  "dev throw"}
  };

  std::ignore = server_registration(AdminSocket::hook_server_tag{this}, internal_hooks_tbl);

  #if 0
  std::ignore = seastar::when_all_succeed(
    register_command(AdminSocket::hook_server_tag{this},
                "0",            "0",                    version_hook.get(),     ""),
    register_command(AdminSocket::hook_server_tag{this},
                "version",      "version",              version_hook.get(),     "get ceph version"),
    register_command(AdminSocket::hook_server_tag{this},
                "git_version",  "git_version",          version_hook.get(),     "get git sha1"),
    register_command(AdminSocket::hook_server_tag{this},
                "help",         "help",                 help_hook.get(),        "list available commands"),
    register_command(AdminSocket::hook_server_tag{this},
                "get_command_descriptions", "get_command_descriptions", getdescs_hook.get(), "list available commands"),
    register_command(AdminSocket::hook_server_tag{this},
                "throwAs",      "throwAs",       test_throw_hook.get(),         "dev throw"),
    register_command(AdminSocket::hook_server_tag{this},
                "fthrowAs",     "fthrowAs",      test_throw_hook.get(),         "dev throw")
  );
  #endif
}

seastar::future<> AdminSocket::init_async(const std::string& path)
{
  //  verify we are the only instance running now RRR
  
  logger().debug("{}: path={}", __func__, path);

  internal_hooks();

  auto sock_path = seastar::socket_address{seastar::unix_domain_addr{path}};

  return seastar::do_with(seastar::engine().listen(sock_path), [this](seastar::server_socket& lstn) {

    return seastar::do_until([this](){ return do_die; }, [&lstn,this]() {
      return lstn.accept().
        then([this](seastar::accept_result from_accept) {

          seastar::connected_socket cn    = std::move(from_accept.connection);
          //seastar::socket_address cn_addr = std::move(from_accept.remote_address);

          return do_with(std::move(cn.input()), std::move(cn.output()), [this](auto& inp, auto& out) {

            return handle_client(std::move(inp), std::move(out)).
	      then([]() { return seastar::make_ready_future<>(); });
	  }).then([]() { return seastar::make_ready_future<>(); });
      });
    });
  });			
}

#if 0
{
  ldout(m_cct, 5) << "init " << path << dendl;

  /* Set up things for the new thread */
  std::string err;
  int pipe_rd = -1, pipe_wr = -1;
  err = create_shutdown_pipe(&pipe_rd, &pipe_wr);
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocketConfigObs::init: error: " << err << dendl;
    return false;
  }
  int sock_fd;
  err = bind_and_listen(path, &sock_fd);
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocketConfigObs::init: failed: " << err << dendl;
    close(pipe_rd);
    close(pipe_wr);
    return false;
  }

  /* Create new thread */
  m_sock_fd = sock_fd;
  m_shutdown_rd_fd = pipe_rd;
  m_shutdown_wr_fd = pipe_wr;
  m_path = path;

  version_hook = std::make_unique<VersionHook>();
  register_command("0", "0", version_hook.get(), "");
  register_command("version", "version", version_hook.get(), "get ceph version");
  register_command("git_version", "git_version", version_hook.get(),
		   "get git sha1");
  help_hook = std::make_unique<HelpHook>(this);
  register_command("help", "help", help_hook.get(),
		   "list available commands");
  getdescs_hook = std::make_unique<GetdescsHook>(this);
  register_command("get_command_descriptions", "get_command_descriptions",
		   getdescs_hook.get(), "list available commands");

  th = make_named_thread("admin_socket", &AdminSocket::entry, this);
  add_cleanup_file(m_path.c_str());
  return true;
}


void AdminSocket::shutdown()
{
  // Under normal operation this is unlikely to occur.  However for some unit
  // tests, some object members are not initialized and so cannot be deleted
  // without fault.
  if (m_shutdown_wr_fd < 0)
    return;

  ldout(m_cct, 5) << "shutdown" << dendl;

  auto err = destroy_shutdown_pipe();
  if (!err.empty()) {
    lderr(m_cct) << "AdminSocket::shutdown: error: " << err << dendl;
  }

  retry_sys_call(::close, m_sock_fd);

  unregister_commands(version_hook.get());
  version_hook.reset();

  unregister_command("help");
  help_hook.reset();

  unregister_command("get_command_descriptions");
  getdescs_hook.reset();

  remove_cleanup_file(m_path);
  m_path.clear();
}

#endif

