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

// temp include, to solve a compilation problem
//#include <seastar/core/future.hh>
//#include "messages/MOSDOp.h"
////ude "crimson/osd/pg.h"
////ude "crimson/osd/osd.h"
////ude "common/Formatter.h"
//ude "crimson/osd/osd_operations/client_request.h"
//ude "crimson/osd/osd_connection_priv.h"


//#include "common/version.h"
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

namespace {
  seastar::logger& logger() {
    return ceph::get_logger(ceph_subsys_osd);
  }
}


AdminSocket::AdminSocket(CephContext *cct)
  : m_cct(cct)
{
  std::cout << "######### a new AdminSocket " << (uint64_t)(this) <<
        " -> " << (uint64_t)(m_cct) << std::endl;
}


AdminSocket::~AdminSocket()
{
  //shutdown();
  std::cout << "######### XXXXX AdminSocket " << (uint64_t)(this) << std::endl;
}


//  the internal handling of a registration request, after that request
//  was forwarded from the requesting core.
int AdminSocket::handle_registration(hook_client_tag  client_tag,
                                      std::string_view command,
				      std::string_view cmddesc,
				      AdminSocketHook* hook,
				      std::string_view help)
{
  int ret;
  auto i = hooks.find(command);
  if (i != hooks.cend()) {
    logger().warn("{}: command {} already registered", __func__, command); 
    ret = -EEXIST;
  } else {
    logger().info("{}: command {} registered", __func__, command); 
    hooks.emplace_hint(i,
		       std::piecewise_construct,
		       std::forward_as_tuple(command),
		       std::forward_as_tuple(client_tag, hook, cmddesc, help));
    ret = 0;
  }
  return ret;
}

int AdminSocket::register_command(hook_client_tag  client_tag,
                                  std::string_view command,
				  std::string_view cmddesc,
				  AdminSocketHook *hook,
				  std::string_view help)
{
  //  are we on the admin-specific core? if not - send to that core.
  //  Not handled for now, as only one core is used for Crimson at this point.
  //  if (core != admin_core) submit_to()...

  return handle_registration(client_tag, command, cmddesc, hook, help);
}

#if 0

int AdminSocket::register_command__(std::string_view command,
				  std::string_view cmddesc,
				  AdminSocketHook *hook,
				  std::string_view help)
{
  int ret;
  std::unique_lock l(lock);
  auto i = hooks.find(command);
  if (i != hooks.cend()) {
    ldout(m_cct, 5) << "register_command " << command << " hook " << hook
		    << " EEXIST" << dendl;
    ret = -EEXIST;
  } else {
    ldout(m_cct, 5) << "register_command " << command << " hook " << hook
		    << dendl;
    hooks.emplace_hint(i,
		       std::piecewise_construct,
		       std::forward_as_tuple(command),
		       std::forward_as_tuple(hook, cmddesc, help));
    ret = 0;
  }
  return ret;
}

int AdminSocket::unregister_command__(std::string_view command)
{
  int ret;
  std::unique_lock l(lock);
  auto i = hooks.find(command);
  if (i != hooks.cend()) {
    ldout(m_cct, 5) << "unregister_command " << command << dendl;

    // If we are currently processing a command, wait for it to
    // complete in case it referenced the hook that we are
    // unregistering.
    in_hook_cond.wait(l, [this]() { return !in_hook; });

    hooks.erase(i);


    ret = 0;
  } else {
    ldout(m_cct, 5) << "unregister_command " << command << " ENOENT" << dendl;
    ret = -ENOENT;
  }
  return ret;
}
#endif

///  called when we know that we are not executing any hook
seastar::future<int> AdminSocket::delayed_unregistration(std::string command)
{
  auto i = hooks.find(command);
  if (i == hooks.cend()) {
    // shouldn't happen, but not an issue
    return seastar::make_ready_future<int>(-ENOENT);
  }
  hooks.erase(i);
  return seastar::make_ready_future<int>(0);
}


seastar::future<int> AdminSocket::unregister_command(std::string_view command)
{
  auto i = hooks.find(command);
  if (i == hooks.cend()) {
    logger().warn("{}: {} is not a registered command", __func__, command);
    return seastar::make_ready_future<int>(-ENOENT);
  }

  i->second.is_valid = false;
  
  //  Create an unregistration promise, but do not schedule it yet.
  //  Add it to a queue of waiting deletions.
  //  (When can we schedule it?)
  
  
  return seastar::make_ready_future<int>(0);
  #if 0
  int ret;
  std::unique_lock l(lock);
  auto i = hooks.find(command);
  if (i != hooks.cend()) {
    ldout(m_cct, 5) << "unregister_command " << command << dendl;

    // If we are currently processing a command, wait for it to
    // complete in case it referenced the hook that we are
    // unregistering.
    in_hook_cond.wait(l, [this]() { return !in_hook; });

    hooks.erase(i);


    ret = 0;
  } else {
    ldout(m_cct, 5) << "unregister_command " << command << " ENOENT" << dendl;
    ret = -ENOENT;
  }
  return ret;
  #endif
}

#if 0
void AdminSocket::unregister_commands(const AdminSocketHook *hook)
{
  std::unique_lock l(lock);
  auto i = hooks.begin();
  while (i != hooks.end()) {
    if (i->second.hook == hook) {
      ldout(m_cct, 5) << __func__ << " " << i->first << dendl;

      // If we are currently processing a command, wait for it to
      // complete in case it referenced the hook that we are
      // unregistering.
      in_hook_cond.wait(l, [this]() { return !in_hook; });
      hooks.erase(i++);
    } else {
      i++;
    }
  }
}
#endif

class VersionHook : public AdminSocketHook {
public:
  seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format, bufferlist& out) override {
    if (command == "0"sv) {
      out.append(CEPH_ADMIN_SOCK_VERSION);
    } else {
      JSONFormatter jf;
      jf.open_object_section("version");
      if (command == "version") {
	jf.dump_string("version", "x" /*ceph_version_to_str()*/);
	jf.dump_string("release", "x" /*ceph_release_to_str()*/);
	jf.dump_string("release_type", "x" /*ceph_release_type()*/);
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
  seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format,
	    bufferlist& out) override {
    std::unique_ptr<Formatter> f(Formatter::create(format, "json-pretty"sv,
						   "json-pretty"sv));
    f->open_object_section("help");
    for (const auto& [command, info] : m_as->hooks) {
      if (info.help.length())
	f->dump_string(command.c_str(), info.help);
    }
    f->close_section();
    ostringstream ss;
    f->flush(ss);
    out.append(ss.str());
    return seastar::make_ready_future<bool>(true);
  }
};

class GetdescsHook : public AdminSocketHook {
  AdminSocket *m_as;
public:
  explicit GetdescsHook(AdminSocket *as) : m_as{as} {}
  seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
	    std::string_view format, bufferlist& out) override {
    int cmdnum = 0;
    JSONFormatter jf;
    jf.open_object_section("command_descriptions");
    for (const auto& [command, info] : m_as->hooks) {
      // GCC 8 actually has [[maybe_unused]] on a structured binding
      // do what you'd expect. GCC 7 does not.
      (void)command;
      ostringstream secname;
      secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
      dump_cmd_and_help_to_json(&jf,
                                CEPH_FEATURES_ALL,
				secname.str().c_str(),
				info.desc,
				info.help);
      cmdnum++;
    }
    jf.close_section(); // command_descriptions
    jf.enable_line_break();
    ostringstream ss;
    jf.flush(ss);
    out.append(ss.str());
    return seastar::make_ready_future<bool>(true);
  }
};


/// the incoming command text, which is in JSON or XML, is parsed down
/// to its separate op-codes and arguments
std::optional<AdminSocket::parsed_command_t> AdminSocket::parse_cmd(const std::string& command_text)
{
  cmdmap_t cmdmap;
  vector<string> cmdvec;
  stringstream errss;

  cmdvec.push_back(command_text); // as cmdmap_from_json() likes the input in this format
  if (!cmdmap_from_json(cmdvec, &cmdmap, errss)) {
    logger().error("{}: incoming command error: {}", __func__, errss.str());
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
  decltype(hooks)::iterator p;
  while (match.size()) {
    p = hooks.find(match);
    if (p != hooks.cend() && p->second.is_valid)
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

  if (p == hooks.cend()) {
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

  return parsed_command_t{match, cmdmap, format, p->second.hook};
}


seastar::future<seastar::stop_iteration> AdminSocket::execute_line(std::string cmdline, seastar::output_stream<char>& out)
{
  //  extract the op-code (null or \n-terminated)

  #if 1

  auto parsed = parse_cmd(cmdline);
  if (!parsed) {
    return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
  }

  bufferlist out_buf;

  parsed->m_hook->call(parsed->m_cmd, parsed->m_parameters, (*parsed).m_format, out_buf);
  return out.write(out_buf.to_str()).
    then([&out](){
            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
    });

  #endif
  // dev
  string ans = "+"s + cmdline;

  return out.write(ans).
    then([](){
            return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes);
    });
}


// will handle the closing of the fds
seastar::future<> AdminSocket::handle_client(seastar::input_stream<char>&& inp, seastar::output_stream<char>&& out)
{
  //  RRR \todo safe read
  //  RRR \todo handle old protocol (see original code)
  
  return seastar::repeat([&out, &inp, this]() mutable { //  seems that no repeat is needed to mimic existing functionality

    return inp.read()
    .then( [&out, this](auto full_cmd) {

      return execute_line(full_cmd.share().get(), out);

    }).then([&out](seastar::stop_iteration is_done) {

      //return seastar::do_with( std::move(is_done), [&out](seastar::stop_iteration is_done) {
      //        out.flush();

      return out.flush();

    }).then([&out]() { 
        return seastar::make_ready_future<seastar::stop_iteration>(seastar::stop_iteration::yes); 
    });

  }).then([&out]() { 
    return out.close(); }).then([&inp]() {
    return inp.close(); }).
      discard_result();
}


seastar::future<> AdminSocket::init(const std::string& path)
{
  std::cout << "AdminSocket::init() w " << path << " owner: " << (uint64_t)(this) <<
        " -> " << (uint64_t)(m_cct) << std::endl;

  return seastar::async([this, path] {
    auto serverfut = init_async(path);
    (void)serverfut.get();
  }); 
}


/// the hooks that are served directly by the admin_socket server
void AdminSocket::internal_hooks()
{
  version_hook = std::make_unique<VersionHook>();
  register_command(hook_client_tag{this}, "0", "0", version_hook.get(), "");
  register_command(hook_client_tag{this}, "version", "version", version_hook.get(), "get ceph version");
  register_command(hook_client_tag{this}, "git_version", "git_version", version_hook.get(),
		   "get git sha1");

  help_hook = std::make_unique<HelpHook>(this);
  register_command(hook_client_tag{this}, "help", "help", help_hook.get(),
		   "list available commands");

  getdescs_hook = std::make_unique<GetdescsHook>(this);
  register_command(hook_client_tag{this}, "get_command_descriptions", "get_command_descriptions",
		   getdescs_hook.get(), "list available commands");
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

