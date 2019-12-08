// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "common/version.h"
#include "crimson/net/Socket.h"
#include "crimson/admin/admin_socket.h"
#include "crimson/common/log.h"
#include "seastar/net/api.hh"
#include "seastar/net/inet_address.hh"
#include "seastar/core/reactor.hh"
#include "seastar/core/thread.hh"
#include "seastar/util/std-compat.hh"
#include <boost/algorithm/string.hpp>

/**
  A Crimson-wise version of the admin socket - implementation file

  \todo handle the unlinking of the admin socket. Note that 'cleanup_files'
  at-exit functionality is not yet implemented in Crimson.
*/

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

namespace crimson::admin {

/**
  Hooks table - iterator support
 */

AdminHooksIter AdminSocket::begin()
{
  AdminHooksIter it{ *this };
  return it;
}

AdminHooksIter AdminSocket::end()
{
  AdminHooksIter it{ *this, true };
  return it;
}

/**
  note that 'end-flag' is an optional parameter, and is only used internally (to
  signal the end() state).
 */
AdminHooksIter::AdminHooksIter(AdminSocket& master, bool end_flag)
    : m_master{ master }, m_end_marker{ end_flag }
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
  if (!m_end_marker) {
    ++m_siter;
    if (m_siter == m_miter->second.m_hooks.end()) {
      // move to the next server-block
      m_miter++;
      if (m_miter == m_master.servers.end()) {
        m_end_marker = true;
        return *this;
      }

      m_siter = m_miter->second.m_hooks.begin();
    }
  }
  return *this;
}

/**
  the default implementation of the hook API

  Note that we never throw or return a failed future.
 */
seastar::future<bool> AdminSocketHook::call(std::string_view  command,
                                            const cmdmap_t&   cmdmap,
                                            std::string_view  format,
                                            ceph::bufferlist& out) const
{
  unique_ptr<Formatter> f{ Formatter::create(format, "json-pretty"sv,
                                             "json-pretty"s) };

  // customize the output section, as required by a couple of hook APIs:
  std::string section{ section_name(command) };
  boost::replace_all(section, " ", "_");
  if (format_as_array()) {
    f->open_array_section(section.c_str());
  } else {
    f->open_object_section(section.c_str());
  }

  bool bres{ false };  ///< 'false' for any failure of in API execution

  /*
    call the command-specific hook.
    A note re error handling:
        - will be modified to use the new 'erroretor'. For now:
        - exec_command() may throw or return an exceptional future. We return an
           error message on both failure scenarios.
   */
  return seastar::do_with(
    std::move(f), std::move(bres),
    [this, &command, &cmdmap, &format, &out](unique_ptr<Formatter>& ftr,
                                             bool&                  br) {
      return seastar::futurize_apply(
               [this, &command, &cmdmap, &format, &out, f = ftr.get()] {
                 return exec_command(f, command, cmdmap, format, out);
               })
        .then_wrapped([&ftr, &command,
                       &br](seastar::future<> res) -> seastar::future<bool> {
          //  we now have a ready future (or a failure)
          if (res.failed()) {
            std::ignore = res.handle_exception([](std::exception_ptr eptr) {
              return seastar::make_ready_future<>();
            });
          } else {
            br = true;  // exec_command() successful
            res.ignore_ready_future();
          }

          return seastar::make_ready_future<bool>(br);
        })
        .then([this, &ftr, &out](auto res) -> seastar::future<bool> {
          ftr->close_section();
          ftr->enable_line_break();
          ftr->flush(out);
          return seastar::make_ready_future<bool>(res);
        });
    });
}

AdminSocket::AdminSocket(CephContext* cct) : m_cct(cct) {}

AdminSocket::~AdminSocket() {}

AsokRegistrationRes AdminSocket::server_registration(
  AdminSocket::hook_server_tag       server_tag,
  const std::vector<AsokServiceDef>& apis_served)
{
  auto ne = servers.try_emplace(server_tag, apis_served);

  //  was this server tag already registered?
  if (!ne.second) {
    return std::nullopt;
  }

  logger().info("{}: pid:{} ^:{} (tag: {})", __func__, (int)getpid(),
                (uint64_t)(this), (uint64_t)(server_tag));
  return this->shared_from_this();
}

seastar::future<AsokRegistrationRes> AdminSocket::register_server(
  hook_server_tag server_tag, const std::vector<AsokServiceDef>& apis_served)
{
  return seastar::with_lock(
    servers_tbl_rwlock, [this, server_tag, &apis_served]() {
      return server_registration(server_tag, apis_served);
    });
}

seastar::future<> AdminSocket::unregister_server(hook_server_tag server_tag)
{
  logger().debug("{}: pid:{} server tag: {})", __func__, (int)getpid(),
                 (uint64_t)(server_tag));

  return seastar::with_lock(servers_tbl_rwlock,
                            [this, server_tag]() {
                              auto srv_itr = servers.find(server_tag);
                              if (srv_itr == servers.end()) {
                                logger().warn(
                                  "unregister_server(): unregistering a "
                                  "non-existing registration (tag: {})",
                                  (uint64_t)(server_tag));
                              } else {
                                servers.erase(srv_itr);
                              }
                              return seastar::now();
                            })
    .finally([server_tag]() {
      logger().debug("unregister_server(): done removing server block {}",
                     (uint64_t)(server_tag));
    });
}

seastar::future<> AdminSocket::unregister_server(hook_server_tag  server_tag,
                                                 AdminSocketRef&& server_ref)
{
  // reducing the ref-count on us (the ASOK server) by discarding server_ref:
  return seastar::do_with(std::move(server_ref), [this, server_tag](auto& srv) {
    return unregister_server(server_tag).finally([this, server_tag, &srv]() {
      logger().debug("unregister_server: {}", (uint64_t)(server_tag));
    });
  });
}

AdminSocket::maybe_service_def_t AdminSocket::locate_subcmd(
  std::string match) const
{
  AdminSocket::maybe_service_def_t gh;

  while (match.size()) {
    // try locating this sub-sequence of the incoming command
    for (auto& [tag, srv] : servers) {
      for (auto& api : srv.m_hooks) {
        if (api.command == match) {
          logger().debug("{}: located {} w/ server {}", __func__, match, tag);
          gh = &api;
          break;
        }
      }
    }

    if (gh.has_value()) {
      // found a match
      break;
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

  return gh;
}

/*
  Note: parse_cmd() is executed with servers_tbl_rwlock held as shared
*/
AdminSocket::Maybe_parsed AdminSocket::parse_cmd(std::string       command_text,
                                                 ceph::bufferlist& out)
{
  /*
    preliminaries:
      - create the formatter specified by the command_text parameters
      - locate the "op-code" string (the 'prefix' segment)
      - prepare for command parameters extraction via cmdmap_t
  */
  cmdmap_t       cmdmap;
  vector<string> cmdvec;
  stringstream   errss;

  cmdvec.push_back(command_text);  // as cmdmap_from_json() likes
                                   // the input in this format

  try {
    //  note that cmdmap_from_json() may throw on syntax issues
    if (!cmdmap_from_json(cmdvec, &cmdmap, errss)) {
      logger().error("{}: incoming command error: {}", __func__, errss.str());
      out.append("error:"s);
      out.append(errss.str());

      return Maybe_parsed{ std::nullopt };
    }
  } catch (...) {
    logger().error("{}: incoming command syntax: {}", __func__, command_text);
    out.append("error: command syntax"s);
    return Maybe_parsed{ std::nullopt };
  }

  string      format;
  string      match;
  std::size_t full_command_seq;  // the full sequence, before we start chipping
                                 // away the end
  try {
    cmd_getval(m_cct, cmdmap, "format", format);
    cmd_getval(m_cct, cmdmap, "prefix", match);
    full_command_seq = match.length();
  } catch (const bad_cmd_get& e) {
    logger().error("{}: invalid syntax: {}", __func__, command_text);
    out.append("error: command syntax: missing 'prefix'"s);
    return Maybe_parsed{ std::nullopt };
  }

  if (!match.length()) {
    // no command identified
    out.append("error: no command identified"s);
    return Maybe_parsed{ std::nullopt };
  }

  if (format != "json"s && format != "json-pretty"s && format != "xml"s &&
      format != "xml-pretty"s) {
    format = "json-pretty"s;
  }

  /*
     match the incoming op-code to one of the registered APIs
  */
  auto parsed_cmd = locate_subcmd(match);
  if (!parsed_cmd.has_value()) {
    return Maybe_parsed{ std::nullopt };
  }

  return parsed_command_t{ match,
                           cmdmap,
                           format,
                           parsed_cmd.value(),
                           parsed_cmd.value()->hook,
                           full_command_seq };
}

/*
  Note: validate_command() is executed with servers_tbl_rwlock held as shared
*/
bool AdminSocket::validate_command(const parsed_command_t& parsed,
                                   const std::string&      command_text,
                                   ceph::bufferlist&       out) const
{
  // did we receive any arguments apart from the command word(s)?
  if (parsed.m_cmd_seq_len == parsed.m_cmd.length())
    return true;

  logger().info("{}: validating {} against:{}", __func__, command_text,
                parsed.m_api->cmddesc);

  stringstream os;  // for possible validation error messages
  try {
    // validate_cmd throws on some syntax errors
    if (validate_cmd(m_cct, parsed.m_api->cmddesc, parsed.m_parameters, os)) {
      return true;
    }
  } catch (std::exception& e) {
    logger().error("{}: validation failure ({} : {}) {}", __func__,
                   command_text, parsed.m_cmd, e.what());
  }

  os << "error: command validation failure ";
  logger().error("{}: validation failure (incoming:{}) {}", __func__,
                 command_text, os.str());
  out.append(os);
  return false;
}

seastar::future<> AdminSocket::finalyze_response(
  seastar::output_stream<char>& out, ceph::bufferlist&& msgs)
{
  string outbuf_cont = msgs.to_str();
  if (outbuf_cont.length() == 0) {
    outbuf_cont = " {} ";
  }
  uint32_t response_length = htonl(outbuf_cont.length());
  logger().info("asok response length: {}", outbuf_cont.length());

  return out.write((char*)&response_length, sizeof(uint32_t))
    .then([&out, outbuf_cont]() { return out.write(outbuf_cont.c_str()); });
}

seastar::future<> AdminSocket::execute_line(std::string cmdline,
                                            seastar::output_stream<char>& out)
{
  return seastar::with_shared(servers_tbl_rwlock, [this, cmdline,
                                                   &out]() mutable {
    ceph::bufferlist msgs;
    Maybe_parsed     oparsed;

    bool ok_to_execute{ false };
    try {
      oparsed = parse_cmd(cmdline, msgs);
      if (oparsed.has_value())
        ok_to_execute = validate_command(*oparsed, cmdline, msgs);
    } catch (...) {
    };

    if (!ok_to_execute) {
      return finalyze_response(out, std::move(msgs));
    }

    return seastar::do_with(
      std::move(*oparsed), std::move(msgs),
      [this, &out](auto&& parsed, auto&& out_buf) {
        return (parsed.m_hook->call(parsed.m_cmd, parsed.m_parameters,
                                    parsed.m_format, out_buf))
          .then_wrapped([&out, &out_buf](auto&& fut) -> seastar::future<bool> {
            if (fut.failed()) {
              // add 'failed' to the contents of out_buf? not what
              // happens in the old code
              return seastar::make_ready_future<bool>(false);
            } else {
              return std::move(fut);
            }
          })
          .then([this, &out, &out_buf](auto call_res) {
            return finalyze_response(out, std::move(out_buf));
          });
      });
  });
}

seastar::future<> AdminSocket::handle_client(seastar::input_stream<char>&  inp,
                                             seastar::output_stream<char>& out)
{
  //  RRR \todo handle old protocol (see original code) - is still
  //  needed?

  return inp.read()
    .then([&out, this](auto full_cmd) {
      seastar::sstring cmd_line{ full_cmd.begin(), full_cmd.end() };
      logger().debug("AdminSocket::handle_client: incoming asok string: {}",
                     cmd_line);
      return execute_line(cmd_line, out);
    })
    .then([&out]() { return out.flush(); })
    .finally([&out]() { return out.close(); })
    .then([&inp, &out]() { return inp.close(); })
    .handle_exception([](auto ep) {
      logger().debug("exception on {}: {}", __func__, ep);
      return seastar::make_ready_future<>();
    })
    .discard_result();
}

/**
  \brief continuously run a block of code "within a gate"

  Neither gate closure, nor a failure of the code block we are running, will
  cause an exception to be thrown by safe_action_gate_func()
*/
template <typename AsyncAction>
seastar::future<> do_until_gate(seastar::gate& gt, AsyncAction action)
{
  auto stop_cond = [&gt]() { return gt.is_closed(); };
  auto safe_action{ [act = std::move(action), &gt]() mutable {
    if (gt.is_closed())
      return seastar::make_ready_future<>();
    return with_gate(gt, [act = std::move(act)]() {
      return act().handle_exception([](auto e) {
        // std::cerr << "do_until_gate" << std::endl;
        // logger().warn("do_until_gate");
        // return seastar::now();
      });
    });
  } };

  return seastar::do_until(stop_cond, std::move(safe_action)).discard_result();
}

seastar::future<> AdminSocket::init(const std::string& path)
{
  if (!path.length()) {
    logger().error(
      "{}: Admin Socket socket path missing from the configuration", __func__);
    return seastar::now();
  }

  logger().debug("{}: asok socket path={}", __func__, path);
  auto sock_path = seastar::socket_address{ seastar::unix_domain_addr{ path } };

  std::ignore =
    internal_hooks().then([this, sock_path](AsokRegistrationRes reg_res) {
      return seastar::do_with(
        seastar::engine().listen(sock_path),
        [this](seastar::server_socket& lstn) {
          m_server_sock = &lstn;  // used for 'abort_accept()'
          return do_until_gate(arrivals_gate,
                               [&lstn, this]() {
                                 return lstn.accept().then(
                                   [this](seastar::accept_result from_accept) {
                                     seastar::connected_socket cn =
                                       std::move(from_accept.connection);

                                     return do_with(
                                       std::move(cn.input()),
                                       std::move(cn.output()), std::move(cn),
                                       [this](auto& inp, auto& out, auto& cn) {
                                         return handle_client(inp, out).finally(
                                           [this, &inp, &out] {
                                             ;  // left for debugging
                                           });
                                       });
                                   });
                               })
            .then([this] {
              logger().debug("AdminSocket::init(): admin-sock thread terminated");
              return seastar::now();
            });
        });
    });

  return seastar::make_ready_future<>();
}

seastar::future<> AdminSocket::stop()
{
  // RF Dec-2019: abort_accept(), at least when applied to a UNIX-domain socket
  // with an 'epoll' backend, will not correctly abort a Seastar accept():
  // epoll_wait() will signal EPOLLRDHUP+EPOLLIN, and that would be handled
  // incorrectly.

  if (m_server_sock && !arrivals_gate.is_closed()) {
    // note that we check 'is_closed()' as if already closed - the server-sock
    // may have already been discarded
    m_server_sock->abort_accept();
    m_server_sock = nullptr;
  }

  return seastar::futurize_apply([this] { return arrivals_gate.close(); })
    .then_wrapped([this](seastar::future<> res) {
      if (res.failed()) {
        std::ignore = res.handle_exception([](std::exception_ptr eptr) {
          return seastar::make_ready_future<>();
        });
      }
      return seastar::make_ready_future<>();
    })
    .handle_exception(
      [](std::exception_ptr eptr) { return seastar::make_ready_future<>(); })
    .finally([this]() { return seastar::make_ready_future<>(); });
}

// ///////////////////////////////////////
// the internal hooks
// ///////////////////////////////////////

/**
  The response to the '0' command is not formatted, neither as JSON or
  otherwise.
*/
class The0Hook : public AdminSocketHook {
 public:
  seastar::future<bool> call([[maybe_unused]] std::string_view command,
                             [[maybe_unused]] const cmdmap_t&,
                             [[maybe_unused]] std::string_view format,
                             bufferlist& out) const override
  {
    out.append(CEPH_ADMIN_SOCK_VERSION);
    return seastar::make_ready_future<bool>(true);
  }
};

class VersionHook : public AdminSocketHook {
  AdminSocket* m_as;

 public:
  explicit VersionHook(AdminSocket* as) : m_as{ as } {}

  seastar::future<> exec_command(ceph::Formatter*                  f,
                                 [[maybe_unused]] std::string_view command,
                                 [[maybe_unused]] const cmdmap_t&  cmdmap,
                                 [[maybe_unused]] std::string_view format,
                                 [[maybe_unused]] bufferlist& out) const final
  {
    f->dump_string("version", ceph_version_to_str());
    f->dump_string("release", ceph_release_to_str());
    f->dump_string("release_type", ceph_release_type());
    return seastar::now();
  }
};

/**
  Note that the git_version command is expected to return a 'version' JSON
  segment.
*/
class GitVersionHook : public AdminSocketHook {
  AdminSocket* m_as;

 public:
  explicit GitVersionHook(AdminSocket* as) : m_as{ as } {}

  std::string section_name(std::string_view command) const final
  {
    return "version"s;
  }

  seastar::future<> exec_command(ceph::Formatter*                  f,
                                 [[maybe_unused]] std::string_view command,
                                 [[maybe_unused]] const cmdmap_t&  cmdmap,
                                 [[maybe_unused]] std::string_view format,
                                 [[maybe_unused]] bufferlist& out) const final
  {
    f->dump_string("git_version", git_version_to_str());
    return seastar::now();
  }
};

class HelpHook : public AdminSocketHook {
  AdminSocket* m_as;

 public:
  explicit HelpHook(AdminSocket* as) : m_as{ as } {}

  seastar::future<> exec_command(ceph::Formatter*                  f,
                                 [[maybe_unused]] std::string_view command,
                                 [[maybe_unused]] const cmdmap_t&  cmdmap,
                                 [[maybe_unused]] std::string_view format,
                                 [[maybe_unused]] bufferlist& out) const final
  {

    return seastar::with_shared(m_as->servers_tbl_rwlock, [this, f]() {
      for (const auto& hk_info : *m_as) {
        if (hk_info->help.length())
          f->dump_string(hk_info->command.c_str(), hk_info->help);
      }
      return seastar::now();
    });
  }
};

class GetdescsHook : public AdminSocketHook {
  AdminSocket* m_as;

 public:
  explicit GetdescsHook(AdminSocket* as) : m_as{ as } {}

  std::string section_name(std::string_view command) const final
  {
    return "command_descriptions"s;
  }

  seastar::future<> exec_command(ceph::Formatter*                  f,
                                 [[maybe_unused]] std::string_view command,
                                 [[maybe_unused]] const cmdmap_t&  cmdmap,
                                 [[maybe_unused]] std::string_view format,
                                 [[maybe_unused]] bufferlist& out) const final
  {
    return seastar::with_shared(m_as->servers_tbl_rwlock, [this, f]() {
      int cmdnum = 0;

      for (const auto& hk_info : *m_as) {
        ostringstream secname;
        secname << "cmd" << setfill('0') << std::setw(3) << cmdnum;
        dump_cmd_and_help_to_json(f, CEPH_FEATURES_ALL, secname.str().c_str(),
                                  hk_info->cmddesc, hk_info->help);
        cmdnum++;
      }
      return seastar::now();
    });
  }
};

class TestThrowHook : public AdminSocketHook {
  AdminSocket* m_as;

 public:
  explicit TestThrowHook(AdminSocket* as) : m_as{ as } {}

  seastar::future<> exec_command([[maybe_unused]] ceph::Formatter* f,
                                 std::string_view                  command,
                                 [[maybe_unused]] const cmdmap_t&  cmdmap,
                                 [[maybe_unused]] std::string_view format,
                                 [[maybe_unused]] bufferlist& out) const final
  {
    if (command == "fthrowAs")
      return seastar::make_exception_future<>(
        std::system_error{ 1, std::system_category() });

    return seastar::sleep(3s).then([this]() {
      throw(std::invalid_argument("As::TestThrowHook"));
      return seastar::now();
    });
  }
};

/// the hooks that are served directly by the admin_socket server
seastar::future<AsokRegistrationRes> AdminSocket::internal_hooks()
{
  version_hook    = std::make_unique<VersionHook>(this);
  git_ver_hook    = std::make_unique<GitVersionHook>(this);
  the0_hook       = std::make_unique<The0Hook>();
  help_hook       = std::make_unique<HelpHook>(this);
  getdescs_hook   = std::make_unique<GetdescsHook>(this);
  test_throw_hook = std::make_unique<TestThrowHook>(this);

  // clang-format off
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
  // clang-format on

  // server_registration() returns a shared pointer to the AdminSocket
  // server, i.e. to us. As we already have shared ownership of this object,
  // we do not need it.
  return register_server(AdminSocket::hook_server_tag{ this },
                         internal_hooks_tbl);
}

}  // namespace crimson::admin
