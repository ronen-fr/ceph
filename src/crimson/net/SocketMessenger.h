// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2017 Red Hat, Inc
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#pragma once

#include <map>
#include <optional>
#include <set>
#include <seastar/core/gate.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sharded.hh>
#include <seastar/core/shared_future.hh>


#include "Messenger.h"
#include "SocketConnection.h"

#if 0
template <typename S, typename T>
struct FST
{
        using type = seastar::future<T> (*)(S);
};

template <typename S, typename T, typename F = typename FST<S, T>::type>
auto FuturePass(F f, S s) -> seastar::future<T>
{
        if (s.has_value())
        {
                //std::cout << "\n{ pass: v= " << s.value() << " }\n";
                return f(s);
        }
        else
        {
                //std::cout << "\n{ pass: no value! }\n";
                return seastar::make_ready_future<T>(T{outcome::in_place_type<std::error_code>, s.error()});
        }
}



template <typename S, typename T, typename F = typename FST<S, T>::type>
auto FutureSplit(F happy_days, F on_error, S s) -> seastar::future<T>
{
        // debug prints:

        if (s.has_value())
        {
                //std::cout << " { Split: happy path }\n";
                return happy_days(s);
        }
        else
        {
                //std::cout << " { Split: error path }\n";
                return on_error(s);
        }
}
#endif

namespace ceph::net {

using ForeignConnOrFault = outcome::outcome<seastar::foreign_ptr<ConnectionRef>>;

class SocketMessenger final : public Messenger, public seastar::peering_sharded_service<SocketMessenger> {
  const int master_sid;
  const seastar::shard_id sid;
  seastar::promise<> shutdown_promise;

  std::optional<seastar::server_socket> listener;
  Dispatcher *dispatcher = nullptr;
  std::map<entity_addr_t, SocketConnectionRef> connections;
  std::set<SocketConnectionRef> accepting_conns;
  ceph::net::PolicySet<Throttle> policy_set;
  // Distinguish messengers with meaningful names for debugging
  const std::string logic_name;
  const uint32_t nonce;
  // specifying we haven't learned our addr; set false when we find it.
  bool need_addr = true;

  seastar::future<> accept(seastar::connected_socket socket,
                           seastar::socket_address paddr);

  void do_bind(const entity_addrvec_t& addr);

  bool started = false;
  seastar::shared_promise<> accepting_complete;
  seastar::future<> do_start(Dispatcher *disp);
  seastar::foreign_ptr<ConnectionRef> do_connect(const entity_addr_t& peer_addr,
                                                 const entity_type_t& peer_type);
  
  //ForeignConnOrFault do_connect_wcatch(const entity_addr_t& peer_addr,
  //                              const entity_type_t& peer_type);
  
  seastar::future<> do_shutdown();
  // conn sharding options:
  // 0. Compatible (master_sid >= 0): place all connections to one master shard
  // 1. Simplest (master_sid < 0): sharded by ip only
  // 2. Balanced (not implemented): sharded by ip + port + nonce,
  //        but, need to move SocketConnection between cores.
  seastar::shard_id locate_shard(const entity_addr_t& addr);

 public:
  SocketMessenger(const entity_name_t& myname,
                  const std::string& logic_name,
                  uint32_t nonce,
                  int master_sid);

  seastar::future<> set_myaddrs(const entity_addrvec_t& addr) override;

  // Messenger interfaces are assumed to be called from its own shard, but its
  // behavior should be symmetric when called from any shard.
  seastar::future<> bind(const entity_addrvec_t& addr) override;

  seastar::future<> try_bind(const entity_addrvec_t& addr,
                             uint32_t min_port, uint32_t max_port) override;

  seastar::future<> start(Dispatcher *dispatcher) override;

  seastar::future<ConnectionXRef> connect(const entity_addr_t& peer_addr,
                                          const entity_type_t& peer_type) override;

  //seastar::future<XConnOrFault> connect_wcatch(const entity_addr_t& peer_addr,
  //                                        const entity_type_t& peer_type) override;

  // can only wait once
  seastar::future<> wait() override {
    return shutdown_promise.get_future();
  }

  seastar::future<> shutdown() override;

  Messenger* get_local_shard() override {
    return &container().local();
  }

  void print(ostream& out) const override {
    out << get_myname()
        << "(" << logic_name
        << ") " << get_myaddr();
  }

  SocketPolicy get_policy(entity_type_t peer_type) const override;

  SocketPolicy get_default_policy() const override;

  void set_default_policy(const SocketPolicy& p) override;

  void set_policy(entity_type_t peer_type, const SocketPolicy& p) override;

  void set_policy_throttler(entity_type_t peer_type, Throttle* throttle) override;

 public:
  seastar::future<> learned_addr(const entity_addr_t &peer_addr_for_me,
                                 const SocketConnection& conn);

  SocketConnectionRef lookup_conn(const entity_addr_t& addr);
  void accept_conn(SocketConnectionRef);
  void unaccept_conn(SocketConnectionRef);
  void register_conn(SocketConnectionRef);
  void unregister_conn(SocketConnectionRef);

  // required by sharded<>
  seastar::future<> stop();

  seastar::shard_id shard_id() const {
    return sid;
  }
};

} // namespace ceph::net
