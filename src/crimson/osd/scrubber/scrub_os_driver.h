// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <string>
#include <optional>
#include "seastar/core/shared_future.hh"

#include "crimson/common/sharedptr_registry.h"
#include "crimson/os/futurized_store.h"
#include "crimson/os/futurized_collection.h"
#include "crimson/osd/scrubber/scrub_map_cacher.h"
#include "os/Transaction.h"

namespace crimson::osd {


class OSDriver : public MapCacher::StoreDriver {
 public: // dev

  //using MaybeColl = std::optional<crimson::os::FuturizedStore::CollectionRef>;
  using FutureColl = seastar::shared_future<crimson::os::FuturizedStore::CollectionRef>;
  //using k_to_vlist_t = std::map<std::string, ceph::bufferlist>;

  using OptPair_KVlist = std::optional<std::pair<std::string, ceph::bufferlist>>;

  ::crimson::os::FuturizedStore* os;
  ghobject_t hoid;
  mutable FutureColl ch_fut;
  //MaybeColl maybe_ch;

  seastar::future<crimson::os::FuturizedStore::CollectionRef> get_collection() { return ch_fut.get_future(); }
  seastar::future<crimson::os::FuturizedStore::CollectionRef> get_collection() const { return ch_fut.get_future(); }

 public:


  class OSTransaction : public MapCacher::Transaction {
    friend class ::crimson::osd::OSDriver;

    coll_t cid;
    ghobject_t hoid;
    ::ceph::os::Transaction* t;

   public: // RRR fix that. Why do we need the
    OSTransaction(const coll_t& cid, const ghobject_t& hoid, ::ceph::os::Transaction* t)
	: cid(cid), hoid(hoid), t(t)
    {}

    OSTransaction(const OSTransaction& o) : cid{o.cid}, hoid{o.hoid}, t{o.t}{}
    OSTransaction(OSTransaction&& o) noexcept : cid{std::move(o.cid)}, hoid{std::move(o.hoid)}, t{o.t}{}

   public:
    void set_keys(const std::map<std::string, ceph::bufferlist>& to_set) override
    {
      t->omap_setkeys(cid, hoid, to_set);
    }

    void remove_keys(const std::set<std::string>& to_remove) override
    {
      t->omap_rmkeys(cid, hoid, to_remove);
    }

    void add_callback(Context* c) override
    {
      t->register_on_applied(c);
    }
  };

  struct next_by_key_t {


  };

  seastar::future<std::unique_ptr<OSTransaction>> get_transaction(::ceph::os::Transaction* t);

  /*OSTransaction get_transaction(::ceph::os::Transaction* t)
  {
    return OSTransaction(ch->get_cid(), hoid, t);
  }*/



  OSDriver(crimson::os::FuturizedStore* os, const coll_t& cid, const ghobject_t& hoid)
      : os(os), hoid(hoid), ch_fut{os->open_collection(cid)}
  {
  }

  os::FuturizedStore::read_errorator::future<MapCacher::k_to_vlist_t> get_keys(const std::set<std::string>& keys) override;


  auto get_next(const std::string& key) ->
  os::FuturizedStore::read_errorator::future<OptPair_KVlist> override;

  //int get_nextXXX(const std::string& key,
  //	       std::pair<std::string, ceph::bufferlist>* next) override { return 0; }
};


}  // namespace crimson::osd
