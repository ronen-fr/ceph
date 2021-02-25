// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2004-2006 Sage Weil <sage@newdream.net>
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#ifndef CEPH_MOSDSCRUBRESERVE_H
#define CEPH_MOSDSCRUBRESERVE_H

#include "MOSDFastDispatchOp.h"
#include <sstream>
#include <string_view>
#include <string>

class MOSDScrubReserve : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 1;
  static constexpr int COMPAT_VERSION = 1;
public:
  spg_t pgid;
  epoch_t map_epoch;
  enum {
    REQUEST = 0,
    GRANT = 1,
    RELEASE = 2,
    REJECT = 3,
  };
  inline static constexpr string_view req_names[4] = {"request"sv, "grant"sv,
                                                      "release"sv, "reject"sv};
  int32_t type;
  pg_shard_t from;

  epoch_t get_map_epoch() const override {
    return map_epoch;
  }
  spg_t get_spg() const override {
    return pgid;
  }

  MOSDScrubReserve()
    : MOSDFastDispatchOp{MSG_OSD_SCRUB_RESERVE, HEAD_VERSION, COMPAT_VERSION},
      map_epoch(0), type(-1) {}

  MOSDScrubReserve(spg_t pgid,
		   epoch_t map_epoch,
		   int type,
		   pg_shard_t from)
    : MOSDFastDispatchOp{MSG_OSD_SCRUB_RESERVE, HEAD_VERSION, COMPAT_VERSION},
      pgid(pgid), map_epoch(map_epoch),
      type(type), from(from) {}

  std::string_view get_type_name() const {
    return "MOSDScrubReserve";
  }

  std::string_view get_specific_req_name() const {
    return req_names[type];
  }

  std::string get_desc() const {
    std::stringstream s;
    print(s);
    return s.str();
  }

  void print(std::ostream& out) const {
    out << "MOSDScrubReserve(" << pgid << " " << get_specific_req_name() <<
      " e" << map_epoch << ")";
  }

  void decode_payload() final {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(pgid, p);
    decode(map_epoch, p);
    decode(type, p);
    decode(from, p);
  }

  void encode_payload(uint64_t features) final {
    using ceph::encode;
    encode(pgid, payload);
    encode(map_epoch, payload);
    encode(type, payload);
    encode(from, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
