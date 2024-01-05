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

class MOSDScrubReserve : public MOSDFastDispatchOp {
private:
  static constexpr int HEAD_VERSION = 2;
  static constexpr int COMPAT_VERSION = 1;
public:
  spg_t pgid;
  epoch_t map_epoch;
  enum ReserveMsgOp {
    REQUEST = 0,
    GRANT = 1,
    RELEASE = 2,
    REJECT = 3,
  };
  int32_t type;
  pg_shard_t from;
  /// 'false' if the (legacy) primary is expecting an immediate
  /// granted / denied response
  bool wait_for_resources{false};

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
		   ReserveMsgOp type_code,
		   pg_shard_t from)
    : MOSDFastDispatchOp{MSG_OSD_SCRUB_RESERVE, HEAD_VERSION, COMPAT_VERSION},
      pgid(pgid), map_epoch(map_epoch),
      type(static_cast<int32_t>(type_code)), from(from) {}

  std::string_view get_type_name() const {
    return "MOSDScrubReserve";
  }

  void print(std::ostream& out) const {
    out << "MOSDScrubReserve(" << pgid << " ";
    switch (type) {
    case REQUEST:
      out << (wait_for_resources ? "QREQUEST " : "REQUEST ");
      break;
    case GRANT:
      out << "GRANT ";
      break;
    case REJECT:
      out << "REJECT ";
      break;
    case RELEASE:
      out << "RELEASE ";
      break;
    }
    out << "e" << map_epoch << ")";
    return;
  }

  void decode_payload() {
    using ceph::decode;
    auto p = payload.cbegin();
    decode(pgid, p);
    decode(map_epoch, p);
    decode(type, p);
    decode(from, p);
    if (header.version >= 2) {
      decode(wait_for_resources, p);
    } else {
      wait_for_resources = false;
    }
  }

  void encode_payload(uint64_t features) {
    using ceph::encode;
    encode(pgid, payload);
    encode(map_epoch, payload);
    encode(type, payload);
    encode(from, payload);
    encode(wait_for_resources, payload);
  }
private:
  template<class T, typename... Args>
  friend boost::intrusive_ptr<T> ceph::make_message(Args&&... args);
};

#endif
