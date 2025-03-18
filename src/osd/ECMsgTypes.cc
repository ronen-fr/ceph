// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Inktank Storage, Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */

#include "ECMsgTypes.h"

using std::list;
using std::make_pair;
using std::map;
using std::pair;
using std::set;
using ceph::bufferlist;
using ceph::Formatter;

using namespace std::literals;

void ECSubWrite::encode(bufferlist &bl) const
{
  ENCODE_START(4, 1, bl);
  encode(from, bl);
  encode(tid, bl);
  encode(reqid, bl);
  encode(soid, bl);
  encode(stats, bl);
  encode(t, bl);
  encode(at_version, bl);
  encode(trim_to, bl);
  encode(log_entries, bl);
  encode(temp_added, bl);
  encode(temp_removed, bl);
  encode(updated_hit_set_history, bl);
  encode(pg_committed_to, bl);
  encode(backfill_or_async_recovery, bl);
  ENCODE_FINISH(bl);
}

void ECSubWrite::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(4, bl);
  decode(from, bl);
  decode(tid, bl);
  decode(reqid, bl);
  decode(soid, bl);
  decode(stats, bl);
  decode(t, bl);
  decode(at_version, bl);
  decode(trim_to, bl);
  decode(log_entries, bl);
  decode(temp_added, bl);
  decode(temp_removed, bl);
  if (struct_v >= 2) {
    decode(updated_hit_set_history, bl);
  }
  if (struct_v >= 3) {
    decode(pg_committed_to, bl);
  } else {
    pg_committed_to = trim_to;
  }
  if (struct_v >= 4) {
    decode(backfill_or_async_recovery, bl);
  } else {
    // The old protocol used an empty transaction to indicate backfill or async_recovery
    backfill_or_async_recovery = t.empty();
  }
  DECODE_FINISH(bl);
}

std::ostream &operator<<(
  std::ostream &lhs, const ECSubWrite &rhs)
{
  lhs << "ECSubWrite(tid=" << rhs.tid
      << ", reqid=" << rhs.reqid
      << ", at_version=" << rhs.at_version
      << ", trim_to=" << rhs.trim_to
      << ", pg_committed_to=" << rhs.pg_committed_to;
  if (rhs.updated_hit_set_history)
    lhs << ", has_updated_hit_set_history";
  if (rhs.backfill_or_async_recovery)
    lhs << ", backfill_or_async_recovery";
  return lhs <<  ")";
}

void ECSubWrite::dump(Formatter *f) const
{
  f->dump_unsigned("tid", tid);
  f->dump_stream("reqid") << reqid;
  f->dump_stream("at_version") << at_version;
  f->dump_stream("trim_to") << trim_to;
  f->dump_stream("pg_committed_to") << pg_committed_to;
  f->dump_bool("has_updated_hit_set_history",
      static_cast<bool>(updated_hit_set_history));
  f->dump_bool("backfill_or_async_recovery", backfill_or_async_recovery);
}

void ECSubWrite::generate_test_instances(list<ECSubWrite*> &o)
{
  o.push_back(new ECSubWrite());
  o.back()->tid = 1;
  o.back()->at_version = eversion_t(2, 100);
  o.back()->trim_to = eversion_t(1, 40);
  o.push_back(new ECSubWrite());
  o.back()->tid = 4;
  o.back()->reqid = osd_reqid_t(entity_name_t::CLIENT(123), 1, 45678);
  o.back()->at_version = eversion_t(10, 300);
  o.back()->trim_to = eversion_t(5, 42);
  o.push_back(new ECSubWrite());
  o.back()->tid = 9;
  o.back()->reqid = osd_reqid_t(entity_name_t::CLIENT(123), 1, 45678);
  o.back()->at_version = eversion_t(10, 300);
  o.back()->trim_to = eversion_t(5, 42);
  o.back()->pg_committed_to = eversion_t(8, 250);
}

void ECSubWriteReply::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(from, bl);
  encode(tid, bl);
  encode(last_complete, bl);
  encode(committed, bl);
  encode(applied, bl);
  ENCODE_FINISH(bl);
}

void ECSubWriteReply::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(from, bl);
  decode(tid, bl);
  decode(last_complete, bl);
  decode(committed, bl);
  decode(applied, bl);
  DECODE_FINISH(bl);
}

std::ostream &operator<<(
  std::ostream &lhs, const ECSubWriteReply &rhs)
{
  return lhs
    << "ECSubWriteReply(tid=" << rhs.tid
    << ", last_complete=" << rhs.last_complete
    << ", committed=" << rhs.committed
    << ", applied=" << rhs.applied << ")";
}

void ECSubWriteReply::dump(Formatter *f) const
{
  f->dump_unsigned("tid", tid);
  f->dump_stream("last_complete") << last_complete;
  f->dump_bool("committed", committed);
  f->dump_bool("applied", applied);
}

void ECSubWriteReply::generate_test_instances(list<ECSubWriteReply*>& o)
{
  o.push_back(new ECSubWriteReply());
  o.back()->tid = 20;
  o.back()->last_complete = eversion_t(100, 2000);
  o.back()->committed = true;
  o.push_back(new ECSubWriteReply());
  o.back()->tid = 80;
  o.back()->last_complete = eversion_t(50, 200);
  o.back()->applied = true;
}

void ECSubRead::encode(bufferlist &bl, uint64_t features) const
{
  if ((features & CEPH_FEATURE_OSD_FADVISE_FLAGS) == 0) {
    ENCODE_START(2, 1, bl);
    encode(from, bl);
    encode(tid, bl);
    map<hobject_t, list<pair<uint64_t, uint64_t> >> tmp;
    for (auto m = to_read.cbegin(); m != to_read.cend(); ++m) {
      list<pair<uint64_t, uint64_t> > tlist;
      for (auto l = m->second.cbegin(); l != m->second.cend(); ++l) {
	tlist.push_back(std::make_pair(l->get<0>(), l->get<1>()));
      }
      tmp[m->first] = tlist;
    }
    encode(tmp, bl);
    encode(attrs_to_read, bl);
    encode(subchunks, bl);
    ENCODE_FINISH(bl);
    return;
  }

  ENCODE_START(3, 2, bl);
  encode(from, bl);
  encode(tid, bl);
  encode(to_read, bl);
  encode(attrs_to_read, bl);
  encode(subchunks, bl);
  ENCODE_FINISH(bl);
}

void ECSubRead::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(3, bl);
  decode(from, bl);
  decode(tid, bl);
  if (struct_v == 1) {
    map<hobject_t, list<pair<uint64_t, uint64_t> >>tmp;
    decode(tmp, bl);
    for (auto m = tmp.cbegin(); m != tmp.cend(); ++m) {
      list<boost::tuple<uint64_t, uint64_t, uint32_t> > tlist;
      for (auto l = m->second.cbegin(); l != m->second.cend(); ++l) {
	tlist.push_back(boost::make_tuple(l->first, l->second, 0));
      }
      to_read[m->first] = tlist;
    }
  } else {
    decode(to_read, bl);
  }
  decode(attrs_to_read, bl);
  if (struct_v > 2 && struct_v > struct_compat) {
    decode(subchunks, bl);
  } else {
    for (auto &i : to_read) {
      subchunks[i.first].push_back(make_pair(0, 1));
    }
  }
  DECODE_FINISH(bl);
}

std::ostream &operator<<(
  std::ostream &lhs, const ECSubRead &rhs)
{
  return lhs
    << "ECSubRead(tid=" << rhs.tid
    << ", to_read=" << rhs.to_read
    << ", subchunks=" << rhs.subchunks
    << ", attrs_to_read=" << rhs.attrs_to_read << ")";
}

void ECSubRead::dump(Formatter *f) const
{
  f->dump_stream("from") << from;
  f->dump_unsigned("tid", tid);
  f->open_array_section("objects");
  for (auto i = to_read.cbegin(); i != to_read.cend(); ++i) {
    f->open_object_section("object");
    f->dump_stream("oid") << i->first;
    f->open_array_section("extents");
    for (auto j = i->second.cbegin(); j != i->second.cend(); ++j) {
      f->open_object_section("extent");
      f->dump_unsigned("off", j->get<0>());
      f->dump_unsigned("len", j->get<1>());
      f->dump_unsigned("flags", j->get<2>());
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("object_attrs_requested");
  for (auto i = attrs_to_read.cbegin(); i != attrs_to_read.cend(); ++i) {
    f->open_object_section("object");
    f->dump_stream("oid") << *i;
    f->close_section();
  }
  f->close_section();
}

void ECSubRead::generate_test_instances(list<ECSubRead*>& o)
{
  hobject_t hoid1(sobject_t("asdf", 1));
  hobject_t hoid2(sobject_t("asdf2", CEPH_NOSNAP));
  o.push_back(new ECSubRead());
  o.back()->from = pg_shard_t(2, shard_id_t(-1));
  o.back()->tid = 1;
  o.back()->to_read[hoid1].push_back(boost::make_tuple(100, 200, 0));
  o.back()->to_read[hoid1].push_back(boost::make_tuple(400, 600, 0));
  o.back()->to_read[hoid2].push_back(boost::make_tuple(400, 600, 0));
  o.back()->attrs_to_read.insert(hoid1);
  o.push_back(new ECSubRead());
  o.back()->from = pg_shard_t(2, shard_id_t(-1));
  o.back()->tid = 300;
  o.back()->to_read[hoid1].push_back(boost::make_tuple(300, 200, 0));
  o.back()->to_read[hoid2].push_back(boost::make_tuple(400, 600, 0));
  o.back()->to_read[hoid2].push_back(boost::make_tuple(2000, 600, 0));
  o.back()->attrs_to_read.insert(hoid2);
}

void ECSubReadReply::encode(bufferlist &bl) const
{
  ENCODE_START(1, 1, bl);
  encode(from, bl);
  encode(tid, bl);
  encode(buffers_read, bl);
  encode(attrs_read, bl);
  encode(errors, bl);
  ENCODE_FINISH(bl);
}

void ECSubReadReply::decode(bufferlist::const_iterator &bl)
{
  DECODE_START(1, bl);
  decode(from, bl);
  decode(tid, bl);
  decode(buffers_read, bl);
  decode(attrs_read, bl);
  decode(errors, bl);
  DECODE_FINISH(bl);
}

std::ostream &operator<<(
  std::ostream &lhs, const ECSubReadReply &rhs)
{
  return lhs
    << "ECSubReadReply(tid=" << rhs.tid
    << ", attrs_read=" << rhs.attrs_read.size()
    << ")";
}

#if 0

/*

  template<typename Callback, typename...Args>
  decltype(auto) with_osdmap(Callback&& cb, Args&&... args) const {
    std::shared_lock l(rwlock);
    return std::forward<Callback>(cb)(*osdmap, std::forward<Args>(args)...);
  }
*/
  //template<typename Container, typename...Args>
  template <typename K, typename V, typename FN, typename...Args>
  //require container is a map
  void /*decltype(auto)*/ with_array_section(Formatter* f, std::string_view txt, const map<K,V>& m, FN&& fn, Args&&... args) {
    Formatter::ArraySection as(*f, txt);
    for (const auto& [k, v] : m) {
      std::forward<FN>(fn)(f, k, v, std::forward<Args>(args)...);
    }
  }

  template <typename CONT, typename V, typename FN, typename...Args> // V is the value_type of CONT ?
  //require container is a map
  void /*decltype(auto)*/ with_array_section(Formatter* f, std::string_view txt, const CONT& m, FN&& fn, Args&&... args) {
    Formatter::ArraySection as(*f, txt);
    for (const auto& v : m) {
      std::forward<FN>(fn)(f, v, std::forward<Args>(args)...);
    }
  }

  // map of <txt> to <object>
  template <typename K, typename V, typename FN, typename...Args>
  void with_objects_array_section(Formatter* f, std::string_view arr_txt, std::string_view obj_txt, /*fn to get object name*/ map<K,V>& m, FN&& fn, Args&&... args) {
    Formatter::ArraySection as(*f, arr_txt);
    for (const auto& [k, v] : m) {
      Formatter::ObjectSection os(*f, obj_txt);
      std::forward<FN>(fn)(f, k, v, std::forward<Args>(args)...);
    }
  }


#endif

void ECSubReadReply::dump(Formatter* f) const
{
  using offset_pair_t = pair<uint64_t, bufferlist>;
  using extents_list_t = list<offset_pair_t>;


  // an extent is an element in the list of extents, i.e. a pair of <offset, bl>
  auto extent_dump = [](Formatter* f, const offset_pair_t& offset_n_bl,
			std::string_view mg) mutable -> void {
    const auto& [off, bl] = offset_n_bl;
    Formatter::ObjectSection os(*f, "extent");	// mg
    f->dump_unsigned("off", off);
    f->dump_unsigned("buf_len", bl.length());
  };

  auto extent_dump3 = [](Formatter* f, const offset_pair_t& offset_n_bl,
			std::string_view mg) mutable -> void {
    const auto& [off, bl] = offset_n_bl;
    f->dump_unsigned("off", off);
    f->dump_unsigned("buf_len", bl.length());
  };

  auto buff_dump = [extent_dump](
		       Formatter* f, const hobject_t& oid,
		       const extents_list_t& l, std::string_view mg) mutable -> void {
    f->dump_stream("oid") << oid;
    f->with_array_section<
	const extents_list_t,
	decltype(extent_dump)>	// mg
	("data", l, std::move(extent_dump));
  };

  auto buff_dump2 = [extent_dump](
		       Formatter* f, const hobject_t& oid,
		       const extents_list_t& l, std::string_view mg) mutable -> void {
    f->dump_stream("oid") << oid;
    f->with_array_section<
	const extents_list_t,
	decltype(extent_dump)>	// mg
	("data", l, std::move(extent_dump));
  };

  auto buff_dump3 = [extent_dump3](
		       Formatter* f, const hobject_t& oid,
		       const extents_list_t& l, std::string_view mg) mutable -> void {
    f->dump_stream("oid") << oid;
    f->with_obj_array_section("data", l, std::move(extent_dump3));
  };

  f->dump_stream("from") << from;
  f->dump_unsigned("tid", tid);

//   Formatter::ArraySection as(*f, "buffers_read", buffers_read, [](Formatter *f, std::pair<hobject_t, list<pair<uint64_t, bufferlist> > > const& p) {
//     f->open_object_section("object");
//     f->dump_stream("oid") << p.first;
//     Formatter::ArraySection as2(*f, "extents", p.second, [](Formatter *f, std::pair<uint64_t, bufferlist> const& p) {
//       f->open_object_section("extent");
//       f->dump_unsigned("off", p.first);
//       f->dump_unsigned("buf_len", p.second.length());
//       f->close_section();
//     });
//     f->open_array_section("");
//     f->dump_stream("oid") << oid;
//   });
  f->with_obj_array_section("buffers_read", buffers_read, buff_dump2);

  f->with_obj_array_section("buffers_read", buffers_read, buff_dump3);

  f->with_array_section("buffers_read", buffers_read,
      [buff_dump](Formatter* f, const hobject_t& oid, const extents_list_t& l, std::string_view mg) mutable {
        buff_dump(f, oid, l, mg);
      });


  f->with_obj_array_section("buffers_read", buffers_read,
      [buff_dump](Formatter* f, const hobject_t& oid, const extents_list_t& l, std::string_view mg) mutable {
        buff_dump(f, oid, l, mg);
      });

//   f->with_array_section("buffers_read", buffers_read, [](Formatter *f, const hobject_t& oid, const list<pair<uint64_t, bufferlist>>& l,
//         std::string_view mg) mutable {
//     f->with_object_section("object"sv, oid, l, [](Formatter *f, const hobject_t& oid, const list<pair<uint64_t, bufferlist>>& l, std::string_view mg) mutable -> void  {
//       f->dump_stream("oid") << oid;
//       f->with_array_section("extents", l, [](Formatter *f, uint64_t off, bufferlist const& bl, std::string_view mg) mutable {
//         f->open_object_section("extent");
//         f->dump_unsigned("off", off);
//         f->dump_unsigned("buf_len", bl.length());
//         f->close_section();
//       });
//     });
//     f->open_object_section("object");
//     f->dump_stream("oid") << oid;
//     f->close_section();
// //     f->with_array_section("extents", l, [](Formatter *f, uint64_t off, bufferlist const& bl) {
// //       f->open_object_section("extent");
// //       f->dump_unsigned("off", off);
// //       f->dump_unsigned("buf_len", bl.length());
// //       f->close_section();
// //     });
// //     f->close_section();
//   });

  f->open_array_section("buffers_read");
  for (auto i = buffers_read.cbegin(); i != buffers_read.cend(); ++i) {
    f->open_object_section("object");
    f->dump_stream("oid") << i->first;
    f->open_array_section("data");
    for (auto j = i->second.cbegin(); j != i->second.cend(); ++j) {
      f->open_object_section("extent");
      f->dump_unsigned("off", j->first);
      f->dump_unsigned("buf_len", j->second.length());
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("attrs_returned");
  for (auto i = attrs_read.cbegin(); i != attrs_read.cend(); ++i) {
    f->open_object_section("object_attrs");
    f->dump_stream("oid") << i->first;
    f->open_array_section("attrs");
    for (auto j = i->second.cbegin(); j != i->second.cend(); ++j) {
      f->open_object_section("attr");
      f->dump_string("attr", j->first);
      f->dump_unsigned("val_len", j->second.length());
      f->close_section();
    }
    f->close_section();
    f->close_section();
  }
  f->close_section();

  f->open_array_section("errors");
  for (auto i = errors.cbegin(); i != errors.cend(); ++i) {
    f->open_object_section("error_pair");
    f->dump_stream("oid") << i->first;
    f->dump_int("error", i->second);
    f->close_section();
  }
  f->close_section();
}

void ECSubReadReply::generate_test_instances(list<ECSubReadReply*>& o)
{
  hobject_t hoid1(sobject_t("asdf", 1));
  hobject_t hoid2(sobject_t("asdf2", CEPH_NOSNAP));
  bufferlist bl;
  bl.append_zero(100);
  bufferlist bl2;
  bl2.append_zero(200);
  o.push_back(new ECSubReadReply());
  o.back()->from = pg_shard_t(2, shard_id_t(-1));
  o.back()->tid = 1;
  o.back()->buffers_read[hoid1].push_back(make_pair(20, bl));
  o.back()->buffers_read[hoid1].push_back(make_pair(2000, bl2));
  o.back()->buffers_read[hoid2].push_back(make_pair(0, bl));
  o.back()->attrs_read[hoid1]["foo"] = bl;
  o.back()->attrs_read[hoid1]["_"] = bl2;
  o.push_back(new ECSubReadReply());
  o.back()->from = pg_shard_t(2, shard_id_t(-1));
  o.back()->tid = 300;
  o.back()->buffers_read[hoid2].push_back(make_pair(0, bl2));
  o.back()->attrs_read[hoid2]["foo"] = bl;
  o.back()->attrs_read[hoid2]["_"] = bl2;
  o.back()->errors[hoid1] = -2;
}
