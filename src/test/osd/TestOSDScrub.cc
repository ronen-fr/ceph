// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2013 Cloudwatt <libre.licensing@cloudwatt.com>
 *
 * Author: Loic Dachary <loic@dachary.org>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library Public License as published by
 * the Free Software Foundation; either version 2, or (at your option)
 * any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library Public License for more details.
 *
 */

#include <stdio.h>
#include <signal.h>
#include <gtest/gtest.h>
#include "common/async/context_pool.h"
#include "osd/OSD.h"
#include "os/ObjectStore.h"
#include "mon/MonClient.h"
#include "common/ceph_argparse.h"
#include "msg/Messenger.h"

class TestOSDScrub: public OSD {

public:
  TestOSDScrub(CephContext *cct_,
      std::unique_ptr<ObjectStore> store_,
      int id,
      Messenger *internal,
      Messenger *external,
      Messenger *hb_front_client,
      Messenger *hb_back_client,
      Messenger *hb_front_server,
      Messenger *hb_back_server,
      Messenger *osdc_messenger,
      MonClient *mc, const std::string &dev, const std::string &jdev,
      ceph::async::io_context_pool& ictx) :
      OSD(cct_, std::move(store_), id, internal, external,
	  hb_front_client, hb_back_client,
	  hb_front_server, hb_back_server,
	  osdc_messenger, mc, dev, jdev, ictx)
  {
  }

  bool scrub_time_permit(utime_t now) {
    return service.get_scrub_services().scrub_time_permit(now);
  }
};

TEST(TestOSDScrub, scrub_time_permit) {
  ceph::async::io_context_pool icp(1);
  std::unique_ptr<ObjectStore> store = ObjectStore::create(g_ceph_context,
             g_conf()->osd_objectstore,
             g_conf()->osd_data,
             g_conf()->osd_journal);
  std::string cluster_msgr_type = g_conf()->ms_cluster_type.empty() ? g_conf().get_val<std::string>("ms_type") : g_conf()->ms_cluster_type;
  Messenger *ms = Messenger::create(g_ceph_context, cluster_msgr_type,
				    entity_name_t::OSD(0), "make_checker",
				    getpid());
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms->set_default_policy(Messenger::Policy::stateless_server(0));
  ms->bind(g_conf()->public_addr);
  MonClient mc(g_ceph_context, icp);
  mc.build_initial_monmap();
  TestOSDScrub* osd = new TestOSDScrub(g_ceph_context, std::move(store), 0, ms, ms, ms, ms, ms, ms, ms, &mc, "", "", icp);

  // These are now invalid
  int err = g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "24");
  ASSERT_TRUE(err < 0);
  //GTEST_LOG_(INFO) << " osd_scrub_begin_hour = " << g_ceph_context->_conf.get_val<int64_t>("osd_scrub_begin_hour");

  err = g_ceph_context->_conf.set_val("osd_scrub_end_hour", "24");
  ASSERT_TRUE(err < 0);
  //GTEST_LOG_(INFO) << " osd_scrub_end_hour = " << g_ceph_context->_conf.get_val<int64_t>("osd_scrub_end_hour");

  err = g_ceph_context->_conf.set_val("osd_scrub_begin_week_day", "7");
  ASSERT_TRUE(err < 0);
  //GTEST_LOG_(INFO) << " osd_scrub_begin_week_day = " << g_ceph_context->_conf.get_val<int64_t>("osd_scrub_begin_week_day");

  err = g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "7");
  ASSERT_TRUE(err < 0);
  //GTEST_LOG_(INFO) << " osd_scrub_end_week_day = " << g_ceph_context->_conf.get_val<int64_t>("osd_scrub_end_week_day");

  // Test all day
  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "0");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "0");
  g_ceph_context->_conf.apply_changes(nullptr);
  tm tm;
  tm.tm_isdst = -1;
  strptime("2015-01-16 12:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  utime_t now = utime_t(mktime(&tm), 0);
  bool ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 01:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 20:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "20");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 08:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 20:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 00:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  g_ceph_context->_conf.set_val("osd_scrub_begin_hour", "01");
  g_ceph_context->_conf.set_val("osd_scrub_end_hour", "07");
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  // Sun = 0, Mon = 1, Tue = 2, Wed = 3, Thu = 4m, Fri = 5, Sat = 6
  // Jan 16, 2015 is a Friday (5)
  // every day
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "0"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "0"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  // test Sun - Thu
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "0"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "5"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);

  // test Fri - Sat
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "5"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "0"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-16 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  // Jan 14, 2015 is a Wednesday (3)
  // test Tue - Fri
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "2"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "6"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-14 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_TRUE(ret);

  // Test Sat - Sun
  g_ceph_context->_conf.set_val("osd_scrub_begin_week day", "6"); // inclusive
  g_ceph_context->_conf.set_val("osd_scrub_end_week_day", "1"); // not inclusive
  g_ceph_context->_conf.apply_changes(nullptr);
  strptime("2015-01-14 04:05:13", "%Y-%m-%d %H:%M:%S", &tm);
  now = utime_t(mktime(&tm), 0);
  ret = osd->scrub_time_permit(now);
  ASSERT_FALSE(ret);
}


#include "messages/MOSDRepScrubMap.h"







#include "messages/MOSDRepScrubMap.h"

// reef constructs

struct SMRR {
  struct object {
    std::map<std::string, ceph::buffer::ptr, std::less<>> attrs;
    uint64_t size;
    __u32 omap_digest;         ///< omap crc32c
    __u32 digest;              ///< data crc32c
    bool negative:1;
    bool digest_present:1;
    bool omap_digest_present:1;
    bool read_error:1;
    bool stat_error:1;
    bool ec_hash_mismatch:1;
    bool ec_size_mismatch:1;
    bool large_omap_object_found:1;
    uint64_t large_omap_object_key_count = 0;
    uint64_t large_omap_object_value_size = 0;
    uint64_t object_omap_bytes = 0;
    uint64_t object_omap_keys = 0;

    object() :
      // Init invalid size so it won't match if we get a stat EIO error
      size(-1), omap_digest(0), digest(0),
      negative(false), digest_present(false), omap_digest_present(false),
      read_error(false), stat_error(false), ec_hash_mismatch(false),
      ec_size_mismatch(false), large_omap_object_found(false) {}

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<object*>& o);
    //static std::list<object*> generate_test_instances();
  };
  WRITE_CLASS_ENCODER(object)

  std::map<hobject_t,object> objects;
  eversion_t valid_through;
  eversion_t incr_since;
  bool has_large_omap_object_errors{false};
  bool has_omap_keys{false};

  void merge_incr(const SMRR &l);
  void clear_from(const hobject_t& start) {
    objects.erase(objects.lower_bound(start), objects.end());
  }
  void insert(const SMRR &r) {
    objects.insert(r.objects.begin(), r.objects.end());
  }
  void swap(SMRR &r) {
    using std::swap;
    swap(objects, r.objects);
    swap(valid_through, r.valid_through);
    swap(incr_since, r.incr_since);
    swap(has_large_omap_object_errors, r.has_large_omap_object_errors);
    swap(has_omap_keys, r.has_omap_keys);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl, int64_t pool=-1);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<SMRR*>& o);
  static std::list<SMRR*> generate_test_instances();  // RRR added
};
WRITE_CLASS_ENCODER(SMRR::object)
WRITE_CLASS_ENCODER(SMRR)

void SMRR::merge_incr(const SMRR &l)
{
  ceph_assert(valid_through == l.incr_since);
  valid_through = l.valid_through;

  for (auto p = l.objects.cbegin(); p != l.objects.cend(); ++p){
    if (p->second.negative) {
      auto q = objects.find(p->first);
      if (q != objects.end()) {
	objects.erase(q);
      }
    } else {
      objects[p->first] = p->second;
    }
  }
}


#include <algorithm>
#include <list>
#include <map>
#include <ostream>
#include <sstream>
#include <set>
#include <string>
#include <utility>
#include <vector>


#include <boost/assign/list_of.hpp>

#include "include/ceph_features.h"
#include "include/encoding.h"
#include "include/stringify.h"
extern "C" {
#include "crush/hash.h"
}

#include "common/Formatter.h"
#include "common/StackStringStream.h"
#include "include/utime_fmt.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
//#include "osd_types_fmt.h"
#include "os/Transaction.h"

using std::list;
using std::make_pair;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::decode_nohead;
using ceph::encode;
using ceph::encode_nohead;
using ceph::Formatter;
using ceph::make_timespan;
using ceph::JSONFormatter;

using namespace std::literals;

void SMRR::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(3, 2, bl);
  encode(objects, bl);
  encode((__u32)0, bl); // used to be attrs; now deprecated
  ceph::buffer::list old_logbl;  // not used
  encode(old_logbl, bl);
  encode(valid_through, bl);
  encode(incr_since, bl);
  ENCODE_FINISH(bl);
}

void SMRR::decode(ceph::buffer::list::const_iterator& bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(objects, bl);
  {
    map<string,string> attrs;  // deprecated
    decode(attrs, bl);
  }
  ceph::buffer::list old_logbl;   // not used
  decode(old_logbl, bl);
  decode(valid_through, bl);
  decode(incr_since, bl);
  DECODE_FINISH(bl);

  // handle hobject_t upgrade
  if (struct_v < 3) {
    map<hobject_t, object> tmp;
    tmp.swap(objects);
    for (auto i = tmp.begin(); i != tmp.end(); ++i) {
      hobject_t first(i->first);
      if (!first.is_max() && first.pool == -1)
	first.pool = pool;
      objects[first] = i->second;
    }
  }
}

void SMRR::dump(Formatter *f) const
{
  f->dump_stream("valid_through") << valid_through;
  f->dump_stream("incremental_since") << incr_since;
  f->open_array_section("objects");
  for (auto p = objects.cbegin(); p != objects.cend(); ++p) {
    f->open_object_section("object");
    f->dump_string("name", p->first.oid.name);
    f->dump_unsigned("hash", p->first.get_hash());
    f->dump_string("key", p->first.get_key());
    f->dump_int("snapid", p->first.snap);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void SMRR::generate_test_instances(list<SMRR*>& o)
{
  o.push_back(new SMRR);
  o.push_back(new SMRR);
  o.back()->valid_through = eversion_t(1, 2);
  o.back()->incr_since = eversion_t(3, 4);
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0, "")] = *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0, "")] = *obj.back();
}

list<SMRR*> SMRR::generate_test_instances()
{
  list<SMRR*> o;
  //o.push_back(new SMRR);
  o.push_back(new SMRR);
  o.back()->valid_through = eversion_t(1, 2);
  o.back()->incr_since = eversion_t(3, 4);
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0, "")] = *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0, "")] = *obj.back();
  return o;
}

// -- SMRR::object --

void SMRR::object::encode(ceph::buffer::list& bl) const
{
  bool compat_read_error = read_error || ec_hash_mismatch || ec_size_mismatch;
  ENCODE_START(10, 7, bl);
  encode(size, bl);
  encode(negative, bl);
  encode(attrs, bl);
  encode(digest, bl);
  encode(digest_present, bl);
  encode((uint32_t)0, bl);  // obsolete nlinks
  encode((uint32_t)0, bl);  // snapcolls
  encode(omap_digest, bl);
  encode(omap_digest_present, bl);
  encode(compat_read_error, bl);
  encode(stat_error, bl);
  encode(read_error, bl);
  encode(ec_hash_mismatch, bl);
  encode(ec_size_mismatch, bl);
  encode(large_omap_object_found, bl);
  encode(large_omap_object_key_count, bl);
  encode(large_omap_object_value_size, bl);
  encode(object_omap_bytes, bl);
  encode(object_omap_keys, bl);
  ENCODE_FINISH(bl);
}

void SMRR::object::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(10, bl);
  decode(size, bl);
  bool tmp, compat_read_error = false;
  decode(tmp, bl);
  negative = tmp;
  decode(attrs, bl);
  decode(digest, bl);
  decode(tmp, bl);
  digest_present = tmp;
  {
    uint32_t nlinks;
    decode(nlinks, bl);
    set<snapid_t> snapcolls;
    decode(snapcolls, bl);
  }
  decode(omap_digest, bl);
  decode(tmp, bl);
  omap_digest_present = tmp;
  decode(compat_read_error, bl);
  decode(tmp, bl);
  stat_error = tmp;
  if (struct_v >= 8) {
    decode(tmp, bl);
    read_error = tmp;
    decode(tmp, bl);
    ec_hash_mismatch = tmp;
    decode(tmp, bl);
    ec_size_mismatch = tmp;
  }
  // If older encoder found a read_error, set read_error
  if (compat_read_error && !read_error && !ec_hash_mismatch && !ec_size_mismatch)
    read_error = true;
  if (struct_v >= 9) {
    decode(tmp, bl);
    large_omap_object_found = tmp;
    decode(large_omap_object_key_count, bl);
    decode(large_omap_object_value_size, bl);
  }
  if (struct_v >= 10) {
    decode(object_omap_bytes, bl);
    decode(object_omap_keys, bl);
  }
  DECODE_FINISH(bl);
}

void SMRR::object::dump(Formatter *f) const
{
  f->dump_int("size", size);
  f->dump_int("negative", negative);
  f->open_array_section("attrs");
  for (auto p = attrs.cbegin(); p != attrs.cend(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();
}

void SMRR::object::generate_test_instances(list<object*>& o)
{
  o.push_back(new object);
  o.push_back(new object);
  o.back()->negative = true;
  o.push_back(new object);
  o.back()->size = 123;
  o.back()->attrs["foo"] = ceph::buffer::copy("foo", 3);
  o.back()->attrs["bar"] = ceph::buffer::copy("barval", 6);
}

namespace fmt {
template <>
struct formatter<SMRR::object> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  ///\todo: consider passing the 'D" flag to control snapset dump
  template <typename FormatContext>
  auto format(const SMRR::object& so, FormatContext& ctx)
  {
    fmt::format_to(ctx.out(),
		   "so{{ sz:{} dd:{} od:{} ",
		   so.size,
		   so.digest,
		   so.digest_present);

    // note the special handling of (1) OI_ATTR and (2) non-printables
    for (auto [k, v] : so.attrs) {
      std::string bkstr{v.raw_c_str(), v.raw_length()};
      if (k == std::string{OI_ATTR}) {
	/// \todo consider parsing the OI args here. Maybe add a specific format
	/// specifier
	fmt::format_to(ctx.out(), "{{{}:<<OI_ATTR>>({})}} ", k, bkstr.length());
      } else if (k == std::string{SS_ATTR}) {
	bufferlist bl;
	bl.push_back(v);
	SnapSet sns{bl};
	fmt::format_to(ctx.out(), "{{{}:{:D}}} ", k, sns);
      } else {
	fmt::format_to(ctx.out(), "{{{}:{}({})}} ", k, bkstr, bkstr.length());
      }
    }

    return fmt::format_to(ctx.out(), "}}");
  }
};

template <>
struct fmt::formatter<SMRR> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 'D') {
      debug_log = true;	 // list the objects
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const SMRR& smap, FormatContext& ctx)
  {
    fmt::format_to(ctx.out(),
		   "smap{{ valid:{} incr-since:{} #:{}",
		   smap.valid_through,
		   smap.incr_since,
		   smap.objects.size());
    if (debug_log) {
      fmt::format_to(ctx.out(), " objects:");
      for (const auto& [ho, so] : smap.objects) {
	fmt::format_to(ctx.out(), "\n\th.o<{}>:<{}> ", ho, so);
      }
      fmt::format_to(ctx.out(), "\n");
    }
    return fmt::format_to(ctx.out(), "}}");
  }

  bool debug_log{false};
};

}





// -----------------------------------   modified reef constructs

struct SM_REEF_MODIFIED {
  struct object {
    std::map<std::string, ceph::buffer::ptr, std::less<>> attrs;
    uint64_t size;//{0};
    __u32 omap_digest;         ///< omap crc32c
    __u32 digest;              ///< data crc32c
    bool negative:1;
    bool digest_present:1;
    bool omap_digest_present:1{false};
    bool read_error:1;
    bool stat_error:1;
    bool ec_hash_mismatch:1;
    bool ec_size_mismatch:1;
    bool large_omap_object_found:1;
    uint64_t large_omap_object_key_count = 0;
    uint64_t large_omap_object_value_size = 0;
    uint64_t object_omap_bytes = 0;
    uint64_t object_omap_keys = 0;

    object() :
      // Init invalid size so it won't match if we get a stat EIO error
      size(-1), omap_digest(0), digest(0),
      negative(false), digest_present(false), omap_digest_present(false),
      read_error(false), stat_error(false), ec_hash_mismatch(false),
      ec_size_mismatch(false), large_omap_object_found(false) {}

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<object*>& o);
    //static std::list<object*> generate_test_instances();
  };
  WRITE_CLASS_ENCODER(object)

  std::map<hobject_t,object> objects;
  eversion_t valid_through;
  eversion_t incr_since;
  bool has_large_omap_object_errors{false};
  bool has_omap_keys{false};

  void merge_incr(const SM_REEF_MODIFIED &l);
  void clear_from(const hobject_t& start) {
    objects.erase(objects.lower_bound(start), objects.end());
  }
  void insert(const SM_REEF_MODIFIED &r) {
    objects.insert(r.objects.begin(), r.objects.end());
  }
  void swap(SM_REEF_MODIFIED &r) {
    using std::swap;
    swap(objects, r.objects);
    swap(valid_through, r.valid_through);
    swap(incr_since, r.incr_since);
    swap(has_large_omap_object_errors, r.has_large_omap_object_errors);
    swap(has_omap_keys, r.has_omap_keys);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl, int64_t pool=-1);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<SM_REEF_MODIFIED*>& o);
  static std::list<SM_REEF_MODIFIED*> generate_test_instances();
};
WRITE_CLASS_ENCODER(SM_REEF_MODIFIED::object)
WRITE_CLASS_ENCODER(SM_REEF_MODIFIED)

void SM_REEF_MODIFIED::merge_incr(const SM_REEF_MODIFIED &l)
{
  ceph_assert(valid_through == l.incr_since);
  valid_through = l.valid_through;

  for (auto p = l.objects.cbegin(); p != l.objects.cend(); ++p){
    if (p->second.negative) {
      auto q = objects.find(p->first);
      if (q != objects.end()) {
	objects.erase(q);
      }
    } else {
      objects[p->first] = p->second;
    }
  }
}


#include <algorithm>
#include <list>
#include <map>
#include <ostream>
#include <sstream>
#include <set>
#include <string>
#include <utility>
#include <vector>


#include <boost/assign/list_of.hpp>

#include "include/ceph_features.h"
#include "include/encoding.h"
#include "include/stringify.h"
extern "C" {
#include "crush/hash.h"
}

#include "common/Formatter.h"
#include "common/StackStringStream.h"
#include "include/utime_fmt.h"
#include "osd/OSDMap.h"
#include "osd/osd_types.h"
//#include "osd_types_fmt.h"
#include "os/Transaction.h"

using std::list;
using std::make_pair;
using std::map;
using std::ostream;
using std::pair;
using std::set;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;

using ceph::bufferlist;
using ceph::decode;
using ceph::decode_nohead;
using ceph::encode;
using ceph::encode_nohead;
using ceph::Formatter;
using ceph::make_timespan;
using ceph::JSONFormatter;

using namespace std::literals;

void SM_REEF_MODIFIED::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(3, 2, bl);
  encode(objects, bl);
  encode((__u32)0, bl); // used to be attrs; now deprecated
  ceph::buffer::list old_logbl;  // not used
  encode(old_logbl, bl);
  encode(valid_through, bl);
  encode(incr_since, bl);
  ENCODE_FINISH(bl);
}

void SM_REEF_MODIFIED::decode(ceph::buffer::list::const_iterator& bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(objects, bl);
  {
    map<string,string> attrs;  // deprecated
    decode(attrs, bl);
  }
  ceph::buffer::list old_logbl;   // not used
  decode(old_logbl, bl);
  decode(valid_through, bl);
  decode(incr_since, bl);
  DECODE_FINISH(bl);

  // handle hobject_t upgrade
  if (struct_v < 3) {
    map<hobject_t, object> tmp;
    tmp.swap(objects);
    for (auto i = tmp.begin(); i != tmp.end(); ++i) {
      hobject_t first(i->first);
      if (!first.is_max() && first.pool == -1)
	first.pool = pool;
      objects[first] = i->second;
    }
  }
}

void SM_REEF_MODIFIED::dump(Formatter *f) const
{
  f->dump_stream("valid_through") << valid_through;
  f->dump_stream("incremental_since") << incr_since;
  f->open_array_section("objects");
  for (auto p = objects.cbegin(); p != objects.cend(); ++p) {
    f->open_object_section("object");
    f->dump_string("name", p->first.oid.name);
    f->dump_unsigned("hash", p->first.get_hash());
    f->dump_string("key", p->first.get_key());
    f->dump_int("snapid", p->first.snap);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void SM_REEF_MODIFIED::generate_test_instances(list<SM_REEF_MODIFIED*>& o)
{
  //o.push_back(new SM_REEF_MODIFIED);
  o.push_back(new SM_REEF_MODIFIED);
  o.back()->valid_through = eversion_t(1, 2);
  o.back()->incr_since = eversion_t(3, 4);
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0, "")] = *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0, "")] = *obj.back();
}

list<SM_REEF_MODIFIED*> SM_REEF_MODIFIED::generate_test_instances()
{
  list<SM_REEF_MODIFIED*> o;
  //o.push_back(new SM_REEF_MODIFIED);
  o.push_back(new SM_REEF_MODIFIED);
  o.back()->valid_through = eversion_t(1, 2);
  o.back()->incr_since = eversion_t(3, 4);
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0, "")] = *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0, "")] = *obj.back();
  return o;
}

// -- SM_REEF_MODIFIED::object --

void SM_REEF_MODIFIED::object::encode(ceph::buffer::list& bl) const
{
  bool compat_read_error = read_error || ec_hash_mismatch || ec_size_mismatch;
  ENCODE_START(10, 7, bl);
  std::cout << "\nobbjec size " << size << std::endl;
  encode(size, bl);
  encode(negative, bl);
  //encode(attrs, bl);

  // encode((__u32)0xffff, bl);
  encode((__u32)attrs.size(), bl);

  std::cout << "\nencoding attrs " << attrs.size() << std::endl;
  if (attrs.size() > 0) {
  for (const auto& [k,v] : attrs) {
   std::cout << "encoding attr " << k << " with value " << v.c_str() << std::endl;
    encode(k, bl);
    encode(v, bl);
  }
  }



  encode(digest, bl);
  encode(digest_present, bl);
  encode((uint32_t)0, bl);  // obsolete nlinks
  encode((uint32_t)0, bl);  // snapcolls
  encode(omap_digest, bl);
  encode(omap_digest_present, bl);
  encode(compat_read_error, bl);
  encode(stat_error, bl);
  encode(read_error, bl);
  encode(ec_hash_mismatch, bl);
  encode(ec_size_mismatch, bl);
  encode(large_omap_object_found, bl);
  encode(large_omap_object_key_count, bl);
  encode(large_omap_object_value_size, bl);
  encode(object_omap_bytes, bl);
  encode(object_omap_keys, bl);
  ENCODE_FINISH(bl);
}

void SM_REEF_MODIFIED::object::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(10, bl);
  decode(size, bl);
  std::cout << "\nobbjec size " << size << std::endl;
  bool tmp, compat_read_error = false;
  decode(tmp, bl);
  negative = tmp;
  //decode(attrs, bl);

  attrs.clear();
  //__u32 expect_0xffff;
  //decode(expect_0xffff, bl);
  //ceph_assert(expect_0xffff == 0xffff);
  __u32 attr_len;
  decode(attr_len, bl);
  std::cout << "decoding " << attr_len << " attrs" << std::endl;
  for (unsigned i = 0; i < attr_len; ++i) {
    string k;
    decode(k, bl);
    ceph::buffer::ptr v;
   __u32 len;
    decode(len, bl);
    std::cout << "\tl " << len << std::endl;
//     if (len == (uint32_t)-1) {
//       continue;
//     }
    bufferlist s;
    bl.copy(len, s);

    if (len > 0) {
      if (s.get_num_buffers() == 1) {
        attrs[k] = s.front();
      } else {
        attrs[k] = buffer::copy(s.c_str(), s.length());
      }
    }
   //decode(v, bl);
    //attrs[k] = v;
  }




  decode(digest, bl);
  decode(tmp, bl);
  digest_present = tmp;
  {
    uint32_t nlinks;
    decode(nlinks, bl);
    set<snapid_t> snapcolls;
    decode(snapcolls, bl);
  }
  decode(omap_digest, bl);
  decode(tmp, bl);
  omap_digest_present = tmp;
  decode(compat_read_error, bl);
  decode(tmp, bl);
  stat_error = tmp;
  if (struct_v >= 8) {
    decode(tmp, bl);
    read_error = tmp;
    decode(tmp, bl);
    ec_hash_mismatch = tmp;
    decode(tmp, bl);
    ec_size_mismatch = tmp;
  }
  // If older encoder found a read_error, set read_error
  if (compat_read_error && !read_error && !ec_hash_mismatch && !ec_size_mismatch)
    read_error = true;
  if (struct_v >= 9) {
    decode(tmp, bl);
    large_omap_object_found = tmp;
    decode(large_omap_object_key_count, bl);
    decode(large_omap_object_value_size, bl);
  }
  if (struct_v >= 10) {
    decode(object_omap_bytes, bl);
    decode(object_omap_keys, bl);
  }
  DECODE_FINISH(bl);
}

void SM_REEF_MODIFIED::object::dump(Formatter *f) const
{
  f->dump_int("size", size);
  f->dump_int("negative", negative);
  f->open_array_section("attrs");
  for (auto p = attrs.cbegin(); p != attrs.cend(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();
}

void SM_REEF_MODIFIED::object::generate_test_instances(list<object*>& o)
{
  o.push_back(new object);
  o.back()->size = 1;
  o.push_back(new object);
  o.back()->negative = true;
  o.push_back(new object);
  o.back()->size = 123;
  o.back()->attrs["foo"] = ceph::buffer::copy("foo", 3);
  o.back()->attrs["bar"] = ceph::buffer::copy("barval", 6);
}

namespace fmt {
template <>
struct formatter<SM_REEF_MODIFIED::object> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  ///\todo: consider passing the 'D" flag to control snapset dump
  template <typename FormatContext>
  auto format(const SM_REEF_MODIFIED::object& so, FormatContext& ctx)
  {
    fmt::format_to(ctx.out(),
		   "so{{ sz:{} dd:{} od:{} ",
		   so.size,
		   so.digest,
		   so.digest_present);

    // note the special handling of (1) OI_ATTR and (2) non-printables
    for (auto [k, v] : so.attrs) {
      std::string bkstr{v.raw_c_str(), v.raw_length()};
      if (k == std::string{OI_ATTR}) {
	/// \todo consider parsing the OI args here. Maybe add a specific format
	/// specifier
	fmt::format_to(ctx.out(), "{{{}:<<OI_ATTR>>({})}} ", k, bkstr.length());
      } else if (k == std::string{SS_ATTR}) {
	bufferlist bl;
	bl.push_back(v);
	SnapSet sns{bl};
	fmt::format_to(ctx.out(), "{{{}:{:D}}} ", k, sns);
      } else {
	fmt::format_to(ctx.out(), "{{{}:{}({})}} ", k, bkstr, bkstr.length());
      }
    }

    return fmt::format_to(ctx.out(), "}}");
  }
};

template <>
struct fmt::formatter<SM_REEF_MODIFIED> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 'D') {
      debug_log = true;	 // list the objects
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const SM_REEF_MODIFIED& smap, FormatContext& ctx)
  {
    fmt::format_to(ctx.out(),
		   "smap{{ valid:{} incr-since:{} #:{}",
		   smap.valid_through,
		   smap.incr_since,
		   smap.objects.size());
    if (debug_log) {
      fmt::format_to(ctx.out(), " objects:");
      for (const auto& [ho, so] : smap.objects) {
	fmt::format_to(ctx.out(), "\n\th.o<{}>:<{}> ", ho, so);
      }
      fmt::format_to(ctx.out(), "\n");
    }
    return fmt::format_to(ctx.out(), "}}");
  }

  bool debug_log{false};
};

}

// ////////////////////////////////// the Squid version

struct SMQD {
  struct object {
    std::map<std::string, ceph::buffer::list, std::less<>> attrs;
    uint64_t size{0};
    __u32 omap_digest;         ///< omap crc32c
    __u32 digest;              ///< data crc32c
    bool negative:1;
    bool digest_present:1;
    bool omap_digest_present:1;
    bool read_error:1;
    bool stat_error:1;
    bool ec_hash_mismatch:1;
    bool ec_size_mismatch:1;
    bool large_omap_object_found:1;
    uint64_t large_omap_object_key_count = 0;
    uint64_t large_omap_object_value_size = 0;
    uint64_t object_omap_bytes = 0;
    uint64_t object_omap_keys = 0;

    object() :
      // Init invalid size so it won't match if we get a stat EIO error
      size(-1), omap_digest(0), digest(0),
      negative(false), digest_present(false), omap_digest_present(false),
      read_error(false), stat_error(false), ec_hash_mismatch(false),
      ec_size_mismatch(false), large_omap_object_found(false) {}

    void encode(ceph::buffer::list& bl) const;
    void decode(ceph::buffer::list::const_iterator& bl);
    void dump(ceph::Formatter *f) const;
    static void generate_test_instances(std::list<object*>& o);
  };
  WRITE_CLASS_ENCODER(object)

  std::map<hobject_t,object> objects;
  eversion_t valid_through;
  eversion_t incr_since;
  bool has_large_omap_object_errors{false};
  bool has_omap_keys{false};

  void merge_incr(const SMQD &l);
  void clear_from(const hobject_t& start) {
    objects.erase(objects.lower_bound(start), objects.end());
  }
  void insert(const SMQD &r) {
    objects.insert(r.objects.begin(), r.objects.end());
  }
  void swap(SMQD &r) {
    using std::swap;
    swap(objects, r.objects);
    swap(valid_through, r.valid_through);
    swap(incr_since, r.incr_since);
    swap(has_large_omap_object_errors, r.has_large_omap_object_errors);
    swap(has_omap_keys, r.has_omap_keys);
  }

  void encode(ceph::buffer::list& bl) const;
  void decode(ceph::buffer::list::const_iterator& bl, int64_t pool=-1);
  void dump(ceph::Formatter *f) const;
  static void generate_test_instances(std::list<SMQD*>& o);
};
WRITE_CLASS_ENCODER(SMQD::object)
WRITE_CLASS_ENCODER(SMQD)


void SMQD::merge_incr(const SMQD &l)
{
  ceph_assert(valid_through == l.incr_since);
  valid_through = l.valid_through;

  for (auto p = l.objects.cbegin(); p != l.objects.cend(); ++p){
    if (p->second.negative) {
      auto q = objects.find(p->first);
      if (q != objects.end()) {
	objects.erase(q);
      }
    } else {
      objects[p->first] = p->second;
    }
  }
}          

void SMQD::encode(ceph::buffer::list& bl) const
{
  ENCODE_START(3, 2, bl);
  encode(objects, bl);
  encode((__u32)0, bl); // used to be attrs; now deprecated
  ceph::buffer::list old_logbl;  // not used
  encode(old_logbl, bl);
  encode(valid_through, bl);
  encode(incr_since, bl);
  ENCODE_FINISH(bl);
}

void SMQD::decode(ceph::buffer::list::const_iterator& bl, int64_t pool)
{
  DECODE_START_LEGACY_COMPAT_LEN(3, 2, 2, bl);
  decode(objects, bl);
  {
    map<string,string> attrs;  // deprecated
    decode(attrs, bl);
  }
  ceph::buffer::list old_logbl;   // not used
  decode(old_logbl, bl);
  decode(valid_through, bl);
  decode(incr_since, bl);
  DECODE_FINISH(bl);

  // handle hobject_t upgrade
  if (struct_v < 3) {
    map<hobject_t, object> tmp;
    tmp.swap(objects);
    for (auto i = tmp.begin(); i != tmp.end(); ++i) {
      hobject_t first(i->first);
      if (!first.is_max() && first.pool == -1)
	first.pool = pool;
      objects[first] = i->second;
    }
  }
}

void SMQD::dump(Formatter *f) const
{
  f->dump_stream("valid_through") << valid_through;
  f->dump_stream("incremental_since") << incr_since;
  f->open_array_section("objects");
  for (auto p = objects.cbegin(); p != objects.cend(); ++p) {
    f->open_object_section("object");
    f->dump_string("name", p->first.oid.name);
    f->dump_unsigned("hash", p->first.get_hash());
    f->dump_string("key", p->first.get_key());
    f->dump_int("snapid", p->first.snap);
    p->second.dump(f);
    f->close_section();
  }
  f->close_section();
}

void SMQD::generate_test_instances(list<SMQD*>& o)
{
  //o.push_back(new SMQD);
  o.push_back(new SMQD);
  o.back()->valid_through = eversion_t(1, 2);
  o.back()->incr_since = eversion_t(3, 4);
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0, "")] = *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0, "")] = *obj.back();
}

// -- SMQD::object --

void SMQD::object::encode(ceph::buffer::list& bl) const
{
  bool compat_read_error = read_error || ec_hash_mismatch || ec_size_mismatch;
  ENCODE_START(10, 7, bl);
  encode(size, bl);
  encode(negative, bl);

  // fix?
//   std::map<std::string, ceph::buffer::ptr, std::less<>> l_attrs;
//   for (auto& [k, v] : attrs) {
//     ceph::buffer::ptr x{v.raw_c_str(), v.raw_length()};
//     l_attrs[k] = x;
//     //l_attrs[k] = v.to_bufferptr();
//   }

  encode(attrs, bl);
  encode(digest, bl);
  encode(digest_present, bl);
  encode((uint32_t)0, bl);  // obsolete nlinks
  encode((uint32_t)0, bl);  // snapcolls
  encode(omap_digest, bl);
  encode(omap_digest_present, bl);
  encode(compat_read_error, bl);
  encode(stat_error, bl);
  encode(read_error, bl);
  encode(ec_hash_mismatch, bl);
  encode(ec_size_mismatch, bl);
  encode(large_omap_object_found, bl);
  encode(large_omap_object_key_count, bl);
  encode(large_omap_object_value_size, bl);
  encode(object_omap_bytes, bl);
  encode(object_omap_keys, bl);
  ENCODE_FINISH(bl);
}

void SMQD::object::decode(ceph::buffer::list::const_iterator& bl)
{
  DECODE_START(10, bl);
  decode(size, bl);
  bool tmp, compat_read_error = false;
  decode(tmp, bl);
  negative = tmp;
  decode(attrs, bl);
  decode(digest, bl);
  decode(tmp, bl);
  digest_present = tmp;
  {
    uint32_t nlinks;
    decode(nlinks, bl);
    set<snapid_t> snapcolls;
    decode(snapcolls, bl);
  }
  decode(omap_digest, bl);
  decode(tmp, bl);
  omap_digest_present = tmp;
  decode(compat_read_error, bl);
  decode(tmp, bl);
  stat_error = tmp;
  if (struct_v >= 8) {
    decode(tmp, bl);
    read_error = tmp;
    decode(tmp, bl);
    ec_hash_mismatch = tmp;
    decode(tmp, bl);
    ec_size_mismatch = tmp;
  }
  // If older encoder found a read_error, set read_error
  if (compat_read_error && !read_error && !ec_hash_mismatch && !ec_size_mismatch)
    read_error = true;
  if (struct_v >= 9) {
    decode(tmp, bl);
    large_omap_object_found = tmp;
    decode(large_omap_object_key_count, bl);
    decode(large_omap_object_value_size, bl);
  }
  if (struct_v >= 10) {
    decode(object_omap_bytes, bl);
    decode(object_omap_keys, bl);
  }
  DECODE_FINISH(bl);
}

void SMQD::object::dump(Formatter *f) const
{
  f->dump_int("size", size);
  f->dump_int("negative", negative);
  f->open_array_section("attrs");
  for (auto p = attrs.cbegin(); p != attrs.cend(); ++p) {
    f->open_object_section("attr");
    f->dump_string("name", p->first);
    f->dump_int("length", p->second.length());
    f->close_section();
  }
  f->close_section();
}

void SMQD::object::generate_test_instances(list<object*>& o)
{
  o.push_back(new object);
  o.back()->size = 1;
  o.push_back(new object);
  o.back()->negative = true;
  o.push_back(new object);
  o.back()->size = 123;
  {
    bufferlist foobl;
    foobl.push_back(ceph::buffer::copy("foo", 3));
    o.back()->attrs["foo"] = std::move(foobl);
  }
  {
    bufferlist barbl;
    barbl.push_back(ceph::buffer::copy("barval", 6));
    o.back()->attrs["bar"] = std::move(barbl);
  }
}

namespace fmt {
template <>
struct formatter<SMQD::object> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  ///\todo: consider passing the 'D" flag to control snapset dump
  template <typename FormatContext>
  auto format(const SMQD::object& so, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(),
		   "so{{ sz:{} dd:{} od:{} ",
		   so.size,
		   so.digest,
		   so.digest_present);

    // note the special handling of (1) OI_ATTR and (2) non-printables
    for (auto [k, v] : so.attrs) {
      std::string bkstr = v.to_str();
      if (k == std::string{OI_ATTR}) {
	/// \todo consider parsing the OI args here. Maybe add a specific format
	/// specifier
	fmt::format_to(ctx.out(), "{{{}:<<OI_ATTR>>({})}} ", k, bkstr.length());
      } else if (k == std::string{SS_ATTR}) {
	SnapSet sns{v};
	fmt::format_to(ctx.out(), "{{{}:{:D}}} ", k, sns);
      } else {
	fmt::format_to(ctx.out(), "{{{}:{}({})}} ", k, bkstr, bkstr.length());
      }
    }

    return fmt::format_to(ctx.out(), "}}");
  }
};

template <>
struct formatter<SMQD> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 'D') {
      debug_log = true;	 // list the objects
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const SMQD& smap, FormatContext& ctx) const
  {
    fmt::format_to(ctx.out(),
		   "smap{{ valid:{} incr-since:{} #:{}",
		   smap.valid_through,
		   smap.incr_since,
		   smap.objects.size());
    if (debug_log) {
      fmt::format_to(ctx.out(), " objects:");
      for (const auto& [ho, so] : smap.objects) {
	fmt::format_to(ctx.out(), "\n\th.o<{}>:<{}> ", ho, so);
      }
      fmt::format_to(ctx.out(), "\n");
    }
    return fmt::format_to(ctx.out(), "}}");
  }

  bool debug_log{false};
};

}


// /////////////////////////////////////////////////////////// TESTS

TEST(TestOSDScrub, attr_encoding) {
//MOSDRepScrubMap reg_msg;
  auto reefs = SM_REEF_MODIFIED::generate_test_instances();
  for (const auto& reef : reefs) {
    std::cout << fmt::format("SM_REEF_MODIFIED: {:D}\n", *reef);
    ceph::buffer::list bl;
    encode(*reef, bl);
    bl.hexdump(std::cout);
    std::cout << "bl: SM_REEF_MODIFIED version" << bl << std::endl;
    ceph::buffer::list::const_iterator p = bl.cbegin();
    SM_REEF_MODIFIED reef2;
    decode(reef2, p);
    std::cout << fmt::format("SMRR2: {:D}\n", reef2);
    //std::cout << "reef2: " << reef2 << std::endl;
    //ASSERT_EQ(reef, reef2);
  }
}

TEST(TestOSDScrub, r_to_r) {
//MOSDRepScrubMap reg_msg;
  auto reefs = SMRR::generate_test_instances();
  for (const auto& reef : reefs) {
    std::cout << fmt::format("-------  r_to_r\t\tSMRR: {:D}\n", *reef);
    ceph::buffer::list bl;
    encode(*reef, bl);
    bl.hexdump(std::cout);
    std::cout << "bl: SMRR version" << bl << std::endl;
    SMRR reef2;
    decode(reef2, bl);
    std::cout << fmt::format("SMRR2: {:D}\n", reef2);
  }
}

TEST(TestOSDScrub, mod_r_to_mod_r) {
  auto reefs = SM_REEF_MODIFIED::generate_test_instances();
  for (const auto& reef : reefs) {
    std::cout << fmt::format("-------  mod_r_to_mod_r\t\tSMRR: {:D}\n", *reef);
    ceph::buffer::list bl;
    encode(*reef, bl);
    bl.hexdump(std::cout);
    std::cout << "bl: SM_REEF_MODIFIED version" << bl << std::endl;
    SM_REEF_MODIFIED reef2;
    decode(reef2, bl);
    std::cout << fmt::format("SMRR2 mod-reef: {:D}\n", reef2);
  }
}

TEST(TestOSDScrub, r_to_mod_r) {
  auto reefs = SMRR::generate_test_instances();
  for (const auto& reef : reefs) {
    std::cout << fmt::format("-------  r_to_mod_r\t\torig: {:D}\n", *reef);
    ceph::buffer::list bl;
    encode(*reef, bl);
    bl.hexdump(std::cout);
    std::cout << "bl: SMRR version" << bl << std::endl;
    SM_REEF_MODIFIED reef2;
    decode(reef2, bl);
    std::cout << fmt::format("SMRR2 mod-reef: {:D}\n", reef2);
  }
}

#if 0
TEST(TestOSDScrub, r_to_rs) {
//MOSDRepScrubMap reg_msg;
  auto reefs = SM_REEF_MODIFIED::generate_test_instances();
  reefs.pop_front();
  //const auto& r = reefs.front();
  for (const auto& reef : reefs) {
    std::cout << fmt::format("SM_REEF_MODIFIED: {:D}\n", *reef);
    ceph::buffer::list bl;
    encode(*reef, bl);
    bl.hexdump(std::cout);
    std::cout << "bl: SM_REEF_MODIFIED version" << bl << std::endl;
    SM_REEF_MODIFIED reef2;
    //decode(reef2, bl);
    std::cout << fmt::format("SMRR2: {:D}\n", reef2);
    //std::cout << "reef2: " << reef2 << std::endl;
    //ASSERT_EQ(reef, reef2);
  }
}


TEST(TestOSDScrub, smap_r_to_s) {
//MOSDRepScrubMap reg_msg;
  auto reefs = SM_REEF_MODIFIED::generate_test_instances();
  for (const auto& reef : reefs) {
    std::cout << fmt::format("SM_REEF_MODIFIED: {:D}\n", *reef);
    ceph::buffer::list bl;
    encode(*reef, bl);
    std::cout << "bl: " << bl << std::endl;
    bl.hexdump(std::cout);
    ceph::buffer::list::const_iterator p = bl.cbegin();
    ScrubMap sqmap;
    decode(sqmap, p);
    std::cout << fmt::format("ScrubMap(S): {:D}\n", sqmap);
    //std::cout << "reef2: " << reef2 << std::endl;
    //ASSERT_EQ(reef, reef2);
  }
}

TEST(TestOSDScrub, s_to_s) {
  std::list<ScrubMap*> sqds;
  ScrubMap::generate_test_instances(sqds);
  for (const auto& sq : sqds) {
    std::cout << fmt::format("--------- s_to_s\t\tScrubMap: {:D}\n", *sq);
    ceph::buffer::list bl;
    encode(*sq, bl);
    std::cout << "bl: " << bl << std::endl;
    bl.hexdump(std::cout);

    ceph::buffer::list::const_iterator p = bl.cbegin();
    ScrubMap squid2;
    decode(squid2, p);
    std::cout << fmt::format("squid2: {:D}\n", squid2);
    //std::cout << "reef2: " << reef2 << std::endl;
    //ASSERT_EQ(reef, reef2);
  }
}

TEST(TestOSDScrub, s_to_r) {
  std::list<ScrubMap*> sqds;
  ScrubMap::generate_test_instances(sqds);
  for (const auto& sq : sqds) {
    std::cout << fmt::format("------- s_to_r\t\tScrubMap: {:D}\n", *sq);
    ceph::buffer::list bl;
    encode(*sq, bl);
    std::cout << "----------------------- bl: " << bl << std::endl;
    bl.hexdump(std::cout);
    ceph::buffer::list::const_iterator p = bl.cbegin();
    SM_REEF_MODIFIED reef2;
    decode(reef2, p);
    std::cout << fmt::format("SMRR2: {:D}\n", reef2);

    {
        ceph::buffer::list bl2;
        encode(reef2, bl2);
        std::cout << "bl2: " << bl2 << std::endl;
        bl2.hexdump(std::cout);
        ceph::buffer::list::const_iterator p2 = bl2.cbegin();
        ScrubMap squid3;
        decode(squid3, p2);
        std::cout << fmt::format("back squid3: {:D}\n", squid3);
            }
    //std::cout << "reef2: " << reef2 << std::endl;
    //ASSERT_EQ(reef, reef2);
  }
}

TEST(TestOSDScrub, q_to_r) {
  std::list<SMQD*> sqds;
  SMQD::generate_test_instances(sqds);
  for (const auto& sq : sqds) {
    std::cout << fmt::format("SMQD: {:D}\n", *sq);
    ceph::buffer::list bl;
    encode(*sq, bl);
    std::cout << "bl: " << bl << std::endl;
    bl.hexdump(std::cout);
    ceph::buffer::list::const_iterator p = bl.cbegin();
    SM_REEF_MODIFIED reef2;
    decode(reef2, p);
    std::cout << fmt::format("SMRR2: {:D}\n", reef2);
    //std::cout << "reef2: " << reef2 << std::endl;
    //ASSERT_EQ(reef, reef2);
  }
}

#endif

// Local Variables:
// compile-command: "cd ../.. ; make unittest_osdscrub ; ./unittest_osdscrub --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* "
// End:
