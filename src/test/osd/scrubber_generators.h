// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:f -*-
// vim: ts=8 sw=2 smarttab
#pragma once

// generating scrub-related maps & objects for unit tests


// #include <gtest/gtest.h>
// #include <signal.h>
// #include <stdio.h>

#include <functional>
#include <map>
#include <vector>

//
// #include "common/async/context_pool.h"
// #include "common/ceph_argparse.h"
// #include "global/global_context.h"
// #include "global/global_init.h"
// #include "mon/MonClient.h"
// #include "msg/Messenger.h"
// #include "os/ObjectStore.h"
// #include "osd/PG.h"
// #include "osd/PGBackend.h"
// #include "osd/PrimaryLogPG.h"
// #include "osd/osd_types_fmt.h"
#include "osd/scrubber/pg_scrubber.h"


namespace ScrubGenerator {

#ifdef NOT_YET
class MockLog : /*public*/ LogChannel {
  public:
    MockLog()
      : LogChannel(nullptr, nullptr, "x") {}
//     MockLog(CephContext *cct, LogClient *lc, const string &channel)
//       : LogChannel(cct, lc, channel) {}
//     MockLog(CephContext *cct, LogClient *lc,
//             const string &channel, const string &facility,
//             const string &prio)
//       : LogChannel(cct, lc, channel, facility, prio) {}
    virtual ~MockLog() {}


 };
#endif

// ///////////////////////////////////////////////////////////////////////// //
// ///////////////////////////////////////////////////////////////////////// //

struct pool_conf_t {
  int pg_num{3};
  int pgp_num{3};
  int size{3};
  int min_size{3};
  std::string name{"rep_pool"};
};


using attr_t = std::map<std::string, std::string>;

struct RealObj;

// a function to manipulate (i.e. corrupt) an object in a specific OSD
using CorruptFunc = std::function<RealObj(const RealObj& s, int osd_num)>;
using CorruptFuncList = std::map<int, CorruptFunc>;  // per OSD


/*
 * a description of the objects that we will have in the created
 * scrub-maps.
 *
 * for each object:
 *   - a list of attributes
 * - ...
 * - how should we corrupt the object in the Replicas?
 */

struct TargetHObject {
  // object_t oid;
  std::string object_name;
  std::string key;
  snapid_t snap;
  uint64_t size;
  uint32_t hash;
  // int64_t pool;
};

hobject_t make_hobject(const TargetHObject& blueprint);

struct TargetSmObject {
  TargetHObject hobj_bluep;
  std::vector<std::string> attrs;
  uint64_t size;
  __u32 omap_digest;
  __u32 data_digest;
  // ...
};


ScrubMap::object make_smobject(const TargetSmObject& blueprint);


// an object in our "DB" - which its versioned snaps, "data" (size and hash),
// and "omap" (size and hash)

struct RealData {
  // for now - not needed: std::byte data;

  // the attributes - are they here?
  uint64_t size;
  uint32_t hash;
  attr_t omap;
  attr_t attrs;
};

struct RealObjVer {
  ghobject_t
    ghobj;  // oid, version, snap, hash, pool (and there is a special 'max' one)
  RealData data;
};

struct RealObj {
  std::vector<RealObjVer> real_versions;
  const CorruptFuncList* corrupt_funcs;
//   RealObj& operator=(const RealObj& other) {
//     real_versions = other.real_versions;
//     corrupt_funcs = other.corrupt_funcs;
//     return *this;
//   }
//   RealObj(const RealObj& other) : real_versions(other.real_versions),
//                                   corrupt_funcs(other.corrupt_funcs) {}

};

struct SmapEntry {
  ghobject_t ghobj;
  ScrubMap::object smobj;
};


// need version boundaries for  the following func
// std::vector<ScrubMap::object> make_smobjects(const RealObj& blueprint, int
// osd_num);
SmapEntry make_smap_entry(const RealObj& blueprint, int osd_num);

// void add_objects(ScrubMap& map, const RealObj& obj_versions, int osd_num);
void add_object(ScrubMap& map, const RealObj& obj_versions, int osd_num);

using chunk_smap_setter_t =
  std::function<void(pg_shard_t shard, const ScrubMap& smap)>;

// void add_to_smaps(const pool_conf_t& pool_conf,
//                   const RealObj& blueprint,
//                   chunk_smap_setter_t setter);

struct RealObjsConf {
  std::vector<RealObj> objs;
};

}  // namespace ScrubGenerator
