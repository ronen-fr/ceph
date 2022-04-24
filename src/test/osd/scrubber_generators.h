// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:f -*-
// vim: ts=8 sw=2 smarttab
#pragma once

// generating scrub-related maps & objects for unit tests

#include <functional>
#include <map>
#include <sstream>
#include <string>
#include <variant>
#include <vector>

#include "include/buffer.h"
#include "include/buffer_raw.h"
#include "include/object_fmt.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/pg_scrubber.h"


// ///////////////////////////////////////////////////////////////////////// //
// ///////////////////////////////////////////////////////////////////////// //

namespace ScrubGenerator {

/// \todo fix the MockLog to capture the log messages
class MockLog : public LoggerSinksSet {
 public:
  void info(std::stringstream& s) override
  {
    std::cout << "\n<<info>> " << s.str() << std::endl;
  }
  void warn(std::stringstream& s) override
  {
    std::cout << "\n<<warn>> " << s.str() << std::endl;
  }
  void error(std::stringstream& s) override
  {
    err_count++;
    std::cout << "\n<<error>> " << s.str() << std::endl;
  }
  void sec(std::stringstream& s) override
  {
    std::cout << "\n<<sec>> " << s.str() << std::endl;
  }
  void debug(std::stringstream& s) override
  {
    std::cout << "\n<<debug>> " << s.str() << std::endl;
  }
  OstreamTemp info() override { return OstreamTemp(CLOG_INFO, this); }
  OstreamTemp warn() override { return OstreamTemp(CLOG_WARN, this); }
  OstreamTemp error() override { return OstreamTemp(CLOG_ERROR, this); }
  OstreamTemp sec() override { return OstreamTemp(CLOG_ERROR, this); }
  OstreamTemp debug() override { return OstreamTemp(CLOG_DEBUG, this); }

  void do_log(clog_type prio, std::stringstream& ss) override
  {
    switch (prio) {
      case CLOG_INFO:
        info(ss);
        break;
      case CLOG_WARN:
        warn(ss);
        break;
      case CLOG_ERROR:
      default:
        error(ss);
        break;
      case CLOG_DEBUG:
        debug(ss);
        break;
    }
  }

  void do_log(clog_type prio, const std::string& ss) override
  {
    switch (prio) {
      case CLOG_INFO:
        info() << ss;
        break;
      case CLOG_WARN:
        warn() << ss;
        break;
      case CLOG_ERROR:
      default:
        error() << ss;
        break;
      case CLOG_DEBUG:
        debug() << ss;
        break;
    }
  }

  virtual ~MockLog() {}

  int err_count{0};
};

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

struct RealObjVer;

using all_clones_snaps_t = std::map<hobject_t, std::vector<snapid_t>>;

// a function to manipulate (i.e. corrupt) an object in a specific OSD
using CorruptFunc = std::function<RealObjVer(const RealObjVer& s, int osd_num)>;
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

struct RealObjVer;
struct RealObj;

struct SnapsetMockData {

  using cooked_clone_snaps =
    std::tuple<std::map<snapid_t, uint64_t>,
               std::map<snapid_t, std::vector<snapid_t>>,
               std::map<snapid_t, interval_set<uint64_t>>>;

  using clone_snaps_cooker = cooked_clone_snaps (*)(/*const RealObjVer&*/);

  snapid_t seq;
  std::vector<snapid_t> snaps;   // descending
  std::vector<snapid_t> clones;  // ascending
  std::map<snapid_t, interval_set<uint64_t>>
    clone_overlap;  // overlap w/ next newest
  std::map<snapid_t, uint64_t> clone_size;
  std::map<snapid_t, std::vector<snapid_t>> clone_snaps;  // descending


  SnapsetMockData(snapid_t seq,
                  std::vector<snapid_t> snaps,
                  std::vector<snapid_t> clones,
                  std::map<snapid_t, interval_set<uint64_t>> clone_overlap,
                  std::map<snapid_t, uint64_t> clone_size,
                  std::map<snapid_t, std::vector<snapid_t>> clone_snaps)
      : seq(seq)
      , snaps(snaps)
      , clones(clones)
      , clone_overlap(clone_overlap)
      , clone_size(clone_size)
      , clone_snaps(clone_snaps)
  {}

  SnapsetMockData(snapid_t seq,
                  std::vector<snapid_t> snaps,
                  std::vector<snapid_t> clones,
                  clone_snaps_cooker func)
      : seq{seq}
      , snaps{snaps}
      , clones(clones)
  {
    auto [clone_size_, clone_snaps_, clone_overlap_] = func();
    clone_size = clone_size_;
    clone_snaps = clone_snaps_;
    clone_overlap = clone_overlap_;
  }

  SnapSet make_snapset(/* needed for corrupting? const RealObj& blueprint*/) const
  {
    SnapSet ss;
    ss.seq = seq;
    ss.snaps = snaps;
    ss.clones = clones;
    ss.clone_overlap = clone_overlap;
    ss.clone_size = clone_size;
    ss.clone_snaps = clone_snaps;
    return ss;
  }
};

// an object in our "DB" - which its versioned snaps, "data" (size and hash),
// and "omap" (size and hash)

struct RealData {
  // for now - not needed: std::byte data;

  // the attributes - are they here?
  uint64_t size;
  uint32_t hash;
  uint32_t omap_digest;
  uint32_t omap_bytes;
  attr_t omap;
  attr_t attrs;
};

struct RealObjVer {
  ghobject_t
    ghobj;  // oid, version, snap, hash, pool (and there is a special 'max' one)
  RealData data;
};

struct RealObj {
  //std::vector<RealObjVer> real_versions;  // replaced with only one RRR
  /// \todo merge RealObjVer into RealObj
  RealObjVer object_version;
  const CorruptFuncList* corrupt_funcs;
  const SnapsetMockData* snapset_mock_data;
  //   RealObj& operator=(const RealObj& other) {
  //     real_versions = other.real_versions;
  //     corrupt_funcs = other.corrupt_funcs;
  //     return *this;
  //   }
  //   RealObj(const RealObj& other) : real_versions(other.real_versions),
  //                                   corrupt_funcs(other.corrupt_funcs) {}
};


inline static RealObjVer crpt_do_nothing(const RealObjVer& s, int osdn)
{
  return s;
}

struct SmapEntry {
  ghobject_t ghobj;
  ScrubMap::object smobj;
  std::vector<snapid_t> object_snaps;
};


ScrubGenerator::SmapEntry make_smobject(
  const ScrubGenerator::RealObj& blueprint,  // the whole set of versions
  const ScrubGenerator::RealObjVer& objver,  // the "fixed" object version
  int osd_num);


// need version boundaries for  the following func
// std::vector<ScrubMap::object> make_smobjects(const RealObj& blueprint, int
// osd_num);

// SmapEntry make_smap_entry(
//   const ScrubGenerator::RealObj& blueprint,  // the whole set of versions
//   const ScrubGenerator::RealObjVer& objver,  // the "fixed" object version
//   int osd_num);


/**
 * returns the object's snap-set
 */
std::vector<snapid_t> add_object(ScrubMap& map,
                                 const RealObj& obj_versions,
                                 int osd_num);

void add_object0(ScrubMap& map, const RealObj& obj_versions, int osd_num);


using chunk_smap_setter_t =
  std::function<void(pg_shard_t shard, const ScrubMap& smap)>;

// void add_to_smaps(const pool_conf_t& pool_conf,
//                   const RealObj& blueprint,
//                   chunk_smap_setter_t setter);

struct RealObjsConf {
  std::vector<RealObj> objs;
};

using RealObjsConfRef = std::unique_ptr<RealObjsConf>;

// RealObjsConf will be "developed" into the following of per-osd sets,
// now with the correct pool ID, and with the corrupting functions
// activated on the data
using RealObjsConfList = std::map<int, RealObjsConfRef>;

RealObjsConfList make_real_objs_conf(int64_t pool_id,
                                     const RealObjsConf& blueprint,
                                     std::vector<int32_t> active_osds);
// std::string list_multi_conf(const RealObjsConfList& confs);

/**
 * create the snap-ids set for all clones appearing in the head
 * object's snapset (those will be injected into the scrubber's mock,
 * to be used as the 'snap_mapper')
 */
all_clones_snaps_t all_clones(const RealObj& head_obj);


}  // namespace ScrubGenerator

template <>
struct fmt::formatter<ScrubGenerator::RealObjVer> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const ScrubGenerator::RealObjVer& rlj, FormatContext& ctx)
  {

    return fmt::format_to(ctx.out(),
                          "ROinstance({} / {})",
                          rlj.ghobj.hobj,
                          rlj.data.size);
  }
};

template <>
struct fmt::formatter<ScrubGenerator::RealObj> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const ScrubGenerator::RealObj& rlo, FormatContext& ctx)
  {
    using namespace ScrubGenerator;
    return fmt::format_to(ctx.out(),
                          "RealObj(gh:{}, dt:{}, snaps:{})", rlo.object_version.ghobj,
                                rlo.object_version.data.size,
                                rlo.snapset_mock_data->snaps);
                                //rlo.snapset_mock_data.make_snapset().snaps);
  }
};
