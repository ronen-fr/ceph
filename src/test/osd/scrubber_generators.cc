// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:f -*-
// vim: ts=8 sw=2 smarttab


#include "test/osd/scrubber_generators.h"

using namespace ScrubGenerator;

// ref: PGLogTestRebuildMissing()
bufferptr create_object_info(const ScrubGenerator::RealObjVer& objver)
{
  object_info_t oi{};
  oi.soid = objver.ghobj.hobj;
  oi.version = eversion_t(objver.ghobj.generation, 0);
  // oi.version = objver.ghobj.generation;
  oi.size = objver.data.size;

  bufferlist bl;
  oi.encode(bl,
            0 /*get_osdmap()->get_features(CEPH_ENTITY_TYPE_OSD, nullptr)*/);
  bufferptr bp(bl.c_str(), bl.length());  // RRR check whether allocates
  // bl.append(objver.oi_attr);
  return bp;
}

std::pair<bufferptr, std::vector<snapid_t>> create_object_snapset(
  const ScrubGenerator::RealObj& robj,
  const SnapsetMockData* snapset_mock_data)
{
  if (!snapset_mock_data) {
    return {bufferptr(), {}};
  }
  // RRR \todo fill in missing version/osd details from the robj
  auto sns = snapset_mock_data->make_snapset(/*robj*/);
  bufferlist bl;
  encode(sns, bl);
  bufferptr bp = bufferptr(bl.c_str(), bl.length());

  // extract the set of object snaps
  return {bp, sns.snaps};
}


RealObjsConfList ScrubGenerator::make_real_objs_conf(
  int64_t pool_id,  // const pool_conf_t& pool_conf,
  const RealObjsConf& blueprint,
  std::vector<int32_t> active_osds)
{
  RealObjsConfList all_osds;

  for (auto osd : active_osds) {
    RealObjsConfRef this_osd_fakes = std::make_unique<RealObjsConf>(blueprint);
    // now - fix & corrupt ever "object" in the blueprint
    for (RealObj& robj : this_osd_fakes->objs) {

      robj.object_version.ghobj.hobj.pool = pool_id;
    }

    all_osds[osd] = std::move(this_osd_fakes);
  }
  return all_osds;  // reconsider (maybe add a move ctor?)
}


hobject_t ScrubGenerator::make_hobject(
  const ScrubGenerator::TargetHObject& blueprint)
{
  hobject_t hobj;
  hobj.oid = blueprint.object_name;
  hobj.set_key(blueprint.key);
  hobj.nspace = "";
  hobj.snap = blueprint.snap;
  hobj.set_hash(blueprint.hash);
  hobj.pool = 0;
  return hobj;
}

///\todo dispose of the created buffer pointers

ScrubGenerator::SmapEntry ScrubGenerator::make_smobject(
  const ScrubGenerator::RealObj& blueprint,  // the whole set of versions
  const ScrubGenerator::RealObjVer& objver,  // the "fixed" object version
  int osd_num)
{
  ScrubGenerator::SmapEntry ret;

  ret.ghobj = objver.ghobj;
  ret.smobj.attrs[OI_ATTR] = create_object_info(objver);
  if (blueprint.snapset_mock_data) {
    auto [bp, snaps] =
      create_object_snapset(blueprint, blueprint.snapset_mock_data);
    ret.smobj.attrs[SS_ATTR] = bp;
    ret.object_snaps = snaps;
    std::cout << fmt::format("{}: ({}) osd:{} snaps:{}",
                             __func__,
                             ret.ghobj.hobj,
                             osd_num,
                             snaps)
              << std::endl;
    // ret.smobj.attrs[SS_ATTR] =
    // create_object_snapset(blueprint, blueprint.snapset_mock_data);
  }

  for (const auto& [at_k, at_v] : objver.data.attrs) {
    ret.smobj.attrs[at_k] = ceph::buffer::copy(at_v.c_str(), at_v.size());

    {
      // verifying
      auto bk = ret.smobj.attrs[at_k].clone();
      std::string bkstr{bk.get()->get_data(), bk.get()->get_len()};
      std::cout << "YYY " << bkstr << "\n";
    }
  }
  ret.smobj.size = objver.data.size;
  ret.smobj.digest = objver.data.hash;
  // handle the 'present' etc'

  ret.smobj.object_omap_keys = objver.data.omap.size();
  // ret.smobj.omap_digest = objver.data.omap_hash;
  ret.smobj.object_omap_bytes = objver.data.omap_bytes;

  std::cout << fmt::format("{}: osd:{} gh:{} key:{} snaps:{}\n",
                           __func__,
                           osd_num,
                           17,  // objver.ghobj.hobj.oid,
                           17,  // objver.ghobj.hobj.get_key());
                           ret.object_snaps);
  // fmt::join(ret.object_snaps,","));
  return ret;
}

all_clones_snaps_t ScrubGenerator::all_clones(
  const ScrubGenerator::RealObj& head_obj)
{
  std::map<hobject_t, std::vector<snapid_t>> ret;
  // auto snapset = head_obj.snapset_mock_data->make_snapset();

  for (auto clone : head_obj.snapset_mock_data->clones) {
    auto clone_set_it = head_obj.snapset_mock_data->clone_snaps.find(clone);
    if (clone_set_it == head_obj.snapset_mock_data->clone_snaps.end()) {
      std::cout << "note: no clone_snaps for " << clone << std::endl;
      continue;
    }
    auto clone_set = clone_set_it->second;
    hobject_t clone_hobj{head_obj.object_version.ghobj.hobj};
    clone_hobj.snap = clone;

    ret[clone_hobj] = clone_set_it->second;
  }

  return ret;
}


// ScrubMap::object ScrubGenerator::make_smobject(
//   const ScrubGenerator::RealObj& blueprint,  // the whole set of versions
//   const ScrubGenerator::RealObjVer& objver)  // the "fixed" object version
// {
//   ScrubMap::object obj{};
//
//   obj.attrs[OI_ATTR] = create_object_info(objver);
//   if (blueprint.snapset_mock_data) {
//     // only for head
//     obj.attrs[SS_ATTR] =
//       create_object_snapset(blueprint, blueprint.snapset_mock_data);
//   }
//
//   for (const auto& [at_k, at_v] : objver.data.attrs) {
//     obj.attrs[at_k] = ceph::buffer::copy(at_v.c_str(), at_v.size());
//
//     {
//       // verifying
//       auto bk = obj.attrs[at_k].clone();
//       std::string bkstr{bk.get()->get_data(), bk.get()->get_len()};
//       std::cout << "YYY " << bkstr << "\n";
//     }
//   }
//   obj.size = objver.data.size;
//   obj.digest = objver.data.hash;
//   // handle the 'present' etc'
//
//   obj.object_omap_keys = objver.data.omap.size();
//   // obj.omap_digest = objver.data.omap_hash;
//   obj.object_omap_bytes = objver.data.omap_bytes;
//
//   return obj;
// }
#if 0
static ScrubGenerator::SmapEntry make_smap_entry(
  const ScrubGenerator::RealObj& blueprint,  // the whole set of versions
  const ScrubGenerator::RealObjVer& objver,  // the "fixed" object version
  int osd_num)
{
  using namespace ScrubGenerator;
  std::cout << fmt::format("{}: osd:{} gh:{} key:{}\n",
                           __func__,
                           osd_num,
                           17 /*objver.ghobj.hobj.oid*/,
                           objver.ghobj.hobj.get_key());

  SmapEntry entry{};
  entry.ghobj = objver.ghobj;
  entry.smobj = make_smobject(blueprint, objver);
  entry.object_snaps = ;
  // obj.attrs = blueprint.attrs;

  return entry;
}
#endif

#ifdef EXAMPLE_SCRUB_MAP
void ScrubMap::generate_test_instances(list<ScrubMap*>& o)
{
  o.push_back(new ScrubMap);
  o.push_back(new ScrubMap);
  o.back()->valid_through = eversion_t(1, 2);
  o.back()->incr_since = eversion_t(3, 4);
  list<object*> obj;
  object::generate_test_instances(obj);
  o.back()->objects[hobject_t(object_t("foo"), "fookey", 123, 456, 0, "")] =
    *obj.back();
  obj.pop_back();
  o.back()->objects[hobject_t(object_t("bar"), string(), 123, 456, 0, "")] =
    *obj.back();
}
void ScrubMap::object::generate_test_instances(list<object*>& o)
{
  o.push_back(new object);
  o.push_back(new object);
  o.back()->negative = true;
  o.push_back(new object);
  o.back()->size = 123;
  o.back()->attrs["foo"] = ceph::buffer::copy("foo", 3);
  o.back()->attrs["bar"] = ceph::buffer::copy("barval", 6);
}
#endif

std::vector<snapid_t> ScrubGenerator::add_object(
  ScrubMap& map,
  const ScrubGenerator::RealObj& real_obj,
  int osd_num)
{
  using namespace ScrubGenerator;

  // do we have data corruption recipe for this OSD?
  // \todo c++20: use contains()
  CorruptFunc relevant_fix = crpt_do_nothing;

  //   auto p = obj_versions.corrupt_funcs->find(osd_num);
  //   if (false && p != obj_versions.corrupt_funcs->end()) {
  //     // yes, we have a corruption recepie for this OSD
  //     // \todo c++20: use at()
  //     relevant_fix = p->second;
  //   }


  // RRR should we persist the corrupted objects?
  // only 1 entry. The vector is obsolete
  // if (obj_versions.real_versions.size() > 0) {
  auto& obj_version = real_obj.object_version;
  // for (auto& obj_version : obj_versions.real_versions) {  // note: a copy

  // obj_version = relevant_fix(obj_version, osd_num);
  std::cout << fmt::format("{}: osd:{} gh:{} key:{}\n",
                           __func__,
                           osd_num,
                           17 /*obj_version.ghobj.hobj.oid*/,  // RRR
                           obj_version.ghobj.hobj.get_key());

  auto entry = make_smobject(real_obj, obj_version, osd_num);
  std::cout << fmt::format("{}: osd:{} smap entry: {} {}\n",
                           __func__,
                           osd_num,
                           entry.smobj.size,
                           entry.smobj.attrs.size());
  map.objects[entry.ghobj.hobj] = entry.smobj;

  // add the snap_id sets for all the clones


  std::cout << fmt::format("{}: done\n", __func__);
  return entry.object_snaps;
}


// void ScrubGenerator::add_object0(ScrubMap& map,
//                                  const ScrubGenerator::RealObj& obj_versions,
//                                  int osd_num)
// {
//   using namespace ScrubGenerator;
//
//   // do we have data corruption recipe for this OSD?
//   // \todo c++20: use contains()
//   CorruptFunc relevant_fix = crpt_do_nothing;
//
//   //   auto p = obj_versions.corrupt_funcs->find(osd_num);
//   //   if (false && p != obj_versions.corrupt_funcs->end()) {
//   //     // yes, we have a corruption recepie for this OSD
//   //     // \todo c++20: use at()
//   //     relevant_fix = p->second;
//   //   }
//
//
//   // RRR should we persist the corrupted objects?
//   for (auto& obj_version : obj_versions.real_versions) {  // note: a copy
//
//     // obj_version = relevant_fix(obj_version, osd_num);
//     std::cout << fmt::format("{}: osd:{} gh:{} key:{}\n",
//                              __func__,
//                              osd_num,
//                              17 /*obj_version.ghobj.hobj.oid*/,
//                              obj_version.ghobj.hobj.get_key());
//
//     auto entry = make_smap_entry(obj_versions, obj_version, osd_num);
//     std::cout << fmt::format("{}: osd:{} smap entry: {} {}\n",
//                              __func__,
//                              osd_num,
//                              entry.smobj.size,
//                              entry.smobj.attrs.size());
//     map.objects[entry.ghobj.hobj] = entry.smobj;
//   }
//   std::cout << fmt::format("{}: done\n", __func__);
// }

// void ScrubGenerator::add_to_smaps(const pool_conf_t& pool_conf,
//                   const RealObj& obj_versions,
//                   chunk_smap_setter_t setter)
// {
//   for (int osd_num = 0; osd_num < pool_conf.size; ++osd_num) {
//     auto smp_entry = make_smap_entry(obj_versions, osd_num);
//     (setter)(pg_shard_t{osd_num}, smp_entry.smobj);
//   }
//
// }


// std::vector<ScrubMap::object> ScrubGenerator::make_smobjects(const
// ScrubGenerator::RealObj& blueprint, int osd_num)
// {
//   using namespace ScrubGenerator;
//
//   std::vector<ScrubMap::object> objs;
//   //      ScrubMap::object obj{};
//         //obj.attrs = blueprint.attrs;
//
//   return objs;
// }
//
//
// void ScrubGenerator::add_objects(ScrubMap& map, const
// ScrubGenerator::RealObj& obj_versions, int osd_num)
// {
//   using namespace ScrubGenerator;
//
//   // RRR we should be creating multiple entries
//
//   auto smp_obj = make_smobjects(obj_versions, osd_num);
//   map.objects[smp_obj.] = smp_obj;
// }
