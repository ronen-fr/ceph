// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:f -*-
// vim: ts=8 sw=2 smarttab


#include "test/osd/scrubber_generators.h"


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


ScrubMap::object ScrubGenerator::make_smobject(
  const ScrubGenerator::RealObj& blueprint,  // the whole set of versions
  const ScrubGenerator::RealObjVer& objver)  // the "fixed" object version
{
  ScrubMap::object obj{};
  for (const auto& at : objver.data.attrs) {
    obj.attrs[at.first] =
      ceph::buffer::copy(at.second.c_str(), at.second.length());
  }
  obj.size = objver.data.size;
  obj.digest = objver.data.hash;
  // handle the 'present' etc'

  obj.object_omap_keys = objver.data.omap.size();
  // obj.omap_digest = objver.data.omap_hash;
  obj.object_omap_bytes = objver.data.omap_bytes;

  return obj;
}

ScrubGenerator::SmapEntry ScrubGenerator::make_smap_entry(
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
  // obj.attrs = blueprint.attrs;

  return entry;
}


void ScrubGenerator::add_object(ScrubMap& map,
                                const ScrubGenerator::RealObj& obj_versions,
                                int osd_num)
{
  using namespace ScrubGenerator;

  // do we have data corruption recipe for this OSD?
  // \todo c++20: use contains()
  CorruptFunc relevant_fix = crpt_do_nothing;

  auto p = obj_versions.corrupt_funcs->find(osd_num);
  if (p != obj_versions.corrupt_funcs->end()) {
    // yes, we have a corruption recepie for this OSD
    // \todo c++20: use at()
    relevant_fix = p->second;
  }


  // RRR should we keep the corrupted objects?
  for (auto obj_version : obj_versions.real_versions) {  // note: a copy

    obj_version = relevant_fix(obj_version, osd_num);
    std::cout << fmt::format("{}: osd:{} gh:{} key:{}\n",
                             __func__,
                             osd_num,
                             17 /*obj_version.ghobj.hobj.oid*/,
                             obj_version.ghobj.hobj.get_key());

    auto entry = make_smap_entry(obj_versions, obj_version, osd_num);
    std::cout << fmt::format("{}: osd:{} smap entry: {} {}\n",
                             __func__,
                             osd_num,
                             entry.smobj.size,
                             entry.smobj.attrs.size());
    map.objects[entry.ghobj.hobj] = entry.smobj;
  }
  std::cout << fmt::format("{}: done\n", __func__);
}

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
