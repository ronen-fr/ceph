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
  const ScrubGenerator::TargetSmObject& blueprint)
{
  ScrubMap::object obj{};


  return obj;
}

ScrubGenerator::SmapEntry ScrubGenerator::make_smap_entry(
  const ScrubGenerator::RealObj& blueprint,
  int osd_num)
{
  using namespace ScrubGenerator;

  SmapEntry entry{};
  // obj.attrs = blueprint.attrs;

  return entry;
}


void ScrubGenerator::add_object(ScrubMap& map,
                              const ScrubGenerator::RealObj& obj_versions,
                              int osd_num)
{
  using namespace ScrubGenerator;

  // RRR we should be creating multiple entries

  auto smp_entry = make_smap_entry(obj_versions, osd_num);
  map.objects[smp_entry.ghobj.hobj] = smp_entry.smobj;
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
// void ScrubGenerator::add_objects(ScrubMap& map, const ScrubGenerator::RealObj&
// obj_versions, int osd_num)
// {
//   using namespace ScrubGenerator;
//
//   // RRR we should be creating multiple entries
//
//   auto smp_obj = make_smobjects(obj_versions, osd_num);
//   map.objects[smp_obj.] = smp_obj;
// }
