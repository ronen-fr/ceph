// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <gtest/gtest.h>
#include <signal.h>
#include <stdio.h>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "global/global_context.h"
#include "global/global_init.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "os/ObjectStore.h"
#include "osd/PG.h"
#include "osd/PGBackend.h"
#include "osd/PrimaryLogPG.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/pg_scrubber.h"
#include "osd/scrubber/scrub_backend.h"


#define FRIEND_TEST(test_case_name, test_name) \
  friend class test_case_name##_##test_name##_Test

// testing isolated parts of the Scrubber backend


using namespace std;

int main(int argc, char** argv)
{
  map<string, string> defaults = {
    // make sure we have 3 copies, or some tests won't work
    {"osd_pool_default_size", "3"},
    // our map is flat, so just try and split across OSDs, not hosts or whatever
    {"osd_crush_chooseleaf_type", "0"},
  };
  std::vector<const char*> args(argv, argv + argc);
  auto cct = global_init(&defaults,
                         args,
                         CEPH_ENTITY_TYPE_CLIENT,
                         CODE_ENVIRONMENT_UTILITY,
                         CINIT_FLAG_NO_DEFAULT_CONFIG_FILE);
  common_init_finish(g_ceph_context);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}


class TestScrubBackend : public ScrubBackend {
 public:
  TestScrubBackend(ScrubBeListener& scrubber,
                   PgScrubBeListener& pg,
                   pg_shard_t i_am,
                   bool repair,
                   scrub_level_t shallow_or_deep,
                   const std::set<pg_shard_t>& acting)
      : ScrubBackend(scrubber, pg, i_am, repair, shallow_or_deep, acting)
  {}

  // FRIEND_TEST(TestTScrubberBe, creation_1);
  bool get_m_repair() const { return m_repair; }
};


using SharedPGPool = std::shared_ptr<PGPool>;

struct test_pool_conf_t {
  int pg_num{3};
  int pgp_num{3};
  int size{3};
  int min_size{3};
  std::string name{"rep_pool"};
};


// mocking the PG
class TestPg : public PgScrubBeListener {
 public:
  ~TestPg() = default;

  TestPg(SharedPGPool pool, pg_info_t& pginfo, pg_shard_t my_osd)
      : m_pool{pool}
      , m_info{pginfo}
      , m_pshard{my_osd}
  {}


  const PGPool& get_pgpool() const final { return *(m_pool.get()); }
  pg_shard_t get_primary() const final { return m_pshard; }
  void force_object_missing(ScrubberPasskey,
                            const std::set<pg_shard_t>& peer,
                            const hobject_t& oid,
                            eversion_t version) final
  {}

  const pg_info_t& get_pg_info(ScrubberPasskey) const final { return m_info; }

  uint64_t logical_to_ondisk_size(uint64_t logical_size) const final
  {
    return logical_size;
  }

  SharedPGPool m_pool;
  pg_info_t& m_info;
  pg_shard_t m_pshard;
};

// and the scrubber
class TestScrubber : public ScrubBeListener {
 public:
  ~TestScrubber() = default;

  TestScrubber(spg_t spg,
               // CephContext* cct,
               OSDMapRef osdmap,
               LogChannelRef logger)
      : m_spg{spg}  //, m_cct{cct}
      , m_logger{logger}
      , m_osdmap{osdmap}
  {}


  std::ostream& gen_prefix(std::ostream& out) const final { return out; }

  CephContext* get_pg_cct() const final { return g_ceph_context; }

  CephContext* get_osd_cct() const final { return g_ceph_context; }

  LogChannelRef get_logger() const final
  { /* RRR */
    return m_logger;
  }

  bool is_primary() const final { return m_primary; }

  spg_t get_pgid() const final { return m_info.pgid; }

  const OSDMapRef& get_osdmap() const final { return m_osdmap; }

  void add_to_stats(const object_stat_sum_t& stat) final { m_stats.add(stat); }

  void submit_digest_fixes(const digests_fixes_t& fixes) final {}


  bool m_primary{true};
  spg_t m_spg;
  LogChannelRef m_logger{nullptr};
  OSDMapRef m_osdmap;
  pg_info_t m_info;
  object_stat_sum_t m_stats;
  // digests_fixes_t m_digest_fixes;
};


// parameters for TestTScrubberBe construction (until
// I've learned how to use gtest's constructor-arguments)

struct TestTScrubberBeParams {
  pg_shard_t i_am;
  bool m_primary{true};
  bool m_repair{false};
  scrub_level_t m_shallow_or_deep{scrub_level_t::deep};
  // std::set<pg_shard_t> m_acting{0,1,2};
};

// note: the actual owner of the OSD "objects" that are used by
// the mockers
class TestTScrubberBe : public ::testing::Test {
 public:
  TestTScrubberBe()
  {
    // create the OSDMap

    osdmap = setup_map(num_osds, pool_conf);

    std::cout << "osdmap: " << *osdmap << std::endl;

    // extract the pool from the osdmap

    pool_id = osdmap->lookup_pg_pool_name(pool_conf.name);
    const pg_pool_t* ext_pool_info = osdmap->get_pg_pool(pool_id);
    pool =
      std::make_shared<PGPool>(osdmap, pool_id, *ext_pool_info, pool_conf.name);

    std::cout << "pool: " << pool->info << std::endl;

    // a PG in that pool?
    info = setup_pg_in_map();
    std::cout << "info: " << info << std::endl;

    // now we can create the main mockers

    // the "PgScrubber"
    test_scrubber = std::make_unique<TestScrubber>(spg, osdmap, logger);

    // the "Pg" (and its backend)
    test_pg = std::make_unique<TestPg>(pool, info, i_am);
  }

  ~TestTScrubberBe() = default;

  void SetUp() override;
  void TearDown() override;

 public:
  std::unique_ptr<TestScrubBackend> sbe;

 private:
  // I am the primary
  int num_osds{3};
  // std::string pool_name{"rep_pool"};

  spg_t spg;

  pg_shard_t i_am;  // my osd and no shard
  std::set<pg_shard_t> acting;

  test_pool_conf_t pool_conf;
  int64_t pool_id;
  pg_pool_t pool_info;

  // OSDMap osdmap;
  OSDMapRef osdmap;

  std::shared_ptr<PGPool> pool;
  pg_info_t info;


  // CephContext* cct{nullptr};
  std::unique_ptr<TestScrubber> test_scrubber;
  LogChannelRef logger;
  std::unique_ptr<TestPg> test_pg;

 private:
  // void setup_map(int num_osds, const test_pool_conf_t& pconf);
  OSDMapRef setup_map(int num_osds, const test_pool_conf_t& pconf);

  pg_info_t setup_pg_in_map();
};

// copied from TestOSDMap.cc
OSDMapRef TestTScrubberBe::setup_map(int num_osds,
                                     const test_pool_conf_t& pconf)
{
  auto osdmap = std::make_shared<OSDMap>();
  uuid_d fsid;
  osdmap->build_simple(g_ceph_context, 0, fsid, num_osds);
  OSDMap::Incremental pending_inc(osdmap->get_epoch() + 1);
  pending_inc.fsid = osdmap->get_fsid();
  entity_addrvec_t sample_addrs;
  sample_addrs.v.push_back(entity_addr_t());
  uuid_d sample_uuid;
  for (int i = 0; i < num_osds; ++i) {
    sample_uuid.generate_random();
    sample_addrs.v[0].nonce = i;
    pending_inc.new_state[i] = CEPH_OSD_EXISTS | CEPH_OSD_NEW;
    pending_inc.new_up_client[i] = sample_addrs;
    pending_inc.new_up_cluster[i] = sample_addrs;
    pending_inc.new_hb_back_up[i] = sample_addrs;
    pending_inc.new_hb_front_up[i] = sample_addrs;
    pending_inc.new_weight[i] = CEPH_OSD_IN;
    pending_inc.new_uuid[i] = sample_uuid;
  }
  osdmap->apply_incremental(pending_inc);

  // create a replicated pool
  OSDMap::Incremental new_pool_inc(osdmap->get_epoch() + 1);
  new_pool_inc.new_pool_max = osdmap->get_pool_max();
  new_pool_inc.fsid = osdmap->get_fsid();
  uint64_t pool_id = ++new_pool_inc.new_pool_max;
  pg_pool_t empty;
  auto p = new_pool_inc.get_new_pool(pool_id, &empty);
  p->size = pconf.size;
  p->set_pg_num(pconf.pg_num);
  p->set_pgp_num(pconf.pgp_num);
  p->type = pg_pool_t::TYPE_REPLICATED;
  p->crush_rule = 0;
  p->set_flag(pg_pool_t::FLAG_HASHPSPOOL);
  new_pool_inc.new_pool_names[pool_id] = pconf.name;
  osdmap->apply_incremental(new_pool_inc);
  return osdmap;
}

pg_info_t TestTScrubberBe::setup_pg_in_map()
{

  pg_t rawpg(0, pool_id);
  pg_t pgid = osdmap->raw_pg_to_pg(rawpg);
  vector<int> up_osds;
  vector<int> acting_osds;
  int up_primary;
  int acting_primary;

  osdmap->pg_to_up_acting_osds(pgid,
                               &up_osds,
                               &up_primary,
                               &acting_osds,
                               &acting_primary);

  std::cout << fmt::format(
    "{}: pg: {} up_osds: {} up_primary: {} acting_osds: {} acting_primary: "
    "{}\n",
    __func__,
    pgid,
    up_osds,
    up_primary,
    acting_osds,
    acting_primary);

  spg = spg_t{pgid};  // 0 /*static_cast<int8_t>(acting_primary)}};
  i_am = pg_shard_t{up_primary};
  std::cout << fmt::format("{}: spg: {}  and I am {}\n", __func__, spg, i_am);

  pg_info_t info;
  info.pgid = spg;
  // info.last_update = osdmap->get_epoch();
  // info.last_complete = osdmap->get_epoch();
  info.last_user_version = 1;
  info.purged_snaps = {};
  info.last_user_version = 1;
  // info.last_osdmap_epoch = osdmap->get_epoch();
  info.history.last_epoch_clean = osdmap->get_epoch();
  info.history.last_epoch_split = osdmap->get_epoch();
  info.history.last_epoch_marked_full = osdmap->get_epoch();
  // info.history.last_epoch_marked_removed = osdmap->get_epoch();
  info.last_backfill = hobject_t::get_max();
  info.stats.stats.sum.num_objects_degraded = 0;
  info.stats.stats.sum.num_objects_misplaced = 0;
  info.stats.stats.sum.num_objects_unfound = 0;
  info.stats.stats.sum.num_objects = 0;
  info.stats.stats.sum.num_object_clones = 0;
  info.stats.stats.sum.num_object_copies = 0;
  info.stats.stats.sum.num_objects_missing_on_primary = 0;
  info.stats.stats.sum.num_objects_degraded = 0;
  info.stats.stats.sum.num_objects_misplaced = 0;
  info.stats.stats.sum.num_objects_unfound = 0;
  // info.stats.stats.sum.num_bytes_used = 0;
  info.stats.stats.sum.num_bytes = 0;
  info.stats.stats.sum.num_objects = 0;
  return info;
}

void TestTScrubberBe::SetUp()
{
    sbe = std::make_unique<TestScrubBackend>(*test_scrubber,
                                             *test_pg,
                                             i_am,
                                             /* repair? */ false,
                                             scrub_level_t::deep,
                                             acting);
}

void TestTScrubberBe::TearDown() {}

// some basic sanity checks
// (mainly testing the constructor)

TEST_F(TestTScrubberBe, creation_1)
{
  // copy some osdmap tests from TestOSDMap.cc


  ASSERT_TRUE(sbe);
  //   ASSERT_FALSE(sbe->get_m_repair());
  //   sbe->update_repair_status(true);
  //   ASSERT_TRUE(sbe->get_m_repair());
}


// whitebox testing (OK if failing after a change to the backend internals)


// blackbox testing - testing the published functionality
// (should not depend on internals of the backend)


// Local Variables:
// compile-command: "cd ../.. ; make unittest_osdscrub ; ./unittest_osdscrub
// --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* " End:
