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

  TestPg(SharedPGPool pool, pg_info_t& pginfo, pg_shard_t pshard)
      : m_pool{pool}
      , m_info{pginfo}
      , m_pshard{pshard}
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
  LogChannelRef get_logger() const final;
  bool is_primary() const final { return m_primary; }
  spg_t get_pgid() const final;
  const OSDMapRef& get_osdmap() const final { return m_osdmap; }
  void add_to_stats(const object_stat_sum_t& stat) final { m_stats.add(stat); }
  void submit_digest_fixes(const digests_fixes_t& fixes) final {}


  bool m_primary{true};
  spg_t m_spg;
  // CephContext* m_cct;
  LogChannelRef m_logger;
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
  std::set<pg_shard_t> m_acting{0,1,2};
};

// note: the actual owner of the OSD "objects" that are used by
// the mockers
class TestTScrubberBe : public ::testing::Test {
 public:
  TestTScrubberBe(/*spg_t spg, pg_shard_t me, std::set<pg_shard_t> acting*/)
      : i_am{me}
      , acting{acting}
  {
    // create the OSDMap

    osdmap = setup_map(num_osds, pool_conf);

    // extract the pool from the osdmap

    int64_t pid = osdmap->lookup_pg_pool_name(pool_conf.name);
    const pg_pool_t* ext_pool_info = osdmap->get_pg_pool(pool_id);

    pool = std::make_shared<PGPool>(osdmap, ext_pool_info, pool_conf.name);

    // now we can create the main mockers

    // the "PgScrubber"
    test_scrubber = std::make_unique<TestScrubber>(spg, osdmap, logger);

    // the "Pg" (and its backend)
    test_pg = std::make_unique<TestPg>(pool, test_scrubber->get_pg_info(), me);
  }

  void SetUp() override;
  void TearDown() override;

 public:
  std::unique_ptr<TestScrubBackend> sbe;

 private:
  // I am the primary
  int num_osds{3};
  // std::string pool_name{"rep_pool"};

  pg_shard_t i_am;
  std::set<pg_shard_t> acting;

  test_pool_conf_t pool_conf;
  int64_t pool_id;
  pg_pool_t pool_info;

  // OSDMap osdmap;
  OSDMapRef osdmap;

  std::shared_ptr<PGPool> pool;
  pg_info_t info;


  //CephContext* cct{nullptr};
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
  pg_info_t info;

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


  /*
          info.pgid = spg;
          info.last_update = osdmap->get_epoch();
          info.last_complete = osdmap->get_epoch();
          info.last_user_version = 1;
          info.purged_snaps = {};
          info.last_user_version = 1;
          info.last_osdmap_epoch = osdmap->get_epoch();
          info.history.last_epoch_clean = osdmap->get_epoch();
          info.history.last_epoch_split = osdmap->get_epoch();
          info.history.last_epoch_marked_full = osdmap->get_epoch();
          info.history.last_epoch_marked_removed = osdmap->get_epoch();
          info.last_backfill = hobject_t::get_max();
          info.stats.stats.sum.num_objects_degraded = 0;
          info.stats.stats.sum.num_objects_misplaced = 0;
          info.stats.stats.sum.num_objects_unfound = 0;
          info.stats.stats.sum.num_bytes_used = 0;
          info.stats.stats.sum.num_bytes = 0;
          info.stats.stats.sum.num_objects = 0;
          info.stats.stats.sum.num_object_clones = 0;
          info.stats.stats.sum.num_object_copies = 0;
          info.stats.stats.sum.num_objects_missing_on_primary = 0;
          info.stats.stats.sum.num_objects_degraded = 0;
          info.stats.stats.sum.num_objects_misplaced = 0;
          info.stats.stats.sum.num_objects_unfound = 0;
          info.stats.stats.sum.num_bytes_used = 0;
          info.stats.stats.sum.num_bytes = 0;
          info.stats.stats.sum.num_objects = 0;
  */
}

void TestTScrubberBe::SetUp()
{
  sbe = std::make_unique<TestScrubBackend>(test_scrubber,
                                           test_pg,
                                           i_am,
                                           false,
                                           scrub_level_t::deep,
                                           acting);
}

void TestTScrubberBe::TearDown() {}

// some basic sanity checks

TEST_F(TestTScrubberBe, creation_1)
{
  // copy some osdmap tests from TestOSDMap.cc



  ASSERT_TRUE(sbe);
  ASSERT_FALSE(sbe->get_m_repair());
  sbe->update_repair_status(true);
  ASSERT_TRUE(sbe->get_m_repair());
}


// whitebox testing (OK if failing after a change to the backend internals)
.

// blackbox testing - testing the published functionality
// (should not depend on internals of the backend)


#if 0
class TestOSDScrub : public OSD {

  //  public:
  //   static std::unique_ptr<TestOSDScrub> create(int osd_id);

 public:
  TestOSDScrub(std::unique_ptr<ObjectStore> store,
               int id,
               Messenger* msgrs,
               Messenger* osdc_messenger,
               MonClient* mc,
               const std::string& dev,
               const std::string& jdev,
               ceph::async::io_context_pool& ictx)
      : OSD(g_ceph_context,
            std::move(store),
            id,
            msgrs,
            msgrs,
            msgrs,
            msgrs,
            msgrs,
            msgrs,
            osdc_messenger,
            mc,
            dev,
            jdev,
            ictx)

  {}

  bool scrub_time_permit(utime_t now)
  {
    return service.get_scrub_services().scrub_time_permit(now);
  }
};

using test_osd_t = std::unique_ptr<TestOSDScrub>;

class TestTScrubberBe : public ::testing::Test {
 public:
  // TestTScrubberBe(int num_osd) {}  // for now - handle only 1

  void SetUp() override;
  void TearDown() override;

  //~TestTScrubberBe() { m_osd.reset() ; }
 public:
  std::string cluster_msgr_type{g_conf()->ms_cluster_type.empty()
                                  ? g_conf().get_val<std::string>("ms_type")
                                  : g_conf()->ms_cluster_type};
  //   Messenger* ms = Messenger::create(g_ceph_context,
  //                                     cluster_msgr_type,
  //                                     entity_name_t::OSD(0), // verify the '0'
  //                                     "make_checker",
  //                                     getpid());

  ceph::async::io_context_pool m_icp{1};
  std::unique_ptr<ObjectStore> m_store;

  MonClient* mc;

  test_osd_t m_osd;

  test_osd_t create_member_osd(int osd_id);
};


void TestTScrubberBe::SetUp()
{

  g_ceph_context->_conf.set_val("osd_fast_shutdown", "false");
  //16:03//g_ceph_context->_conf.set_val("osd_fast_shutdown", "0");
  m_osd = create_member_osd(0);
}

void TestTScrubberBe::TearDown()
{
  //delete mc;
  //std::cout << "TearDown" << std::endl;
  //sleep(2);
  // m_osd->shutdown();
  //m_icp.finish();
  //std::cout << "micp" << std::endl;
  //sleep(2);
  // must not: not started m_osd->service.agent_stop();
  //m_osd->shutdown();
  std::cout << "stop" << std::endl;
   m_icp.finish();
  sleep(1);
  try {
    //m_osd->shutdown();
    m_osd.reset();
  } catch (...) {
    std::cout << "exception" << std::endl;
  }
  //EXPECT_EXIT(m_osd.reset(),  ::testing::ExitedWithCode(0), "osd down");
  //auto xxx = m_osd.release();
  std::cout << "mosd" << std::endl;
  sleep(2);
}

#if 0
void TestTScrubberBe::TearDown()
{
  //delete mc;
  //std::cout << "TearDown" << std::endl;
  //sleep(2);
  // m_osd->shutdown();
  m_icp.finish();
  //std::cout << "micp" << std::endl;
  sleep(2);
  // must not: not started m_osd->service.agent_stop();
  m_osd->shutdown();
  std::cout << "stop" << std::endl;
  sleep(2);
  m_osd.reset();
  std::cout << "mosd" << std::endl;
  sleep(2);
}

void TestTScrubberBe::TearDown()
{
  delete mc;
  std::cout << "TearDown" << std::endl;
  sleep(2);
  // m_osd->shutdown();
  m_icp.finish();
  std::cout << "micp" << std::endl;
  sleep(2);
  // must not: not started m_osd->service.agent_stop();
  m_osd->shutdown();
  std::cout << "stop" << std::endl;
  sleep(2);
  m_osd.reset();
  std::cout << "mosd" << std::endl;
  sleep(2);
}
#endif

test_osd_t TestTScrubberBe::create_member_osd(int osd_id)
{
  // create a new ObjectStore
  m_store = ObjectStore::create(g_ceph_context,
                                g_conf()->osd_objectstore,
                                g_conf()->osd_data,
                                g_conf()->osd_journal);

  // will we need access to the messenger?
  Messenger* ms = Messenger::create(g_ceph_context,
                                    cluster_msgr_type,
                                    entity_name_t::OSD(0),  // verify the '0'
                                    "make_checker",
                                    getpid());

  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms->set_default_policy(Messenger::Policy::stateless_server(0));
  ms->bind(g_conf()->public_addr);

  mc = new MonClient(g_ceph_context, m_icp);
  mc->build_initial_monmap();


  // create a new OSD
  return std::make_unique<TestOSDScrub>(std::move(m_store),
                                        osd_id,
                                        ms,
                                        ms,
                                        mc,
                                        g_conf()->osd_data,
                                        g_conf()->osd_journal,
                                        m_icp);
}


TEST_F(TestTScrubberBe, tac)
{
  // TestTScrubberBe* sbt = new TestTScrubberBe(1);


  ASSERT_TRUE(m_osd);

  // sbt.m_osd->shutdown();
  // sbt->m_osd.reset();
  // delete sbt.mc;
  sleep(1);
  // sbt.m_icp.stop();
  // sleep(2);
  ASSERT_TRUE(true);
}

TEST_F(TestTScrubberBe, tac2)
{
  // TestTScrubberBe* sbt = new TestTScrubberBe(1);


  ASSERT_TRUE(m_osd);

  // sbt.m_osd->shutdown();
  // sbt->m_osd.reset();
  // delete sbt.mc;
  sleep(1);
  // sbt.m_icp.stop();
  // sleep(2);
  ASSERT_TRUE(true);
}


#if 0
// GOOD!
TEST(test_scrubber_be, tac)
{
  TestTScrubberBe* sbt = new TestTScrubberBe(1);

  sbt->m_osd = sbt->create_member_osd(0);

  ASSERT_TRUE(sbt->m_osd);

  //sbt.m_osd->shutdown();
  //sbt->m_osd.reset(); 
  //delete sbt.mc;
  sleep(2);
  //sbt.m_icp.stop();
  //sleep(2);
  ASSERT_TRUE(true);
}

// failed
TEST(test_scrubber_be, tad)
{
  TestTScrubberBe* sbt = new TestTScrubberBe(1);

  sbt->m_osd = sbt->create_member_osd(0);

  ASSERT_TRUE(sbt->m_osd);

  //sbt.m_osd->shutdown();
  //sbt->m_osd.reset(); 
  //delete sbt.mc;
  sleep(2);
  //sbt.m_icp.stop();
  //sleep(2);
  ASSERT_TRUE(true);
  delete sbt;
}

TEST(test_scrubber_be, tae)
{
  TestTScrubberBe* sbt = new TestTScrubberBe(1);

  sbt->m_osd = sbt->create_member_osd(0);

  ASSERT_TRUE(sbt->m_osd);

  //sbt.m_osd->shutdown();
  //sbt->m_osd.reset(); 
  //delete sbt.mc;
  sleep(2);
  //sbt.m_icp.stop();
  //sleep(2);
  ASSERT_TRUE(true);
  delete sbt;
}


TEST(test_scrubber_be, taa)
{
  TestTScrubberBe* sbt = new TestTScrubberBe(1);

  sbt->m_osd = sbt->create_member_osd(0);

  ASSERT_TRUE(sbt->m_osd);

  //sbt.m_osd->shutdown();
  sbt->m_osd.reset(); 
  //delete sbt.mc;
  sleep(2);
  //sbt.m_icp.stop();
  //sleep(2);
  ASSERT_TRUE(true);
}

TEST(test_scrubber_be, tab)
{
  TestTScrubberBe* sbt = new TestTScrubberBe(1);

  sbt->m_osd = sbt->create_member_osd(0);

  ASSERT_TRUE(sbt->m_osd);

  sbt->m_osd->shutdown();
  sbt->m_osd.reset(); 
  //delete sbt.mc;
  sleep(2);
  //sbt.m_icp.stop();
  //sleep(2);
  ASSERT_TRUE(true);
}


TEST(test_scrubber_be, t0)
{
  TestTScrubberBe sbt{1};

  sbt.m_osd = sbt.create_member_osd(0);

  ASSERT_TRUE(sbt.m_osd);

  //sbt.m_osd->shutdown();
  sbt.m_osd.reset(); 
  //delete sbt.mc;
  sleep(2);
  //sbt.m_icp.stop();
  //sleep(2);
  ASSERT_TRUE(true);
}


TEST(test_scrubber_be, t1)
{
  TestTScrubberBe sbt{0};

  sbt.m_osd = sbt.create_member_osd(0);

  ASSERT_TRUE(sbt.m_osd);

  sbt.m_osd->shutdown();
  sbt.m_osd.reset(); 
  delete sbt.mc;
  sleep(2);
  sbt.m_icp.stop();
  sleep(2);
  ASSERT_TRUE(true);
}

TEST(test_scrubber_be, t2)
{
  TestTScrubberBe sbt{0};

  sbt.m_osd = sbt.create_member_osd(0);

  ASSERT_TRUE(sbt.m_osd);
   bool ret = sbt.m_osd->scrub_time_permit(utime_t{});
  ASSERT_TRUE(ret);
  sleep(2);

  //sbt.m_osd->shutdown();
  sbt.m_osd.reset(); 
  delete sbt.mc;
  //sleep(2);
  sbt.m_icp.stop();
  sleep(2);
  ASSERT_TRUE(true);
}


TEST(test_scrubber_be, t3)
{
  TestTScrubberBe sbt{0};

  sbt.m_osd = sbt.create_member_osd(0);

  ASSERT_TRUE(sbt.m_osd);

  sbt.m_osd->shutdown();
  sbt.m_osd.reset(); 
  delete sbt.mc;
  sleep(2);
  //sbt.m_icp.stop();
  sleep(2);
}

#if 1

TEST(test_scrubber_be, is_it_there)
{
  TestTScrubberBe sbt{0};

  sbt.m_osd = sbt.create_member_osd(0);

  ASSERT_TRUE(sbt.m_osd);
  sbt.m_osd->shutdown();
  delete sbt.mc;
  sleep(2);
  sbt.m_icp.stop();
  sleep(2);
}
#endif


#endif


#if 0
class SbeTestOsd : public OSD {

 public:
  static std::unique_ptr<SbeTestOsd> create(int osd_id);

 public:
  SbeTestOsd(/*CephContext *cct,*/
             std::unique_ptr<ObjectStore> store,
             int id,
             Messenger* msgrs,
             Messenger* osdc_messenger,
             MonClient* mc,
             const std::string& dev,
             const std::string& jdev/*,
             ceph::async::io_context_pool& ictx*/)
      : OSD(g_ceph_context,
            std::move(store),
            id,
            msgrs,
            msgrs,
            msgrs,
            msgrs,
            msgrs,
            msgrs,
            osdc_messenger,
            mc,
            dev,
            jdev,
            icp)

  {}

  void setup();


  // private:

  ceph::async::io_context_pool icp{1};
  std::string cluster_msgr_type{g_conf()->ms_cluster_type.empty()
                                  ? g_conf().get_val<std::string>("ms_type")
                                  : g_conf()->ms_cluster_type};
  Messenger* ms = Messenger::create(g_ceph_context,
                                    cluster_msgr_type,
                                    entity_name_t::OSD(0),
                                    "make_checker",
                                    getpid());
};

std::unique_ptr<SbeTestOsd> SbeTestOsd::create(int osd_id)
{
  ms->set_cluster_protocol(CEPH_OSD_PROTOCOL);
  ms->set_default_policy(Messenger::Policy::stateless_server(0));  // RRR verify the '0'
  ms->bind(g_conf()->public_addr);

  MonClient mc{g_ceph_context, icp};
  mc.build_initial_monmap();

  std::unique_ptr<ObjectStore> store = ObjectStore::create(g_ceph_context,
                                                           g_conf()->osd_objectstore,
                                                           g_conf()->osd_data,
                                                           g_conf()->osd_journal);

  return std::make_unique<SbeTestOsd>(std::move(store), osd_id, ms, ms, &mc, "", "");
}


void SbeTestOsd::setup()
{
  // create a pool

}

TEST(test_scrubber_be, is_it_there)
{
  auto an_osd = SbeTestOsd::create(0);
  ASSERT_TRUE(an_osd);
}

#endif

#if 0
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
#endif

#endif

// Local Variables:
// compile-command: "cd ../.. ; make unittest_osdscrub ; ./unittest_osdscrub
// --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* " End:
