// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#include <gtest/gtest.h>
#include <signal.h>
#include <stdio.h>

#include "common/async/context_pool.h"
#include "common/ceph_argparse.h"
#include "mon/MonClient.h"
#include "msg/Messenger.h"
#include "os/ObjectStore.h"
#include "osd/OSD.h"

// testing isolated parts pf the Scrubber backend

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

  bool scrub_time_permit(utime_t now) {
    return service.get_scrub_services().scrub_time_permit(now);
  }
};

using test_osd_t = std::unique_ptr<TestOSDScrub>;

class ScrubberBeTest {
 public:
  ScrubberBeTest(int num_osd) {}  // for now - handle only 1

  ~ScrubberBeTest() { m_osd.reset() ; }
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
  // MonClient mc{g_ceph_context, m_icp};
  std::unique_ptr<ObjectStore> m_store;

  MonClient* mc;

  test_osd_t m_osd;

  test_osd_t create_member_osd(int osd_id);
};


test_osd_t ScrubberBeTest::create_member_osd(int osd_id)
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

// GOOD!
TEST(test_scrubber_be, tac)
{
  ScrubberBeTest* sbt = new ScrubberBeTest(1);

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
  ScrubberBeTest* sbt = new ScrubberBeTest(1);

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
  ScrubberBeTest* sbt = new ScrubberBeTest(1);

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
  ScrubberBeTest* sbt = new ScrubberBeTest(1);

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
  ScrubberBeTest* sbt = new ScrubberBeTest(1);

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
  ScrubberBeTest sbt{1};

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
  ScrubberBeTest sbt{0};

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
  ScrubberBeTest sbt{0};

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
  ScrubberBeTest sbt{0};

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
  ScrubberBeTest sbt{0};

  sbt.m_osd = sbt.create_member_osd(0);

  ASSERT_TRUE(sbt.m_osd);
  sbt.m_osd->shutdown();
  delete sbt.mc;
  sleep(2);
  sbt.m_icp.stop();
  sleep(2);
}
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

// Local Variables:
// compile-command: "cd ../.. ; make unittest_osdscrub ; ./unittest_osdscrub
// --log-to-stderr=true  --debug-osd=20 # --gtest_filter=*.* " End:
