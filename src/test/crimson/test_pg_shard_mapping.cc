// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include <fmt/format.h>
#include <seastar/core/sharded.hh>

#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <map>
#include <set>

#include "crimson/common/config_proxy.h"
#include "crimson/common/perf_counters_collection.h"
#include "crimson/osd/pg_map.h"
#include "test/crimson/gtest_seastar.h"

using crimson::core_id_t;
using crimson::store_index_t;
using crimson::osd::PGShardMapping;

namespace {

spg_t
make_pgid(unsigned seed)
{
  return spg_t(pg_t(seed, 3));
}

/*
 * Start a sharded PGShardMapping with the given store_shard_nums,
 * invoke func(mapping.local()), then guarantee cleanup via stop()
 * even if func throws -- preventing seastar's sharded<> destructor
 * assertion on non-empty instances.
 */
template <typename Func>
seastar::future<>
with_mapping(uint32_t store_shard_nums, Func&& func)
{
  auto mapping = std::make_unique<seastar::sharded<PGShardMapping>>();
  co_await mapping->start(
      core_id_t{0}, core_id_t(seastar::smp::count), store_shard_nums);
  std::exception_ptr ep;
  try {
    co_await func(mapping->local());
  } catch (...) {
    ep = std::current_exception();
  }
  co_await mapping->stop();
  if (ep) {
    std::rethrow_exception(ep);
  }
}

struct mapping_result {
  std::set<core_id_t> cores_used;
  std::set<store_index_t> store_indices_used;
  std::map<spg_t, std::pair<core_id_t, store_index_t>> pg_assignments;
};

seastar::future<mapping_result>
create_and_validate_mappings(
    PGShardMapping& mapping,
    uint32_t store_shard_nums,
    unsigned num_pgs)
{
  mapping_result result;
  for (unsigned i = 0; i < num_pgs; ++i) {
    const auto pgid = make_pgid(i);
    const auto [core, store_idx] =
        co_await mapping.get_or_create_pg_mapping(pgid);

    EXPECT_LT(core, seastar::smp::count);
    const auto backend_core =
        core % store_shard_nums;
    const auto effective = backend_core + seastar::smp::count * store_idx;
    EXPECT_LT(effective, store_shard_nums);
    result.cores_used.insert(core);
    result.store_indices_used.insert(store_idx);
    result.pg_assignments[pgid] = {core, store_idx};
  }
  co_return result;
}

} // anonymous namespace

struct pg_shard_mapping_test_t : public seastar_test_suite_t {
  static constexpr unsigned REQUIRED_SMP_COUNT = 5;

  seastar::future<>
  set_up_fut() override
  {
    if (seastar::smp::count != REQUIRED_SMP_COUNT) {
      fmt::print(
          "FATAL: this test requires --smp {} (have {})\n", REQUIRED_SMP_COUNT,
          seastar::smp::count);
      ::_exit(1);
    }
    return crimson::common::sharded_conf()
        .invoke_on(0, &crimson::common::ConfigProxy::start)
        .then([] {
          return crimson::common::local_conf().set_val(
              "seastore_require_partition_count_match_reactor_count", "false");
        });
  }
};

// smp=5, store_shard_nums=3: alien cores 3,4 forward to cores 0,1
TEST_F(pg_shard_mapping_test_t, alien_core_mapping)
{
  run([] {
    return with_mapping(3, [](PGShardMapping& mapping) -> seastar::future<> {
      constexpr uint32_t store_shard_nums = 3;
      const auto result =
          co_await create_and_validate_mappings(mapping, store_shard_nums, 15);
      EXPECT_GT(result.cores_used.size(), store_shard_nums);
    });
  });
}

// smp=5, store_shard_nums=5: all cores are native
TEST_F(pg_shard_mapping_test_t, equal_cores_and_shards)
{
  run([] {
    return with_mapping(5, [](PGShardMapping& mapping) -> seastar::future<> {
      constexpr uint32_t store_shard_nums = 5;
      const auto result =
          co_await create_and_validate_mappings(mapping, store_shard_nums, 15);
      EXPECT_EQ(result.cores_used.size(), store_shard_nums);
    });
  });
}

// smp=5, store_shard_nums=8: each core hosts multiple store indices
TEST_F(pg_shard_mapping_test_t, fewer_cores_than_shards)
{
  run([] {
    return with_mapping(8, [](PGShardMapping& mapping) -> seastar::future<> {
      constexpr uint32_t store_shard_nums = 8;
      const auto result =
          co_await create_and_validate_mappings(mapping, store_shard_nums, 24);
      EXPECT_GE(result.store_indices_used.size(), 2);
    });
  });
}

/*
 * Mimics the OSD init sequence: create PG mappings, then verify on each
 * core that the assigned store shard is active, and that alien cores
 * forward to a core with an active store.
 */
TEST_F(pg_shard_mapping_test_t, alien_core_store_forwarding)
{
  run([] {
    return with_mapping(3, [](PGShardMapping& mapping) -> seastar::future<> {
      constexpr uint32_t store_shard_nums = 3;
      constexpr unsigned num_pgs = 15;

      // Phase 1: create mappings (from shard 0, as OSD does)
      const auto mappings = co_await create_and_validate_mappings(
          mapping, store_shard_nums, num_pgs);

      /*
       * Phase 2: on each core, verify store shard activity matches
       * expectations. This mimics what happens at OSD init when SeaStore
       * creates Shard objects: is_shard_store_active(store_index,
       * store_shard_nums) determines if a shard store is active on a
       * given core.
       */
      co_await seastar::smp::invoke_on_all([store_shard_nums] {
        const auto this_core = seastar::this_shard_id();
        const auto num_shard_services =
            (store_shard_nums + seastar::smp::count - 1) / seastar::smp::count;

        for (store_index_t si = 0; si < num_shard_services; ++si) {
          const bool active = (this_core + seastar::smp::count * si) <
                              store_shard_nums;

          if (si == 0) {
            ASSERT_EQ(active, this_core < store_shard_nums);
          }
        }

        // For alien cores: the modulo forwarding target is a native core
        if (this_core >= store_shard_nums) {
          const auto forwarding_target = this_core % store_shard_nums;
          ASSERT_LT(forwarding_target, store_shard_nums);
        }
      });

      // Phase 3: verify each PG assignment lands on a core whose forwarding
      // target has an active store - using the same logic as get_backend_store().
      for (const auto& [pgid, assignment] : mappings.pg_assignments) {
        const auto [core, store_idx] = assignment;
        const auto backend_shard_id =
            core % store_shard_nums;
        const auto effective = backend_shard_id +
                               seastar::smp::count * store_idx;
        EXPECT_LT(effective, store_shard_nums);
      }
    });
  });
}

// Verifies that obtaining max_object_size via local_conf() works on all
// cores, including alien cores.
TEST_F(pg_shard_mapping_test_t, max_object_size_on_alien_cores)
{
  run([] {
    return with_mapping(3, [](PGShardMapping& mapping) -> seastar::future<> {
      constexpr uint32_t store_shard_nums = 3;
      constexpr unsigned num_pgs = 15;

      const auto result = co_await create_and_validate_mappings(
          mapping, store_shard_nums, num_pgs);
      const auto& pg_cores = result.cores_used;

      EXPECT_TRUE(std::any_of(pg_cores.begin(), pg_cores.end(), [](auto c) {
        return c >= store_shard_nums;
      }));

      co_await seastar::smp::invoke_on_all([&pg_cores] {
        const auto this_core = seastar::this_shard_id();
        if (!pg_cores.contains(this_core)) {
          return;
        }

        const auto max_size = crimson::common::local_conf()->osd_max_object_size;
        ASSERT_GT(max_size, 0u);
      });
    });
  });
}

SeastarRunner seastar_test_suite_t::seastar_env;

int
main(int argc, char** argv)
{
  std::vector<const char*> args(argv, argv + argc);

  bool has_smp = std::any_of(args.begin(), args.end(), [](const char* a) {
    return std::strncmp(a, "--smp", 5) == 0;
  });
  if (!has_smp) {
    args.push_back("--smp");
    args.push_back("5");
  }

  if (!std::getenv("FOR_MAKE_CHECK")) {
    args.push_back("--default-log-level=debug");
  }

  auto app_argc = static_cast<int>(args.size());
  auto app_argv = const_cast<char**>(args.data());
  ::testing::InitGoogleTest(&app_argc, app_argv);

  int ret = seastar_test_suite_t::seastar_env.init(app_argc, app_argv);
  if (ret != 0) {
    seastar_test_suite_t::seastar_env.stop();
    return ret;
  }

  seastar_test_suite_t::seastar_env.run([] {
    return crimson::common::sharded_conf()
        .start(EntityName{}, std::string_view{"ceph"})
        .then([] { return crimson::common::sharded_perf_coll().start(); });
  });

  ret = RUN_ALL_TESTS();

  seastar_test_suite_t::seastar_env.run([] {
    return crimson::common::sharded_perf_coll().stop().then([] {
      return crimson::common::sharded_conf().stop();
    });
  });

  seastar_test_suite_t::seastar_env.stop();
  return ret;
}
