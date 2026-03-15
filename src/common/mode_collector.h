// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file  A (small) container for fast mode lookups
 *   ('mode' here is the statistical mode of a set of values, i.e. the
 *   value that appears most frequently in the set).
 */

#include <fmt/format.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cstddef>
#include <functional>
#include <memory_resource>
#include <optional>
#include <ranges>
#include <unordered_map>

/**
 * ModeCollector is designed to collect a set of values (e.g. - the data digest
 * reported by each replica), associating each value with an object ID (in our
 * example - the replica ID), and efficiently finding the mode (the value that
 * appears most frequently) of the collected values.
 *
 * The template parameters are:
 * - ObjIdT: The type of the object ID (e.g., replica ID).
 * - K: The type of the value being collected.
 * - HshT: The hash function for K, to be used with the unordered_map.
 *   Note: if HshT is std::identity, then K must fit in size_t.
 * - MAX_ELEM is used to calculate the estimated memory footprint of the
 *   unordered_map.
 *
 * ModeCollector uses a monotonic buffer resource to manage memory
 * efficiently, avoiding frequent allocations and deallocations.
 * My tests (see link for details and caveats) show that using the PMR
 * allocator speeds up the mode-finding process by 20% to 40%.
 */

struct ModeFinder {

  /// a 'non-templated' version of mode_status_t, to simplify usage.
  enum class mode_status_t {
    no_mode_value,     ///< No clear victory for any value
    mode_value,        ///< we have a winner, but it appears in less than half
                       ///< of the samples
    authorative_value  ///< more than half of the samples are of the same value
  };
};

/// Standalone result type so that fmt::formatter can deduce template params.
template <typename ObjIdT, typename K>
struct ModeCollectorResult {
  /// do we have a mode value?
  ModeFinder::mode_status_t tag;
  /// the mode value (if any)
  K key;
  /// an object ID, "arbitrary" selected from the set of objects that
  /// reported the mode value
  ObjIdT id;
  /// the number of times the mode value was reported
  size_t count;
  auto operator<=>(const ModeCollectorResult& rhs) const = default;
};

// note the use of std::identity: it's a pretty fast hash function,
// but we are restricted to size_t sized keys (per stdlib implementation
// of the unrdered map).

template <
    typename ObjIdT,  ///< how to identify the object that reported a value
    typename K,       ///< the type of the value being collected
    typename HshT = std::identity,  ///< the hash function for K
    int MAX_ELEM = 12>
  requires(
      std::invocable<HshT, K> &&
      sizeof(std::invoke_result_t<HshT, K>) <= sizeof(size_t))
class ModeCollector : public ModeFinder {
  struct node_type_t {
    size_t m_count{0};
    ObjIdT m_id;  ///< Stores the object ID associated with this value
  };

  // estimated (upper limit) memory footprint of the unordered_map
  // vvvvvvvvvvvvvvvvvvvvvvvvvvvv
  // Bucket array: typically 2x num_elements for good load factor
  static const size_t BUCKET_ARRAY_SIZE = (MAX_ELEM * 2) * sizeof(void*);
  // Node storage: each elem needs hash + next-ptr
  static constexpr size_t NODE_OVERHEAD = sizeof(void*) + sizeof(size_t);
  static constexpr size_t NODE_STORAGE =
      MAX_ELEM * (sizeof(K) + sizeof(node_type_t) + NODE_OVERHEAD);
  // PMR allocator overhead (alignment, bookkeeping)
  static constexpr size_t PMR_OVERHEAD_PER_ALLOC = 16;  // typical
  // bucket array + nodes
  static constexpr size_t TOTAL_OVERHEAD = PMR_OVERHEAD_PER_ALLOC * 2;
  static constexpr size_t ESTIMATED_MEMORY_FOOTPRINT =
      BUCKET_ARRAY_SIZE + NODE_STORAGE + TOTAL_OVERHEAD;
  // ^^^^^^^^^^^^^^^^^^^^^^^^^^^^

  std::array<std::byte, ESTIMATED_MEMORY_FOOTPRINT> m_buffer;
  std::pmr::monotonic_buffer_resource m_mbr{m_buffer.data(), m_buffer.size()};

  /// Map to store the occurrence count of each value
  std::pmr::unordered_map<
      K,
      node_type_t,
      HshT,
      std::equal_to<K> >
      m_frequency_map;

  /// Actual count of elements added
  size_t m_actual_count{0};

 public:
  using mode_status_t = ModeFinder::mode_status_t;

  using results_t = ModeCollectorResult<ObjIdT, K>;

  explicit ModeCollector() : m_frequency_map(&m_mbr)
  {
    m_frequency_map.reserve(MAX_ELEM);
  }

  /// Add a value to the collector
  void insert(const ObjIdT& obj, const K& value) noexcept
  {
    auto& node = m_frequency_map[value];
    node.m_count++;
    // Store the object ID associated with this value
    // (note: it's OK to overwrite the ID here)
    node.m_id = obj;
    m_actual_count++;
  }

  /// Overload accepting an optional key. If the key is not present,
  /// the sample is silently ignored (not counted).
  void insert(const ObjIdT& obj, const std::optional<K>& value) noexcept
  {
    if (value.has_value()) {
      insert(obj, *value);
    }
  }


  /**
   * Find the mode of the collected values
   *
   * Note: we are losing ~4% performance due to find_mode() not being noexcept.
   */
  results_t find_mode()
  {
    if (m_frequency_map.empty()) {
      return {mode_status_t::no_mode_value, K{}, ObjIdT{}, 0};
    }

    auto max_elem = std::ranges::max_element(
        m_frequency_map, {},
        [](const auto& pair) { return pair.second.m_count; });

    // Check for clear victory
    if (max_elem->second.m_count > m_actual_count / 2) {
      return {
          mode_status_t::authorative_value, max_elem->first,
          max_elem->second.m_id, max_elem->second.m_count};
    }

    // Check for possible ties
    const auto max_elem_cnt = max_elem->second.m_count;

    max_elem->second.m_count = 0;  // Reset the count of the max element
    const auto second_best_elem = std::ranges::max_element(
        m_frequency_map, {},
        [](const auto& pair) { return pair.second.m_count; });
    max_elem->second.m_count = max_elem_cnt;  // Restore the count

    if (second_best_elem->second.m_count == max_elem_cnt) {
      return {
          mode_status_t::no_mode_value, max_elem->first, max_elem->second.m_id,
          max_elem_cnt};
    }

    return {
        mode_status_t::mode_value, max_elem->first, max_elem->second.m_id,
        max_elem_cnt};
  }
};


template <typename ObjIdT, typename K>
    requires requires (K const& x) { fmt::format("{}", x); }
struct fmt::formatter<ModeCollectorResult<ObjIdT, K>> : fmt::formatter<std::string_view> {
  auto format(const ModeCollectorResult<ObjIdT, K>& p, fmt::format_context& ctx) const
      -> fmt::format_context::iterator
  {
    auto s = [&]() -> std::string {
      switch (p.tag) {
        case ModeFinder::mode_status_t::no_mode_value:
          return fmt::format("no_mode_value");
        case ModeFinder::mode_status_t::mode_value:
          return fmt::format("mode_value: key={}, id={}, count={}", p.key, p.id, p.count);
        case ModeFinder::mode_status_t::authorative_value:
          return fmt::format("authorative_value: key={}, id={}, count={}", p.key, p.id, p.count);
      }
      return "unknown";
    }();
    return fmt::formatter<std::string_view>::format(s, ctx);
  }
};


