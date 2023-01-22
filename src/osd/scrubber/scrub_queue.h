// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include "include/expected.hpp"
#include "include/utime.h"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber/scrub_queue_if.h"
#include "osd/scrubber/scrub_resources.h"
#include "osd/scrubber_common.h"

namespace Scrub {
class ScrubSchedListener;
class ScrubJob;
class SchedEntry;
}  // namespace Scrub


// RRR add scrub-sched-loop sequence diagram

struct ScrubQueueStats {
  uint_fast16_t num_ready{0};
  uint_fast16_t num_total{0};
};

struct ScrubQueueImp_IF {
  using SchedEntry = Scrub::SchedEntry;

  using EntryPred = std::function<bool(const SchedEntry&)>;

  virtual void push_entry(const SchedEntry& entry) = 0;

  virtual bool remove_entry(spg_t pgid, scrub_level_t s_or_d) = 0;

  virtual ScrubQueueStats get_stats(utime_t scrub_clock_now) const = 0;

  virtual std::optional<SchedEntry> pop_ready_pg(utime_t scrub_clock_now) = 0;

  virtual void dump_scrubs(ceph::Formatter* f) const = 0;

  virtual std::set<spg_t> get_pgs(EntryPred) const = 0;

  virtual std::vector<SchedEntry> get_entries(EntryPred) const = 0;

  virtual ~ScrubQueueImp_IF() = default;
};


class ScrubQueueImp : public ScrubQueueImp_IF {
  using SchedEntry = Scrub::SchedEntry;
  using SchedulingQueue = std::deque<SchedEntry>;


 public:
  ScrubQueueImp(Scrub::ScrubQueueOps& parent_queue) : parent_queue(parent_queue)
  {}

  void push_entry(const SchedEntry& entry) override;

  bool remove_entry(spg_t pgid, scrub_level_t s_or_d) override;

  ScrubQueueStats get_stats(utime_t scrub_clock_now) const override;

  std::optional<SchedEntry> pop_ready_pg(utime_t scrub_clock_now) override;

  void dump_scrubs(ceph::Formatter* f) const override;

  std::set<spg_t> get_pgs(EntryPred) const override;

  std::vector<SchedEntry> get_entries(EntryPred) const override;

 private:
  SchedulingQueue to_scrub;
  Scrub::ScrubQueueOps& parent_queue;

  // very temporary:
  // sorts the 'ripe' entries (those with 'not earlier than' time
  // in the past) and the future entries separately.
  void normalize_queue(utime_t scrub_clock_now);
};


/**
 * The 'ScrubQueue' is a "sub-component" of the OSD. It is responsible (mainly)
 * for selecting the PGs to be scrubbed, and initiating the scrub operation.
 * 
 * Other responsibilities "traditionally" associated with the scrub-queue are:
 * - monitoring system load, and
 * - monitoring the number of scrubs performed by the OSD, as either a
 *   primary or replica.
 * 
 * The object's main functionality is implemented inn two layers:
 * - an upper layer (the 'ScrubQueue' class) is responsible for initiating a
 *   scrub on the top-most (priority-wise) eligible PG;
 * - a prioritized container of "scrub targets". A target conveys both the
 *   PG to be scrubbed, and the scrub type (deep or shallow). It contains the
 *   information required in order to prioritize the specific scrub request
 *   compared to all other requests.
 *
 * \note: the following invariants hold:
 * - there are at most two objects for each PG (one for each scrub level) in
 *   the queue.
 * - when a queue element is removed, the corresponding object held by the
 *   PgScrubber is (not necessarily immediately) marked as 'not in the queue'.
 * - 'white-out' queue elements are never reported to the queue users.
 */
class ScrubQueue : public Scrub::ScrubQueueOps {

  /**
   * the bookkeeping involved with an on-going 'scrub initiation
   * loop'.
   */
  struct ScrubStartLoop {
    ScrubStartLoop(
	utime_t now,
	int budget,
	Scrub::ScrubPreconds preconds,
	spg_t first_tried,
	scrub_level_t first_level_tried)
	: loop_id{now}
	, retries_budget{budget}
	, env_restrictions{preconds}
	, first_pg_tried{first_tried}
	, first_level_tried{first_level_tried}
    {}

    Scrub::loop_token_t loop_id;  // its ID - and its start time

    /// how many scrub queue entries would we try at most
    int retries_budget;

    /// restrictions on the next scrub imposed by OSD environment
    Scrub::ScrubPreconds env_restrictions;

    /// noting the 1'st entry tried in this loop, to avoid looping on the same
    /// sched target
    spg_t first_pg_tried;
    scrub_level_t first_level_tried;

    int attempted{0};  // how many retries were done

    ///\todo consider adding 'last update' time, to detect a stuck loop
  };

 public:
  ScrubQueue(CephContext* cct, Scrub::ScrubSchedListener& osds);
  virtual ~ScrubQueue() = default;

  friend class TestOSDScrub;
  friend class ScrubQueueTestWrapper;  ///< unit-tests structure

  using SchedEntry = Scrub::SchedEntry;
  using SchedulingQueue = std::deque<SchedEntry>;

  std::ostream& gen_prefix(std::ostream& out) const;

  // //////////////////////////////////////////////////////////////
  // the ScrubQueueOps interface (doc in scrub_queue_if.h)

  utime_t scrub_clock_now() const override;

  Scrub::sched_conf_t populate_config_params(
      const pool_opts_t& pool_conf) const override;

  void remove_entry(spg_t pgid, scrub_level_t s_or_d) final;

  void cp_and_queue_target(SchedEntry t) final;

  bool queue_entries(
      spg_t pgid,
      const SchedEntry& shallow,
      const SchedEntry& deep) final;

  void scrub_next_in_queue(Scrub::loop_token_t loop_id) final;

  void initiation_loop_done(Scrub::loop_token_t loop_id) final;

  // ///////////////////////////////////////////////////////////////
  // outside the scope of the I/F used by the ScrubJob:

  /**
   * the main entry point for the OSD. Called in OSD::tick_without_osd_lock()
   * to determine if there are PGs that are ready to be scrubbed, and to
   * initiate a scrub of one of those that are ready.
   */
  void initiate_a_scrub(
      const ceph::common::ConfigProxy& config,
      bool is_recovery_active);

  /*
   * handles a change to the configuration parameters affecting the scheduling
   * of scrubs.
   */
  void on_config_times_change();

 public:
  void dump_scrubs(ceph::Formatter* f);

  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  void set_reserving_now() { a_pg_is_reserving = true; }
  void clear_reserving_now() { a_pg_is_reserving = false; }
  bool is_reserving_now() const { return a_pg_is_reserving; }

  // resource reservation management

  Scrub::ScrubResources& resource_bookkeeper();
  const Scrub::ScrubResources& resource_bookkeeper() const;
  /// and the logger function used by that bookkeeper:
  void log_fwd(std::string_view text);

  /// counting the number of PGs stuck while scrubbing, waiting for objects
  void mark_pg_scrub_blocked(spg_t blocked_pg);
  void clear_pg_scrub_blocked(spg_t blocked_pg);

 private:
  int get_blocked_pgs_count() const;

 public:
  /**
   * Pacing the scrub operation by inserting delays (mostly between chunks)
   *
   * Special handling for regular scrubs that continued into "no scrub" times.
   * Scrubbing will continue, but the delays will be controlled by a separate
   * (read - with higher value) configuration element
   * (osd_scrub_extended_sleep).
   */
  std::chrono::milliseconds required_sleep_time(bool high_priority_scrub) const;

  /**
   *  called every heartbeat to update the "daily" load average
   *
   *  @returns a load value for the logger
   */
  [[nodiscard]] std::optional<double> update_load_average();

 private:
  CephContext* cct;
  Scrub::ScrubSchedListener& osd_service;
  Scrub::ScrubResources m_osd_resources;

#ifdef WITH_SEASTAR
  auto& conf() const
  {
    return local_conf();
  }
#else
  auto& conf() const
  {
    return cct->_conf;
  }
#endif

  mutable ceph::mutex jobs_lock = ceph::make_mutex("ScrubQueue::jobs_lock");

  // the underlying implementation of the scrub queue
  std::unique_ptr<ScrubQueueImp_IF> m_queue_impl;

  /**
   * m_initiation_loop, when set, indicates that we are traversing the scrub
   * queue looking for a PG to scrub. It also maintains the look ID (its start
   * time) and the number of retries left.
   */
  std::optional<ScrubStartLoop> m_initiation_loop;

  /**
   * protects 'm_initiation_loop'
   *
   * \attn never take 'jobs_lock' while holding this lock!
   */
  ceph::mutex m_loop_lock{ceph::make_mutex("ScrubQueue::m_loop_lock")};

  double daily_loadavg{0.0};

  std::string log_prefix;

  /**
   * The set of "environmental" restrictions that possibly affects the
   * scheduling of scrubs (e.g. preventing some non-urgent scrubs from being
   * scheduled).
   * If restrictions_on_scrubbing() determines that no scrubbing is possible,
   * std::nullopt is returned, which should result in no more attempted scrubs
   * at this tick (this 'scheduling loop').
   */
  std::optional<Scrub::ScrubPreconds> restrictions_on_scrubbing(
      const ceph::common::ConfigProxy& config,
      bool is_recovery_active,
      utime_t scrub_clock_now) const;

  /**
   * The scrubbing of PGs might be delayed if the scrubbed chunk of objects is
   * locked by some other operation. A bug might cause this to be an infinite
   * delay. If that happens, the OSDs "scrub resources" (i.e. the
   * counters that limit the number of concurrent scrub operations) might
   * be exhausted.
   * We do issue a cluster-log warning in such occasions, but that message is
   * easy to miss. The 'some pg is blocked' global flag is used to note the
   * existence of such a situation in the scrub-queue log messages.
   */
  std::atomic_int_fast16_t blocked_scrubs_cnt{0};

  std::atomic_bool a_pg_is_reserving{false};

  [[nodiscard]] bool scrub_load_below_threshold() const;
  [[nodiscard]] bool scrub_time_permit() const;

 public:  // used by the unit-tests
  /**
   * unit-tests will override this function to return a mock time
   */
  virtual utime_t time_now() const
  {
    return ceph_clock_now();
  }
};
