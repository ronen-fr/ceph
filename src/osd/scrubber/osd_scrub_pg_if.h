// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <optional>
#include <vector>

#include "common/RefCountedObj.h"
#include "common/ceph_atomic.h"
#include "include/utime_fmt.h"
#include "osd/osd_types.h"
#include "osd/osd_types_fmt.h"
#include "osd/scrubber_common.h"

//#include "include/utime.h"

class PG;
class PGLockWrapper;

/*
 * identifying a PG to scrub:
 * - in this version: a PG
 * - in the (near) future: a PG + a scrub type (shallow/deep)
 */
using ScrubTargetId = spg_t;


namespace Scrub {

enum class must_scrub_t { not_mandatory, mandatory };

enum class qu_state_t {
  not_registered,  // not a primary, thus not considered for scrubbing by this
		   // OSD (also the temporary state when just created)
  registered,	   // in either of the two queues ('to_scrub' or 'penalized')
  unregistering	   // in the process of being unregistered. Will be finalized
		   // under lock
};


struct scrub_schedule_t {
  utime_t scheduled_at{};
  utime_t deadline{0, 0};
};


struct sched_params_t {
  utime_t proposed_time{};
  double min_interval{0.0};
  double max_interval{0.0};
  must_scrub_t is_must{must_scrub_t::not_mandatory};
};

class ScrubJob final : public RefCountedObject {
 public:
  /**
   * a time scheduled for scrub, and a deadline: The scrub could be delayed
   * if system load is too high (but not if after the deadline),or if trying
   * to scrub out of scrub hours.
   */
  scrub_schedule_t schedule;

  /// pg to be scrubbed
  const spg_t pgid;

  /// the OSD id (for the log)
  const int whoami;

  ceph::atomic<qu_state_t> state{qu_state_t::not_registered};

  /**
   * the old 'is_registered'. Set whenever the job is registered with the OSD,
   * i.e. is in either the 'to_scrub' or the 'penalized' vectors.
   */
  std::atomic_bool in_queues{false};

  /// last scrub attempt failed to secure replica resources
  bool resources_failure{false};

  /**
   *  'updated' is a temporary flag, used to create a barrier after
   *  'sched_time' and 'deadline' (or any other job entry) were modified by
   *  different task.
   *  'updated' also signals the need to move a job back from the penalized
   *  queue to the regular one.
   */
  std::atomic_bool updated{false};

  /**
     * the scrubber is waiting for locked objects to be unlocked.
     * Set after a grace period has passed.
     */
  bool blocked{false};
  utime_t blocked_since{};

  utime_t penalty_timeout{0, 0};

  CephContext* cct;

  ScrubJob(CephContext* cct, const spg_t& pg, int node_id);

  utime_t get_sched_time() const { return schedule.scheduled_at; }

  static std::string_view qu_state_text(qu_state_t st);

  /**
   * relatively low-cost(*) access to the scrub job's state, to be used in
   * logging.
   *  (*) not a low-cost access on x64 architecture
   */
  std::string_view state_desc() const
  {
    return qu_state_text(state.load(std::memory_order_relaxed));
  }

  void update_schedule(const scrub_schedule_t& adjusted);

  void dump(ceph::Formatter* f) const;

  /*
   * as the atomic 'in_queues' appears in many log prints, accessing it for
   * display-only should be made less expensive (on ARM. On x86 the _relaxed
   * produces the same code as '_cs')
   */
  std::string_view registration_state() const
  {
    return in_queues.load(std::memory_order_relaxed) ? "in-queue"
						     : "not-queued";
  }

  /**
   * access the 'state' directly, for when a distinction between 'registered'
   * and 'unregistering' is needed (both have in_queues() == true)
   */
  bool is_state_registered() const { return state == qu_state_t::registered; }

  /**
   * a text description of the "scheduling intentions" of this PG:
   * are we already scheduled for a scrub/deep scrub? when?
   */
  std::string scheduling_state(utime_t now_is, bool is_deep_expected) const;

  friend std::ostream& operator<<(std::ostream& out, const ScrubJob& pg);
};

using ScrubJobRef = ceph::ref_t<ScrubJob>;
using ScrubQContainer = std::vector<ScrubJobRef>;


/// the interface to the OSD's scrub services provided to the PGs
class OsdScrubPgIF {
 public:
  // update/access OSD-wide state
  virtual ~OsdScrubPgIF() = default;

  /**
   * No new scrub session will start while a scrub was initiated on a PG,
   * and that PG is trying to acquire replica resources.
   */
  virtual void set_reserving_now() = 0;
  virtual void clear_reserving_now() = 0;

  virtual bool inc_scrubs_local() = 0;
  virtual void dec_scrubs_local() = 0;
  virtual bool inc_scrubs_remote() = 0;
  virtual void dec_scrubs_remote() = 0;

  /// counting the number of PGs stuck while scrubbing, waiting for objects
  virtual void mark_pg_scrub_blocked(spg_t blocked_pg) = 0;
  virtual void clear_pg_scrub_blocked(spg_t blocked_pg) = 0;
  virtual int get_blocked_pgs_count() const = 0;

  // update scheduling information for a specific PG
  virtual sched_params_t determine_scrub_time(
      const requested_scrub_t& request_flags,
      const pg_info_t& pg_info,
      const pool_opts_t& pool_conf) const = 0;

  virtual void register_with_osd(
      ScrubJobRef sjob,
      const sched_params_t& suggested) = 0;

  /**
   * remove the pg from set of PGs to be scanned for scrubbing.
   * To be used if we are no longer the PG's primary, or if the PG is removed.
   */
  virtual void remove_from_osd_queue(ScrubJobRef sjob) = 0;

  /**
   * modify a scrub-job's scheduled time and deadline
   *
   * There are 3 argument combinations to consider:
   * - 'must' is asserted, and the suggested time is 'scrub_must_stamp':
   *   the registration will be with "beginning of time" target, making the
   *   scrub-job eligible to immediate scrub (given that external conditions
   *   do not prevent scrubbing)
   *
   * - 'must' is asserted, and the suggested time is 'now':
   *   This happens if our stats are unknown. The results are similar to the
   *   previous scenario.
   *
   * - not a 'must': we take the suggested time as a basis, and add to it some
   *   configuration / random delays.
   *
   *  ('must' is Scrub::sched_params_t.is_must)
   *
   *  locking: not using the jobs_lock
   */
  virtual void update_job(
      ScrubJobRef sjob,
      const sched_params_t& suggested) = 0;

  /**
   * scrub_sleep_time
   *
   * Returns std::chrono::milliseconds indicating how long to wait between
   * chunks.
   *
   * Implementation Note: Returned value will either osd_scrub_sleep or
   * osd_scrub_extended_sleep depending on must_scrub_param and time
   * of day (see configs osd_scrub_begin*)
   */
  virtual std::chrono::milliseconds scrub_sleep_time(bool must_scrub) const = 0;
};

}  // namespace Scrub

namespace fmt {
template <>
struct formatter<Scrub::qu_state_t> : formatter<std::string_view> {
  template <typename FormatContext>
  auto format(const Scrub::qu_state_t& s, FormatContext& ctx)
  {
    auto out = ctx.out();
    out = fmt::formatter<string_view>::format(
	std::string{Scrub::ScrubJob::qu_state_text(s)}, ctx);
    return out;
  }
};

template <>
struct formatter<Scrub::ScrubJob> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const Scrub::ScrubJob& sjob, FormatContext& ctx)
  {
    return fmt::format_to(
	ctx.out(),
	"pg[{}] @ {:s} (dl:{:s}) - <{}> / failure: {} / pen. t.o.: {:s} / "
	"queue "
	"state: {:.7}",
	sjob.pgid, sjob.schedule.scheduled_at, sjob.schedule.deadline,
	sjob.registration_state(), sjob.resources_failure, sjob.penalty_timeout,
	sjob.state.load(std::memory_order_relaxed));
  }
};
}  // namespace fmt
