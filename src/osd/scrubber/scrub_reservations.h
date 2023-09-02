// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <cassert>
#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>
#include <set>

#include "osd/PG.h"
#include "osd/scrubber_common.h"
#include "osd_scrub_sched.h"

namespace Scrub {
struct BuildMap;

/**
 * Reserving/freeing scrub resources at the replicas.
 *
 * When constructed - sends reservation requests to the acting_set.
 * A rejection triggers a "couldn't acquire the replicas' scrub resources"
 * event. All previous requests, whether already granted or not, are explicitly
 * released.
 *
 * Timeouts:
 *
 *  Slow-Secondary Warning:
 *  Once at least half of the replicas have accepted the reservation, we start
 *  reporting any secondary that takes too long (more than <conf> milliseconds
 *  after the previous response received) to respond to the reservation request.
 *  (Why? because we have encountered real-life situations where a specific OSD
 *  was systematically very slow (e.g. 5 seconds) to respond to the reservation
 *  requests, slowing the scrub process to a crawl).
 *
 *  Reservation Timeout:
 *  We limit the total time we wait for the replicas to respond to the
 *  reservation request. If we don't get all the responses (either Grant or
 *  Reject) within <conf> milliseconds, we give up and release all the
 *  reservations we have acquired so far.
 *  (Why? because we have encountered instances where a reservation request was
 *  lost - either due to a bug or due to a network issue.)
 *
 * A note re performance: I've measured a few container alternatives for
 * m_reserved_peers, with its specific usage pattern. Std::set is extremely
 * slow, as expected. flat_set is only slightly better. Surprisingly -
 * std::vector (with no sorting) is better than boost::small_vec. And for
 * std::vector: no need to pre-reserve.
 */
class ReplicaReservations {
  using clock = std::chrono::system_clock;
  using tpoint_t = std::chrono::time_point<clock>;

  PG* m_pg;
  std::set<pg_shard_t> m_acting_set;
  OSDService* m_osds;
  std::vector<pg_shard_t> m_waited_for_peers;
  std::vector<pg_shard_t> m_reserved_peers;
  bool m_had_rejections{false};
  int m_pending{-1};
  const pg_info_t& m_pg_info;
  ScrubQueue::ScrubJobRef m_scrub_job;	///< a ref to this PG's scrub job
  const ConfigProxy& m_conf;

  // detecting slow peers (see 'slow-secondary' above)
  std::chrono::milliseconds m_timeout;
  std::optional<tpoint_t> m_timeout_point;

  void release_replica(pg_shard_t peer, epoch_t epoch);

  void send_all_done();	 ///< all reservations are granted

  /// notify the scrubber that we have failed to reserve replicas' resources
  void send_reject();

  std::optional<tpoint_t> update_latecomers(tpoint_t now_is);

 public:
  std::string m_log_msg_prefix;

  /**
   *  quietly discard all knowledge about existing reservations. No messages
   *  are sent to peers.
   *  To be used upon interval change, as we know the the running scrub is no
   *  longer relevant, and that the replicas had reset the reservations on
   *  their side.
   */
  void discard_all();

  ReplicaReservations(PG* pg,
                      pg_shard_t whoami,
                      ScrubQueue::ScrubJobRef scrubjob,
                      const ConfigProxy& conf); 

  ~ReplicaReservations();

  void handle_reserve_grant(OpRequestRef op, pg_shard_t from);

  void handle_reserve_reject(OpRequestRef op, pg_shard_t from);

  // if timing out on receiving replies from our replicas:
  void handle_no_reply_timeout();

  std::ostream& gen_prefix(std::ostream& out) const;
};

} // namespace Scrub
