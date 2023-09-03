// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <cassert>
#include <chrono>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "osd/PG.h"
#include "osd/scrubber_common.h"

#include "osd_scrub_sched.h"

namespace Scrub {

/**
 * Reserving/freeing scrub resources at the replicas.
 *
 * When constructed - sends reservation requests to the acting_set OSDs, one
 * by one.
 * Once a replica's OSD replies with a 'grant'ed reservation, we send a
 * reservation request to the next replica.
 * A rejection triggers a "couldn't acquire the replicas' scrub resources"
 * event. All granted reservations are released.
 * released.
 *
 * Reserved replicas should be released at the end of the scrub session. The one
 * exception is when the scrub terminates upon an interval change. In that
 * scenario the replicas discard their reservations when noticing the change
 * in interval, and there is no need (and no guaranteed way) to send them
 * the release message.
 *
 * Timeouts:
 *
 *  Slow-Secondary Warning: (not re-implemented yet)
 *
 *  Reservation Timeout:
 *  We limit the total time we wait for the replicas to respond to the
 *  reservation request. If the reservation back-and-forth does not complete
 *  within <conf> milliseconds, we give up and release all the reservations we
 *  acquired till that moment.
 *  (Why? because we have encountered instances where a reservation request was
 *  lost - either due to a bug or due to a network issue.)
 */
class ReplicaReservations {
  using clock = std::chrono::system_clock;
  using tpoint_t = std::chrono::time_point<clock>;
  using replica_subset_t = std::span<const pg_shard_t>;

  PG* m_pg;

  /// the acting set (not including myself), sorted by OSD id
  std::vector<pg_shard_t> m_sorted_secondaries;

  /// the next replica to which we will send a reservation request
  std::vector<pg_shard_t>::const_iterator m_next_to_request;

  /// used to send both internal and inter-OSD messages
  OSDService* m_osds;

  const pg_info_t& m_pg_info;
  ScrubQueue::ScrubJobRef m_scrub_job;	///< a ref to this PG's scrub job
  const ConfigProxy& m_conf;

  /// the number of secondaries
  int m_total_needeed{-1};

  /// always <= m_total_needed
  long unsigned int m_requests_sent{0};

  tpoint_t m_request_sent_at;  ///< for detecting slow peers

  std::string m_log_msg_prefix;

 public:
  ReplicaReservations(
      PG* pg,
      pg_shard_t whoami,
      ScrubQueue::ScrubJobRef scrubjob,
      const ConfigProxy& conf);

  ~ReplicaReservations();

  void handle_reserve_grant(OpRequestRef op, pg_shard_t from);

  void handle_reserve_reject(OpRequestRef op, pg_shard_t from);

  /**
   * if timing out on receiving replies from our replicas:
   * All reserved replicas are released.
   * A failure notification is sent to the scrubber.
   */
  void handle_no_reply_timeout();

  /**
   *  quietly discard all knowledge about existing reservations. No messages
   *  are sent to peers.
   *  To be used upon interval change, as we know that the running scrub is no
   *  longer relevant, and that the replicas had reset the reservations on
   *  their side.
   */
  void discard_all();

  // note: 'public', as accessed via the 'standard' dout_prefix() macro
  std::ostream& gen_prefix(std::ostream& out) const;

 private:
  /// send a reservation request to a replica's OSD
  void send_request_to_replica(pg_shard_t peer, epoch_t epoch);

  /// send a release message to that shard's OSD
  void release_replica(pg_shard_t peer, epoch_t epoch);

  /// let the scrubber know that we have reserved all the replicas
  void send_all_done();

  /// notify the scrubber that we have failed to reserve replicas' resources
  void send_reject();

  /// send 'release' messages to all replicas we have managed to reserve
  void release_all(replica_subset_t replicas);
};

}  // namespace Scrub
