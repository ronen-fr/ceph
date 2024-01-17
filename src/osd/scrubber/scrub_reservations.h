// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

#include <cassert>
#include <chrono>
#include <optional>
#include <string_view>
#include <vector>

#include "messages/MOSDScrubReserve.h"
#include "osd/scrubber_common.h"

#include "osd_scrub_sched.h"
#include "scrub_machine_lstnr.h"

namespace Scrub {

using reservation_nonce_t = MOSDScrubReserve::reservation_nonce_t;

/**
 * Reserving/freeing scrub resources at the replicas.
 *
 * When constructed - sends reservation requests to the acting_set OSDs, one
 * by one.
 * Once a replica's OSD replies with a 'grant'ed reservation, we send a
 * reservation request to the next replica.
 * A rejection triggers a "couldn't acquire the replicas' scrub resources"
 * event. All granted reservations are released.
 *
 * Reserved replicas should be released at the end of the scrub session. The
 * one exception is if the scrub terminates upon an interval change. In that
 * scenario - the replicas discard their reservations on their own accord
 * when noticing the change in interval, and there is no need (and no
 * guaranteed way) to send them the release message.
 *
 * Timeouts:
 *
 *  Slow-Secondary Warning:
 *  Warn if a replica takes more than <conf> milliseconds to reply to a
 *  reservation request. Only one warning is issued per session.
 *
 *  Reservation Timeout:
 *  We limit the total time we wait for the replicas to respond to the
 *  reservation request. If the reservation back-and-forth does not complete
 *  within <conf> milliseconds, we give up and release all the reservations
 *  that have been acquired until that moment.
 *  (Why? because we have encountered instances where a reservation request was
 *  lost - either due to a bug or due to a network issue.)
 *
 * Keeping primary & replica in sync:
 *
 *  Reservation requests are unique in that they may time-out and cancelled
 *  by the primary. A request that was delayed for too long and was
 *  cancelled by the sender - might still elicit a late (and irrelevant)
 *  response from the replica. A sequence of such cancelled requests might
 *  cause the primary and the replica to be out of sync.
 *
 *  To avoid this, we use a 'reservation_nonce' field in the request and
 *  response messages. The primary increments the nonce for each request
 *  it sends, and the replica includes the nonce in its response.
 *  Note - 'release' messages, which are not answered by the replica,
 *  do not use that field.
 */
class ReplicaReservations {
  ScrubMachineListener& m_scrubber;
  PG* m_pg;

  /// shorthand for m_scrubber.get_spgid().pgid
  const pg_t m_pgid;

  /// for dout && when queueing messages to the FSM
  OSDService* m_osds;

  /// the acting set (not including myself), sorted by pg_shard_t
  std::vector<pg_shard_t> m_sorted_secondaries;

  /// the next replica to which we will send a reservation request
  std::vector<pg_shard_t>::const_iterator m_next_to_request;

  /// for logs, and for detecting slow peers
  ScrubTimePoint m_last_request_sent_at;

  /// a token to uniquely identify the last reservation request sent.
  /// The response received will be checked against this nonce, to detect
  /// stale responses.
  /// Starts at 1, and is incremented for each request sent. '0' is reserved
  /// for legacy messages.
  reservation_nonce_t m_last_request_sent_nonce{1};

  /// the 'slow response' timeout (in milliseconds) - as configured.
  /// Doubles as a 'do once' flag for the warning.
  std::chrono::milliseconds m_slow_response_warn_timeout;

  /// access to the performance counters container relevant to this scrub
  /// parameters
  PerfCounters& m_perf_set;

  /// used only for the 'duration of the reservation process' perf counter.
  /// discarded once the success or failure are recorded
  std::optional<ScrubTimePoint> m_process_started_at;

 public:
  ReplicaReservations(ScrubMachineListener& scrubber, PerfCounters& pc);

  ~ReplicaReservations();

  /**
   * The OK received from the replica (after verifying that it is indeed
   * the replica we are expecting a reply from) is noted, and triggers
   * one of two: either sending a reservation request to the next replica,
   * or notifying the scrubber that we have reserved them all.
   *
   * \returns true if there are no more replicas to send reservation requests
   * (i.e., the scrubber should proceed to the next phase), false otherwise.
   */
  bool handle_reserve_grant(OpRequestRef op, pg_shard_t from);

  /**
   * React to an incoming reservation rejection.
   *
   * Verify that the sender of the received rejection is the replica we
   * were expecting a reply from, and that the message isn't stale.
   * If a real rejection: log it, and mark the fact that the specific peer
   * need not be released.
   *
   * Note - the actual handling of scrub session termination and of
   * releasing the reserved replicas is done by the caller (the FSM).
   *
   * Returns true if the rejection is valid, false otherwise.
   */
  bool handle_rejection(OpRequestRef op, pg_shard_t from);

  /**
   * Notifies implementation that it is no longer responsible for releasing
   * tracked remote reservations.
   *
   * The intended usage is upon interval change.  In general, replicas are
   * responsible for releasing their own resources upon interval change without
   * coordination from the primary.
   *
   * Sends no messages.
   */
  void discard_remote_reservations();

  /// the only replica we are expecting a reply from
  std::optional<pg_shard_t> get_last_sent() const;

  /**
   * if the start time is still set, i.e. we have not yet marked
   * this as a success or a failure - log its duration as that of a failure.
   */
  void log_failure_and_duration(int failure_cause_counter);

  // note: 'public', as accessed via the 'standard' dout_prefix() macro
  std::ostream& gen_prefix(std::ostream& out, std::string fn) const;

 private:
  /// send 'release' messages to all replicas we have managed to reserve
  void release_all();

  /// The number of requests that have been sent (and not rejected) so far.
  size_t active_requests_cnt() const;

  /**
   * Send a reservation request to the next replica.
   * - if there are no more replicas to send requests to, return true
   */
  bool send_next_reservation_or_complete();

  /// (internal helper) is this is a reply to our last request?
  tl::expected<bool, std::string> is_response_relevant(
      epoch_t msg_epoch,
      reservation_nonce_t msg_nonce,
      pg_shard_t from) const;

  /// (internal helper) is this reply coming from the expected replica?
  tl::expected<bool, std::string> is_msg_source_correct(
      epoch_t msg_epoch,
      pg_shard_t from) const;

  // ---   perf counters helpers

  /**
   * log the duration of the reservation process as that of a success.
   */
  void log_success_and_duration();
};

}  // namespace Scrub
