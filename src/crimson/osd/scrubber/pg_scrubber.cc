// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=2 sw=2 smarttab

#include "./pg_scrubber.h"  // the '.' notation used to affect clang-format order

#include <cmath>
#include <iostream>
#include <vector>

#include "debug.h"

// #include "common/errno.h"
// #include "messages/MOSDOp.h"
// #include "messages/MOSDRepScrub.h"
// #include "messages/MOSDRepScrubMap.h"
// #include "messages/MOSDScrub.h"
// #include "messages/MOSDScrubReserve.h"
// 
// #include "osd/OSD.h"
// #include "osd/osd_types_fmt.h"
// #include "ScrubStore.h"
// #include "scrub_machine.h"

#include "crimson/common/log.h"

#include "crimson/osd/osd_operations/scrub_event.h"
#include "crimson/osd/scrubber/scrub_machine_cr.h"


// using std::list;
// using std::map;
// using std::pair;
// using std::set;
// using std::stringstream;
// using std::vector;
using std::ostream;

using namespace Scrub;
using namespace std::chrono;
using namespace std::chrono_literals;
using namespace std::literals;


using crimson::common::local_conf;

namespace {
  seastar::logger& logger() {
    return crimson::get_logger(ceph_subsys_osd);
  }
}



ostream& operator<<(ostream& out, const scrub_flags_t& sf)
{
  if (sf.auto_repair)
    out << " AUTO_REPAIR";
  if (sf.check_repair)
    out << " CHECK_REPAIR";
  if (sf.deep_scrub_on_error)
    out << " DEEP_SCRUB_ON_ERROR";
  if (sf.required)
    out << " REQ_SCRUB";

  return out;
}

ostream& operator<<(ostream& out, const requested_scrub_t& sf)
{
  if (sf.must_repair)
    out << " must_repair";
  if (sf.auto_repair)
    out << " auto_repair";
  if (sf.check_repair)
    out << " check_repair";
  if (sf.deep_scrub_on_error)
    out << " deep_scrub_on_error";
  if (sf.must_deep_scrub)
    out << " must_deep_scrub";
  if (sf.must_scrub)
    out << " must_scrub";
  if (sf.time_for_deep)
    out << " time_for_deep";
  if (sf.need_auto)
    out << " need_auto";
  if (sf.req_scrub)
    out << " req_scrub";

  return out;
}

PgScrubber::~PgScrubber()
{
  if (m_scrub_job) {
    // make sure the OSD won't try to scrub this one just now
    rm_from_osd_scrubbing();
    m_scrub_job.reset();
  }
}

PgScrubber::PgScrubber(PG* pg)
    : m_pg{pg}
    , m_pg_id{pg->get_pgid()}
    , m_osds{m_pg->shard_services}
    , m_pg_whoami{pg->pg_whoami}
    , preemption_data{pg}
{

  logger().debug("{}: creating PgScrubber for {} / {}", __func__, pg->get_pgid(),
		 m_pg_whoami);
  // RRR not yet m_fsm = std::make_unique<Scrub::ScrubMachine>(m_pg, this);
  // RRR not yet m_fsm->initiate();
  m_scrub_job = ceph::make_ref<ScrubQueue::ScrubJob>(m_pg->get_cct(), m_pg_id, m_pg_whoami.shard);
}

ostream& PgScrubber::show(ostream& out) const
{
  return out << " [ " << m_pg_id << ": " << m_flags << " ] ";
}



// -------------------------------------------------------------------------------------------
// the I/F used by the state-machine (i.e. the implementation of ScrubMachineListener)

// -----------------

bool PgScrubber::is_reserving() const
{
  return false; // RRR m_fsm->is_reserving();
}

bool PgScrubber::is_primary() const
{
  return m_pg->is_primary();
  //return m_pg->recovery_state.is_primary(); 
}


// ///////////////////////////////////////////////////////////////////// //
// scrub-op registration handling

void PgScrubber::unregister_from_osd()
{
  if (m_scrub_job) {
    logger().debug("{}: prev. state: {}", __func__, registration_state());
    m_osds.get_scrub_services().remove_from_osd_queue(m_scrub_job);
  }
}

bool PgScrubber::is_scrub_registered() const
{
  return m_scrub_job && m_scrub_job->in_queues;
}

std::string_view PgScrubber::registration_state() const
{
  if (m_scrub_job) {
    return m_scrub_job->registration_state();
  }
  return "(no sched job)"sv;
}

void PgScrubber::rm_from_osd_scrubbing()
{
  // make sure the OSD won't try to scrub this one just now
  unregister_from_osd();
}

void PgScrubber::on_primary_change(const requested_scrub_t& request_flags)
{
  logger().info("{}: {} flags:{}", __func__, (is_primary() ? " Primary " : " Replica "), request_flags);

  if (!m_scrub_job) {
    return;
  }

  logger().debug("{}: scrub-job state: {}", __func__, m_scrub_job->state_desc());

  if (is_primary()) {
    auto suggested =  determine_scrub_time(request_flags);
    m_osds.get_scrub_services().register_with_osd(m_scrub_job, suggested);
  } else {
    m_osds.get_scrub_services().remove_from_osd_queue(m_scrub_job);
  }

  logger().debug("{}: done (registration state: {})", __func__, registration_state());
}


void PgScrubber::on_maybe_registration_change(const requested_scrub_t& request_flags)
{
  logger().info("{}: {} flags:{}", __func__, (is_primary() ? " Primary " : " Replica "), request_flags);

  on_primary_change(request_flags);
  logger().debug("{}: done (registration state: {})", __func__, registration_state());
}

void PgScrubber::update_scrub_job(const requested_scrub_t& request_flags)
{
  logger().info("{}: flags:{}", __func__, request_flags);

  {
    // verify that the 'in_q' status matches our "Primariority"
    if (m_scrub_job && is_primary() &&  !m_scrub_job->in_queues) {
      //dout(1) << __func__ << " !!! primary but not scheduled! " << dendl;
      m_pg->get_clog_error() << m_pg->get_pgid() << " primary but not scheduled"
                             << " flags:" << request_flags;
    }
  }

  if (is_primary() && m_scrub_job) {
    auto suggested = determine_scrub_time(request_flags);
    m_osds.get_scrub_services().update_job(m_scrub_job, suggested);
  }

  logger().debug("{}: done (registration state: {})", __func__, registration_state());
}

ScrubQueue::sched_params_t
PgScrubber::determine_scrub_time(const requested_scrub_t& request_flags) const
{
  ScrubQueue::sched_params_t res;

  if (!is_primary()) {
    return res; // with ok_to_scrub set to 'false'
  }

  if (request_flags.must_scrub || request_flags.need_auto) {

    // Set the smallest time that isn't utime_t()
    res.proposed_time = PgScrubber::scrub_must_stamp();
    res.is_must = ScrubQueue::must_scrub_t::mandatory;
    // we do not need the interval data in this case

  } else if (m_pg->get_pg_info(ScrubberPasskey{}).stats.stats_invalid &&
           local_conf()->osd_scrub_invalid_stats) {
    res.proposed_time = ceph_clock_now();
    res.is_must = ScrubQueue::must_scrub_t::mandatory;

  } else {
    res.proposed_time = m_pg->get_pg_info(ScrubberPasskey{}).history.last_scrub_stamp;
    res.min_interval =
      m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MIN_INTERVAL, 0.0);
    res.max_interval =
      m_pg->get_pool().info.opts.value_or(pool_opts_t::SCRUB_MAX_INTERVAL, 0.0);
  }

  logger().debug("{}: suggested: {} hist: {} v:{} must:{} pool min: {} max: {}",
    __func__, res.proposed_time, m_pg->get_pg_info(ScrubberPasskey{}).history.last_scrub_stamp,
    m_pg->get_pg_info(ScrubberPasskey{}).stats.stats_invalid,
    res.is_must==ScrubQueue::must_scrub_t::mandatory ? "y" : "n",
    res.min_interval, res.max_interval);

  return res;
}



// ///////////////////// preemption_data_t //////////////////////////////////

PgScrubber::preemption_data_t::preemption_data_t(PG* pg) : m_pg{pg}
{
  m_left = static_cast<int>(
   local_conf().get_val<uint64_t>("osd_scrub_max_preemptions"));
}

void PgScrubber::preemption_data_t::reset()
{
  //std::lock_guard<std::mutex> lk{m_preemption_lock};

  m_preemptable = false;
  m_preempted = false;
  m_left =
    static_cast<int>(local_conf().get_val<uint64_t>("osd_scrub_max_preemptions"));
  m_size_divisor = 1;
}

#ifdef NOT_YET

// ///////////////////// ReplicaReservations //////////////////////////////////
namespace Scrub {

void ReplicaReservations::release_replica(pg_shard_t peer, epoch_t epoch)
{
//   auto m = new MOSDScrubReserve(spg_t(m_pg_info.pgid.pgid, peer.shard), epoch,
// 				MOSDScrubReserve::RELEASE, m_pg->pg_whoami);
//   m_osds->send_message_osd_cluster(peer.osd, m, epoch);
}

ReplicaReservations::ReplicaReservations(PG* pg, pg_shard_t whoami, ScrubQueue::ScrubJobRef scrubjob)
    : m_pg{pg}
    , m_acting_set{pg->get_actingset(ScrubberPasskey{})}
    , m_osds{m_pg->get_shard_services()}
    , m_pending{static_cast<int>(m_acting_set.size()) - 1}
    , m_pg_info{m_pg->get_pg_info(ScrubberPasskey())}
    , m_scrub_job{scrubjob}
{
  epoch_t epoch = m_pg->get_osdmap_epoch();

  {
    std::stringstream prefix;
    prefix << "osd." << m_osds.whoami << " ep: " << epoch
	   << " scrubber::ReplicaReservations pg[" << m_pg->get_pgid() << "]: ";
    m_log_msg_prefix = prefix.str();
  }

  // handle the special case of no replicas
  if (m_pending <= 0) {
    // just signal the scrub state-machine to continue
    send_all_done();

  } else {

    for (auto p : m_acting_set) {
      if (p == whoami)
	continue;
//       auto m = new MOSDScrubReserve(spg_t(m_pg_info.pgid.pgid, p.shard), epoch,
// 				    MOSDScrubReserve::REQUEST, m_pg->pg_whoami);
//       m_osds->send_message_osd_cluster(p.osd, m, epoch);
      m_waited_for_peers.push_back(p);
      logger().info("{}reserve {}", m_log_msg_prefix, p.osd);
    }
  }
}

void ReplicaReservations::send_all_done()
{
  //m_osds->queue_for_scrub_granted(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::send_reject()
{
  m_scrub_job->resources_failure = true;
  //m_osds->queue_for_scrub_denied(m_pg, scrub_prio_t::low_priority);
}

void ReplicaReservations::discard_all()
{
  logger().info("{}/{}: reserved peers: {}", m_log_msg_prefix, __func__, m_reserved_peers);

  m_had_rejections = true;  // preventing late-coming responses from triggering events
  m_reserved_peers.clear();
  m_waited_for_peers.clear();
}

ReplicaReservations::~ReplicaReservations()
{
  m_had_rejections = true;  // preventing late-coming responses from triggering events

  // send un-reserve messages to all reserved replicas. We do not wait for answer (there
  // wouldn't be one). Other incoming messages will be discarded on the way, by our
  // owner.
  epoch_t epoch = m_pg->get_osdmap_epoch();

  for (auto& p : m_reserved_peers) {
    release_replica(p, epoch);
  }
  m_reserved_peers.clear();

  // note: the release will follow on the heels of the request. When tried otherwise,
  // grants that followed a reject arrived after the whole scrub machine-state was
  // reset, causing leaked reservations.
  for (auto& p : m_waited_for_peers) {
    release_replica(p, epoch);
  }
  m_waited_for_peers.clear();
}


/**
 *  @ATTN we would not reach here if the ReplicaReservation object managed by the
 * scrubber was reset.
 */
void ReplicaReservations::handle_reserve_grant(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << ": granted by " << from << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  // are we forced to reject the reservation?
  if (m_had_rejections) {

    dout(10) << __func__ << ": rejecting late-coming reservation from "
	     << from << dendl;
    release_replica(from, m_pg->get_osdmap_epoch());

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    dout(10) << __func__ << ": already had osd." << from << " reserved" << dendl;

  } else {

    dout(10) << __func__ << ": osd." << from << " scrub reserve = success"
	     << dendl;
    m_reserved_peers.push_back(from);
    if (--m_pending == 0) {
      send_all_done();
    }
  }
}

void ReplicaReservations::handle_reserve_reject(OpRequestRef op, pg_shard_t from)
{
  dout(10) << __func__ << ": rejected by " << from << dendl;
  dout(15) << __func__ << ": " << *op->get_req() << dendl;
  op->mark_started();

  {
    // reduce the amount of extra release messages. Not a must, but the log is cleaner
    auto w = find(m_waited_for_peers.begin(), m_waited_for_peers.end(), from);
    if (w != m_waited_for_peers.end())
      m_waited_for_peers.erase(w);
  }

  if (m_had_rejections) {

    // our failure was already handled when the first rejection arrived
    dout(15) << __func__ << ": ignoring late-coming rejection from "
	     << from << dendl;

  } else if (std::find(m_reserved_peers.begin(), m_reserved_peers.end(), from) !=
	     m_reserved_peers.end()) {

    dout(10) << __func__ << ": already had osd." << from << " reserved" << dendl;

  } else {

    dout(10) << __func__ << ": osd." << from << " scrub reserve = fail" << dendl;
    m_had_rejections = true;  // preventing any additional notifications
    send_reject();
  }
}


std::ostream& ReplicaReservations::gen_prefix(std::ostream& out) const
{
  return out << m_log_msg_prefix;
}

// ///////////////////// LocalReservation //////////////////////////////////

// note: no dout()s in LocalReservation functions. Client logs interactions.
LocalReservation::LocalReservation(crimson::osd::ShardServices* osds)
    : m_osds{osds}
{
  if (m_osds->get_scrub_services().inc_scrubs_local()) {
    // the failure is signalled by not having m_holding_local_reservation set
    m_holding_local_reservation = true;
  }
}

LocalReservation::~LocalReservation()
{
  if (m_holding_local_reservation) {
    m_holding_local_reservation = false;
    m_osds->get_scrub_services().dec_scrubs_local();
  }
}

// ///////////////////// ReservedByRemotePrimary ///////////////////////////////

ReservedByRemotePrimary::ReservedByRemotePrimary(const PgScrubber* scrubber,
						 PG* pg,
						 crimson::osd::ShardServices* osds,
						 epoch_t epoch)
    : m_scrubber{scrubber}
    , m_pg{pg}
    , m_osds{osds}
    , m_reserved_at{epoch}
{
  if (!m_osds->get_scrub_services().inc_scrubs_remote()) {
    logger().info("{}: failed to reserve at Primary request", __func__);
    // the failure is signalled by not having m_reserved_by_remote_primary set
    return;
  }

  logger().debug("{}: reserved scrub resources at Primary request", __func__);
  m_reserved_by_remote_primary = true;
}

bool ReservedByRemotePrimary::is_stale() const
{
  return m_reserved_at < m_pg->get_same_interval_since();
}

ReservedByRemotePrimary::~ReservedByRemotePrimary()
{
  if (m_reserved_by_remote_primary) {
    m_reserved_by_remote_primary = false;
    m_osds->get_scrub_services().dec_scrubs_remote();
  }
}

std::ostream& ReservedByRemotePrimary::gen_prefix(std::ostream& out) const
{
  return out; // RRR m_scrubber->gen_prefix(out);
}

#endif

#ifdef NOT_YET

// ///////////////////// MapsCollectionStatus ////////////////////////////////

auto MapsCollectionStatus::mark_arriving_map(pg_shard_t from)
  -> std::tuple<bool, std::string_view>
{
  auto fe = std::find(m_maps_awaited_for.begin(), m_maps_awaited_for.end(), from);
  if (fe != m_maps_awaited_for.end()) {
    // we are indeed waiting for a map from this replica
    m_maps_awaited_for.erase(fe);
    return std::tuple{true, ""sv};
  } else {
    return std::tuple{false, " unsolicited scrub-map"sv};
  }
}

void MapsCollectionStatus::reset()
{
  *this = MapsCollectionStatus{};
}

std::string MapsCollectionStatus::dump() const
{
  std::string all;
  for (const auto& rp : m_maps_awaited_for) {
    all.append(rp.get_osd() + " "s);
  }
  return all;
}

ostream& operator<<(ostream& out, const MapsCollectionStatus& sf)
{
  out << " [ ";
  for (const auto& rp : sf.m_maps_awaited_for) {
    out << rp.get_osd() << " ";
  }
  if (!sf.m_local_map_ready) {
    out << " local ";
  }
  return out << " ] ";
}

#endif

// ///////////////////// blocked_range_t ///////////////////////////////

#ifdef NOT_YET

blocked_range_t::blocked_range_t(crimson::osd::ShardServices* osds, ceph::timespan waittime, spg_t pg_id)
    : m_osds{osds}
{
  auto now_is = std::chrono::system_clock::now();
  m_callbk = new LambdaContext([now_is, pg_id, osds]([[maybe_unused]] int r) {
    std::time_t now_c = std::chrono::system_clock::to_time_t(now_is);
    char buf[50];
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", std::localtime(&now_c));
    lgeneric_subdout(g_ceph_context, osd, 10)
      << "PgScrubber: " << pg_id << " blocked on an object for too long (since " << buf
      << ")" << dendl;
    osds->clog->warn() << "osd." << osds->whoami << " PgScrubber: " << pg_id << " blocked on an object for too long (since " << buf << ")";
    return;
  });

  //std::lock_guard l(m_osds->sleep_lock);
  m_osds->sleep_timer.add_event_after(waittime, m_callbk);
}

blocked_range_t::~blocked_range_t()
{
  //std::lock_guard l(m_osds->sleep_lock);
  m_osds->sleep_timer.cancel_event(m_callbk);
}

#endif


// fakes


void PgScrubber::initiate_regular_scrub(epoch_t epoch_queued) {}

void PgScrubber::initiate_scrub_after_repair(epoch_t epoch_queued) {}

void PgScrubber::send_scrub_resched(epoch_t epoch_queued) {}

void PgScrubber::active_pushes_notification(epoch_t epoch_queued) {}

void PgScrubber::update_applied_notification(epoch_t epoch_queued) {}

void PgScrubber::send_scrub_unblock(epoch_t epoch_queued) {}

void PgScrubber::digest_update_notification(epoch_t epoch_queued) {}

void PgScrubber::send_replica_maps_ready(epoch_t epoch_queued) {}

void PgScrubber::send_start_replica(epoch_t epoch_queued,
                                    Scrub::act_token_t token)
{}

void PgScrubber::send_sched_replica(epoch_t epoch_queued,
                                    Scrub::act_token_t token)
{}

void PgScrubber::send_replica_pushes_upd(epoch_t epoch_queued) {}

void PgScrubber::on_applied_when_primary(const eversion_t& applied_version) {}

void PgScrubber::send_full_reset(epoch_t epoch_queued) {}

void PgScrubber::send_chunk_free(epoch_t epoch_queued) {}

void PgScrubber::send_chunk_busy(epoch_t epoch_queued) {}

void PgScrubber::send_local_map_done(epoch_t epoch_queued) {}

void PgScrubber::send_maps_compared(epoch_t epoch_queued) {}

void PgScrubber::send_get_next_chunk(epoch_t epoch_queued) {}

void PgScrubber::send_scrub_is_finished(epoch_t epoch_queued) {}

bool PgScrubber::write_blocked_by_scrub(const hobject_t& soid)
{
  return false;
}

bool PgScrubber::range_intersects_scrub(const hobject_t& start,
                                        const hobject_t& end)
{
  return false;
}

void PgScrubber::discard_replica_reservations() {}

void PgScrubber::clear_scrub_reservations() {}

void PgScrubber::unreserve_replicas() {}

void PgScrubber::scrub_requested(scrub_level_t scrub_level,
                                 scrub_type_t scrub_type,
                                 requested_scrub_t& req_flags)
{}

bool PgScrubber::reserve_local()
{
  return true;
}

void PgScrubber::handle_query_state(ceph::Formatter* f) {}

void PgScrubber::dump(ceph::Formatter* f) const {}

unsigned int PgScrubber::scrub_requeue_priority(
  Scrub::scrub_prio_t with_priority, unsigned int suggested_priority) const
{
  return 100;
}

unsigned int PgScrubber::scrub_requeue_priority(
  Scrub::scrub_prio_t with_priority) const
{
  return 100;
}

void PgScrubber::scrub_clear_state() {}

void PgScrubber::stats_of_handled_objects(const object_stat_sum_t& delta_stats,
                                          const hobject_t& soid)
{}
void PgScrubber::set_op_parameters(requested_scrub_t& request) {}

void PgScrubber::cleanup_store(ObjectStore::Transaction* t) {}

bool PgScrubber::get_store_errors(const scrub_ls_arg_t& arg,
                                  scrub_ls_result_t& res_inout) const
{
  return false;
}


int PgScrubber::asok_debug(std::string_view cmd,
                           std::string param,
                           Formatter* f,
                           std::stringstream& ss)
{
  return 0;
}

void PgScrubber::select_range_n_notify() {}

Scrub::BlockedRangeWarning PgScrubber::acquire_blocked_alarm()
{
  return Scrub::BlockedRangeWarning();
}

eversion_t PgScrubber::search_log_for_updates() const
{
  return eversion_t();
}

eversion_t PgScrubber::get_last_update_applied() const
{
  return eversion_t();
}

int PgScrubber::pending_active_pushes() const
{
  return 0;
}

void PgScrubber::on_init() {}

void PgScrubber::on_replica_init() {}

void PgScrubber::replica_handling_done() {}

void PgScrubber::clear_pgscrub_state() {}

void PgScrubber::add_delayed_scheduling() {}

void PgScrubber::get_replicas_maps(bool replica_can_preempt) {}

void PgScrubber::on_digest_updates() {}

ScrubMachineListener::MsgAndEpoch PgScrubber::prep_replica_map_msg(
  Scrub::PreemptionNoted was_preempted)
{
  return ScrubMachineListener::MsgAndEpoch();
}

void PgScrubber::send_replica_map(
  const ScrubMachineListener::MsgAndEpoch& preprepared)
{}

void PgScrubber::send_preempted_replica() {}

void PgScrubber::send_remotes_reserved(epoch_t epoch_queued) {}

void PgScrubber::send_reservation_failure(epoch_t epoch_queued) {}

[[nodiscard]] bool PgScrubber::has_pg_marked_new_updates() const
{
  return false;
}

void PgScrubber::set_subset_last_update(eversion_t e) {}

void PgScrubber::maps_compare_n_cleanup() {}

Scrub::preemption_t& PgScrubber::get_preemptor()
{
  return preemption_data;
}

seastar::future<> PgScrubber::build_primary_map_chunk()
{
  return seastar::now();
}

void PgScrubber::initiate_primary_map_build() {}

seastar::future<> PgScrubber::build_replica_map_chunk()
{
  return seastar::now();
}

void PgScrubber::reserve_replicas() {}

void PgScrubber::set_reserving_now() {}
void PgScrubber::clear_reserving_now() {}

[[nodiscard]] bool PgScrubber::was_epoch_changed() const
{
  return false;
}

void PgScrubber::set_queued_or_active() {}
void PgScrubber::clear_queued_or_active() {}

void PgScrubber::mark_local_map_ready() {}

[[nodiscard]] bool PgScrubber::are_all_maps_available() const
{
  return true;
}

std::string PgScrubber::dump_awaited_maps() const
{
  return "";
}

void PgScrubber::set_scrub_begin_time() {}

void PgScrubber::set_scrub_duration() {}

// [[nodiscard]] bool PgScrubber::is_scrub_registered() const;

//  std::string_view PgScrubber::registration_state() const;

void PgScrubber::reset_internal_state() {}

void PgScrubber::advance_token() {}

// bool PgScrubber::is_token_current(Scrub::act_token_t received_token) const {
// return true; }

void PgScrubber::_scan_snaps(ScrubMap& smap) {}

ScrubMap PgScrubber::clean_meta_map()
{
  return ScrubMap();
}

void PgScrubber::reset_epoch(epoch_t epoch_queued) {}

void PgScrubber::run_callbacks() {}

bool PgScrubber::is_message_relevant(epoch_t epoch_to_verify)
{
  return true;
}

[[nodiscard]] bool PgScrubber::should_abort() const
{
  return false;
}


[[nodiscard]] bool PgScrubber::verify_against_abort(epoch_t epoch_to_verify)
{
  return true;
}

[[nodiscard]] bool PgScrubber::check_interval(epoch_t epoch_to_verify)
{
  return true;
}
