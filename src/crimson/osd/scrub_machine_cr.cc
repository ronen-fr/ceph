// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "scrub_machine_cr.h"

#include <chrono>
#include <typeinfo>

#include <boost/core/demangle.hpp>

#include "crimson/osd/osd.h"
// RRR #include "OpRequest.h"
//#include "crimson/osd/scrub_store.h"
#include "scrub_machine_lstnr_cr.h"

#if 0
#define dout_context g_ceph_context
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix *_dout << " scrubberFSM "
#endif

using namespace std::chrono;
using namespace std::chrono_literals;
namespace sc = boost::statechart;

namespace {
seastar::logger& logger()
{
  return crimson::get_logger(ceph_subsys_osd);
}
}  // namespace

#define DECLARE_LOCALS                                           \
  ScrubMachineListener* scrbr = context<ScrubMachine>().m_scrbr; \
  std::ignore = scrbr;                                           \
  auto pg_id = context<ScrubMachine>().m_pg_id;                  \
  std::ignore = pg_id;

namespace crimson::osd {

namespace Scrub {

// --------- trace/debug auxiliaries -------------------------------

void on_event_creation(std::string_view nm)
{
  logger().debug("{}: event: --vvvv---- {}", __func__, nm);
}

void on_event_discard(std::string_view nm)
{
  logger().debug("{}: event: --^^^^---- {}", __func__, nm);
}

void ScrubMachine::my_states() const
{
  for (auto si = state_begin(); si != state_end(); ++si) {
    const auto& siw{*si};  // prevents a warning re side-effects
    logger().debug("{}: state: {}", __func__, boost::core::demangle(typeid(siw).name()));
  }
}

void ScrubMachine::assert_not_active() const
{
  ceph_assert(state_cast<const NotActive*>());
}

bool ScrubMachine::is_reserving() const
{
  return state_cast<const ReservingReplicas*>();
}

#if 0
// for the rest of the code in this file - we know what PG we are dealing with:
#undef dout_prefix
#define dout_prefix _prefix(_dout, this->context<ScrubMachine>().m_pg)
template <class T> static ostream& _prefix(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout) << " scrubberFSM pg(" << t->pg_id << ") ";
  return t->gen_prefix(*_dout) << " scrubberFSM pg(" << t->pg_id << ") ";
}
#endif

// ////////////// the actual actions

// ----------------------- NotActive -----------------------------------------

NotActive::NotActive(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> NotActive");
}

// ----------------------- ReservingReplicas ---------------------------------

ReservingReplicas::ReservingReplicas(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> ReservingReplicas");
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->reserve_replicas();
}

sc::result ReservingReplicas::react(const ReservationFailure&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("ReservingReplicas::react(const ReservationFailure&)");

  // the Scrubber must release all resources and abort the scrubbing
  scrbr->clear_pgscrub_state();
  return transit<NotActive>();
}

/**
 * note: the event poster is handling the scrubber reset
 */
sc::result ReservingReplicas::react(const FullReset&)
{
  logger().debug("ReservingReplicas::react(const FullReset&)");
  return transit<NotActive>();
}

// ----------------------- ActiveScrubbing -----------------------------------

ActiveScrubbing::ActiveScrubbing(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> ActiveScrubbing");
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->on_init();
}

/**
 *  upon exiting the Active state
 */
ActiveScrubbing::~ActiveScrubbing()
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("{}", __func__);
  scrbr->unreserve_replicas();
}

/*
 * The only source of an InternalError event as of now is the BuildMap state,
 * when encountering a backend error.
 * We kill the scrub and reset the FSM.
 */
sc::result ActiveScrubbing::react(const InternalError&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("{}", __func__);
  scrbr->clear_pgscrub_state();
  return transit<NotActive>();
}

sc::result ActiveScrubbing::react(const FullReset&)
{
  logger().debug("zz: ActiveScrubbing::react(const FullReset&)");
  // caller takes care of clearing the scrubber & FSM states
  return transit<NotActive>();
}

// ----------------------- ActStartup -----------------------------------

/*
 * Crimson-specific: waiting for initialization.
 */
ActStartup::ActStartup(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/ActStartup");
}

// ----------------------- RangeBlocked -----------------------------------

/*
 * Blocked. Will be released by kick_object_context_blocked() (or upon
 * an abort)
 */
RangeBlocked::RangeBlocked(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/RangeBlocked");
}

// ----------------------- PendingTimer -----------------------------------

/**
 *  Sleeping till timer reactivation - or just requeuing
 */
PendingTimer::PendingTimer(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/PendingTimer");
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  std::ignore = scrbr->add_delayed_scheduling();
}

// ----------------------- NewChunk -----------------------------------

/**
 *  Preconditions:
 *  - preemption data was set
 *  - epoch start was updated
 */
NewChunk::NewChunk(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/NewChunk");
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  scrbr->get_preemptor().adjust_parameters();

  // choose range to work on.

  //  choose range to work on.
  //  select_range_n_notify() will either signal SelectedChunkFree or
  //  ChunkIsBusy

  scrbr->select_range_n_notify();
}

sc::result NewChunk::react(const SelectedChunkFree&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("zz: NewChunk::react(const SelectedChunkFree&)");

  scrbr->set_subset_last_update(scrbr->search_log_for_updates());
  return transit<WaitPushes>();
}

// ----------------------- WaitPushes -----------------------------------

WaitPushes::WaitPushes(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/WaitPushes");
  post_event(boost::intrusive_ptr<ActivePushesUpd>(new ActivePushesUpd{}));
}

/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result WaitPushes::react(const ActivePushesUpd&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug(
    "{}: WaitPushes::react(const ActivePushesUpd&) pending_active_pushes: {}", __func__,
    scrbr->pending_active_pushes());

  if (!scrbr->pending_active_pushes()) {
    // done waiting
    return transit<WaitLastUpdate>();
  }

  return discard_event();
}

// ----------------------- WaitLastUpdate -----------------------------------

WaitLastUpdate::WaitLastUpdate(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/WaitLastUpdate");
  post_event(boost::intrusive_ptr<UpdatesApplied>(new UpdatesApplied{}));
}

void WaitLastUpdate::on_new_updates(const UpdatesApplied&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("WaitLastUpdate::on_new_updates(const UpdatesApplied&)");

  if (scrbr->has_pg_marked_new_updates()) {
    post_event(boost::intrusive_ptr<InternalAllUpdates>(new InternalAllUpdates{}));
  } else {
    // will be requeued by op_applied
    logger().debug("{}: wait for EC read/modify/writes to queue", __func__);
  }
}

/*
 *  request maps from the replicas in the acting set
 */
sc::result WaitLastUpdate::react(const InternalAllUpdates&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("WaitLastUpdate::react(const InternalAllUpdates&)");

  scrbr->get_replicas_maps(scrbr->get_preemptor().is_preemptable());

  // the transit will be initiated by get_replicas_maps():
  // return transit<BuildMap>();
  return discard_event();
}

// ----------------------- BuildMap -----------------------------------

BuildMap::BuildMap(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/BuildMap");
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases

  // no need to check for an epoch change, as all possible flows that brought us
  // here have a check_interval() verification of their final event.

  if (scrbr->get_preemptor().was_preempted()) {

    // we were preempted, either directly or by a replica
    logger().debug("{}:  preempted!!!", __func__);
    scrbr->mark_local_map_ready();
    post_event(boost::intrusive_ptr<IntBmPreempted>(new IntBmPreempted{}));

  } else {

    scrbr->initiate_primary_map_build();

    //    auto ret = scrbr->build_primary_map_chunk();
    //
    //    if (ret == -EINPROGRESS) {
    //      // must wait for the backend to finish. No specific event provided.
    //      // build_primary_map_chunk() has already requeued us.
    //      logger().debug("{}: waiting for the backend...", __func__);
    //
    //    } else if (ret < 0) {
    //
    //      logger().debug("{}: BuildMap::BuildMap() Error! Aborting. Ret: {}",
    //                     __func__, ret);
    //      // scrbr->mark_local_map_ready();
    //      post_event(boost::intrusive_ptr<InternalError>(new InternalError{}));
    //
    //    } else {
    //
    //      // the local map was created
    //      post_event(boost::intrusive_ptr<IntLocalMapDone>(new IntLocalMapDone{}));
    //    }
  }
}

sc::result BuildMap::react(const IntLocalMapDone&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("BuildMap::react(const IntLocalMapDone&)");

  scrbr->mark_local_map_ready();
  return transit<WaitReplicas>();
}

// ----------------------- DrainReplMaps -----------------------------------

DrainReplMaps::DrainReplMaps(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/DrainReplMaps");
  // we may have received all maps already. Send the event that will make us
  // check.
  post_event(boost::intrusive_ptr<GotReplicas>(new GotReplicas{}));
}

sc::result DrainReplMaps::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("DrainReplMaps::react(const GotReplicas&)");

  if (scrbr->are_all_maps_available()) {
    // NewChunk will handle the preemption that brought us to this state
    return transit<PendingTimer>();
  }

  logger().debug(
    "DrainReplMaps::react(const GotReplicas&): still draining "
    "incoming maps: {}",
    scrbr->dump_awaited_maps());
  return discard_event();
}

// ----------------------- WaitReplicas -----------------------------------

WaitReplicas::WaitReplicas(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/WaitReplicas");
  post_event(boost::intrusive_ptr<GotReplicas>(new GotReplicas{}));
}

sc::result WaitReplicas::react(const GotReplicas&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("WaitReplicas::react(const GotReplicas&)");

  if (scrbr->are_all_maps_available()) {
    logger().debug("{}: WaitReplicas::react(const GotReplicas&) got all", __func__);

    // were we preempted?
    if (scrbr->get_preemptor().disable_and_test()) {  // a test&set

      logger().debug("{}: WaitReplicas::react(const GotReplicas&) PREEMPTED!", __func__);
      return transit<PendingTimer>();

    } else {

      scrbr->maps_compare_n_cleanup();
      return transit<WaitDigestUpdate>();
    }
  } else {
    return discard_event();
  }
}

// ----------------------- WaitDigestUpdate -----------------------------------

WaitDigestUpdate::WaitDigestUpdate(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> Act/WaitDigestUpdate");
  // perform an initial check: maybe we already
  // have all the updates we need:
  // (note that DigestUpdate is usually an external event)
  post_event(boost::intrusive_ptr<DigestUpdate>(new DigestUpdate{}));
}

sc::result WaitDigestUpdate::react(const DigestUpdate&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("WaitDigestUpdate::react(const DigestUpdate&)");

  switch (scrbr->on_digest_updates()) {

    case Scrub::FsmNext::goto_notactive:
      // scrubbing is done
      return transit<NotActive>();

    case Scrub::FsmNext::next_chunk:
      // go get the next chunk
      return transit<PendingTimer>();

    case Scrub::FsmNext::do_discard:
      // still waiting for more updates
      return discard_event();
  }
  __builtin_unreachable();  // Prevent a gcc warning.
			    // Adding a phony 'default:' above is wrong: (a)
			    // prevents a warning if FsmNext is extended, and (b)
			    // elicits a correct warning from Clang
}

ScrubMachine::ScrubMachine(crimson::osd::PG* pg, ScrubMachineListener* pg_scrub)
    : m_pg{pg}, m_pg_id{pg->get_pgid()}, m_scrbr{pg_scrub}
{
  logger().debug("ScrubMachine created {}", m_pg_id);
}

ScrubMachine::~ScrubMachine() = default;

// -------- for replicas -----------------------------------------------------

// ----------------------- ReplicaWaitUpdates --------------------------------

ReplicaWaitUpdates::ReplicaWaitUpdates(my_context ctx) : my_base(ctx)
{
  logger().debug("scrubberFSM -- state -->> ReplicaWaitUpdates");
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  scrbr->on_replica_init();
}

/*
 * Triggered externally, by the entity that had an update re pushes
 */
sc::result ReplicaWaitUpdates::react(const ReplicaPushesUpd&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("ReplicaWaitUpdates::react(const ReplicaPushesUpd&): {}",
		 scrbr->pending_active_pushes());

  if (scrbr->pending_active_pushes() == 0) {

    // done waiting
    return transit<ActiveReplica>();
  }

  return discard_event();
}

/**
 * the event poster is handling the scrubber reset
 */
sc::result ReplicaWaitUpdates::react(const FullReset&)
{
  logger().debug("ReplicaWaitUpdates::react(const FullReset&)");
  return transit<NotActive>();
}

// ----------------------- ActiveReplica -----------------------------------

ActiveReplica::ActiveReplica(my_context ctx) : my_base(ctx)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("scrubberFSM -- state -->> ActiveReplica");
  scrbr->on_replica_init();  // as we might have skipped ReplicaWaitUpdates
  post_event(boost::intrusive_ptr<SchedReplica>(new SchedReplica{}));
}

sc::result ActiveReplica::react(const SchedReplica&)
{
  DECLARE_LOCALS;  // 'scrbr' & 'pg_id' aliases
  logger().debug("{}: ActiveReplica::react(const SchedReplica&). is_preemptable? {}",
		 __func__, scrbr->get_preemptor().is_preemptable());

  if (scrbr->get_preemptor().was_preempted()) {
    logger().debug("{}: replica scrub job preempted", __func__);

    scrbr->send_replica_map(PreemptionNoted::preempted);
    scrbr->replica_handling_done();
    return transit<NotActive>();
  }

  scrbr->build_replica_map_chunk();
  return discard_event();

#if 0
  // start or check progress of build_replica_map_chunk()

  auto ret = scrbr->build_replica_map_chunk();
  logger().debug("zz: ActiveReplica::react(const SchedReplica&) Ret: {}", ret);

  if (ret == -EINPROGRESS) {
    // must wait for the backend to finish. No external event source.
    // build_replica_map_chunk() has already requeued a SchedReplica
    // event.

    logger().debug("zz: waiting for the backend...");
    return discard_event();
  }

  if (ret < 0) {
    //  the existing code ignores this option, treating an error
    //  report as a success.
    logger().debug("zz: Error! Aborting. ActiveReplica::react(SchedReplica) Ret: {}", ret
	   );
    scrbr->replica_handling_done();
    return transit<NotActive>();
  }

  // the local map was created. Send it to the primary.
  scrbr->send_replica_map(PreemptionNoted::no_preemption);
  scrbr->replica_handling_done();
  return transit<NotActive>();
#endif
}

/**
 * the event poster is handling the scrubber reset
 */
sc::result ActiveReplica::react(const FullReset&)
{
  logger().debug("zz: ActiveReplica::react(const FullReset&)");
  return transit<NotActive>();
}

}  // namespace Scrub

}  // namespace crimson::osd
