// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <fmt/core.h>
#include <fmt/format.h>
#include <string_view>
#include <iostream>

#include <boost/statechart/event.hpp>
#include "include/mempool.h"
#include "include/types.h"

namespace crimson::osd {


namespace Scrub {

namespace sc = ::boost::statechart;
namespace mpl = ::boost::mpl;

//
//  EVENTS
//

void on_event_creation(std::string_view nm);
void on_event_discard(std::string_view nm);

#define MEV(E)                          \
  struct E : sc::event<E> {             \
    inline static int actv{0};          \
    E() : boost::statechart::event<E>() \
    {                                   \
      if (!actv++)                      \
	on_event_creation(#E);          \
    }                                   \
    ~E()                                \
    {                                   \
      if (!--actv)                      \
	on_event_discard(#E);           \
    }                                   \
    void print(std::ostream* out) const \
    {                                   \
      *out << #E;                       \
    }                                   \
    std::string_view print() const      \
    {                                   \
      return #E;                        \
    }                                   \
  };

MEV(RemotesReserved)	 ///< all replicas have granted our reserve request
MEV(ReservationFailure)	 ///< a reservation request has failed

MEV(OnInitDone)	 ///< startup operations (mainly - cleaning the scrubstore) have
///< completed
MEV(StartScrub)	 ///< initiate a new scrubbing session (relevant if we are a Primary)
MEV(AfterRepairScrub)  ///< initiate a new scrubbing session. Only triggered at Recovery
///< completion.
MEV(Unblocked)	///< triggered when the PG unblocked an object that was marked for
///< scrubbing. Via the PGScrubUnblocked op
MEV(InternalSchedScrub)
MEV(SelectedChunkFree)
MEV(ChunkIsBusy)
MEV(ActivePushesUpd)  ///< Update to active_pushes. 'active_pushes' represents recovery
///< that is in-flight to the local ObjectStore
MEV(UpdatesApplied)	 // external
MEV(InternalAllUpdates)	 ///< the internal counterpart of UpdatesApplied

MEV(ReplicaRequestsSent)  ///< sent the requests to all replicas for their maps

MEV(GotReplicas)  ///< got a map from a replica

MEV(IntBmPreempted)  ///< internal - BuildMap preempted. Required, as detected within the
///< ctor
MEV(InternalError)

MEV(IntLocalMapDone)

MEV(DigestUpdate)  ///< external. called upon success of a MODIFY op. See
///< scrub_snapshot_metadata()
MEV(AllChunksDone)
MEV(MapsCompared) ///< (Crimson) maps_compare_n_cleanup() transactions are done

MEV(StartReplica)  ///< initiating replica scrub.
MEV(StartReplicaNoWait)	 ///< 'start replica' when there are no pending updates

MEV(SchedReplica)
MEV(ReplicaPushesUpd)  ///< Update to active_pushes. 'active_pushes' represents recovery
///< that is in-flight to the local ObjectStore

MEV(FullReset)	///< guarantee that the FSM is in the quiescent state (i.e. NotActive)

MEV(NextChunk)  ///< finished handling this chunk. Go get the next one
MEV(ScrubFinished) ///< all chunks handled

}  // namespace Scrub

#if 0
class PgScrubEvent {

  epoch_t epoch_sent;
  epoch_t epoch_requested;
  std::string desc;

  // not much content here, for now
 public:
  boost::intrusive_ptr<const boost::statechart::event_base> evt;
  // static constexpr OperationTypeCode type = OperationTypeCode::scrub_event;
  bool requires_pg;

  // needed?
  //MEMPOOL_CLASS_HELPERS();


  template <class T>
  PgScrubEvent(epoch_t epoch_sent, epoch_t epoch_requested, const T& sevt)
      // bool req = true)
      : epoch_sent(epoch_sent)
      , epoch_requested(epoch_requested)
      , evt{sevt.intrusive_from_this()}
      , requires_pg{true}
  {
    // std::stringstream out;
    // out << "epoch_sent: " << epoch_sent << " epoch_requested: " << epoch_requested << "
    // ";
    // sevt.print(&out);
    // desc = out.str();

    ::fmt::memory_buffer bf;
    ::fmt::format_to(bf, "epoch_sent:{} epoch_requested: {} {}-", epoch_sent,
		   epoch_requested, sevt.print());
    desc = fmt::to_string(bf);
  };

  ~PgScrubEvent() {} // RRR make sure it was really needed

  [[nodiscard]] const boost::statechart::event_base& get_event() const
  {
    return *evt;
  }

  [[nodiscard]] std::string get_desc() const
  {
    return desc;
  }

  [[nodiscard]] epoch_t get_epoch_sent() const
  {
    return epoch_sent;
  }

  [[nodiscard]] epoch_t get_epoch_requested() const
  {
    return epoch_requested;
  }
};
typedef std::shared_ptr<PgScrubEvent> PgScrubEventRef;
typedef std::unique_ptr<PgScrubEvent> PgScrubEventURef;


// test:
static inline PgScrubEvent a_demo_sevt{epoch_t{}, epoch_t{}, Scrub::OnInitDone{}};
#endif

}  // namespace crimson::osd
