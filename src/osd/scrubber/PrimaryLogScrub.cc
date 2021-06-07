// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "PrimaryLogScrub.h"

#include "common/scrub_types.h"

#include "osd/PeeringState.h"
#include "osd/PrimaryLogPG.h"
#include "scrub_machine.h"

#define dout_context (m_pg->cct)
#define dout_subsys ceph_subsys_osd
#undef dout_prefix
#define dout_prefix _prefix(_dout, this->m_pg)

using std::vector;

template <class T> static ostream& _prefix(std::ostream* _dout, T* t)
{
  return t->gen_prefix(*_dout) << " PrimaryLog scrubber pg(" << t->pg_id << ") ";
}

using namespace Scrub;
using Scrub::ScrubMachine;

bool PrimaryLogScrub::get_store_errors(const scrub_ls_arg_t& arg,
				       scrub_ls_result_t& res_inout) const
{
  if (!m_store) {
    return false;
  }

  if (arg.get_snapsets) {
    res_inout.vals =
      m_store->get_snap_errors(m_pg->get_pgid().pool(), arg.start_after, arg.max_return);
  } else {
    res_inout.vals = m_store->get_object_errors(m_pg->get_pgid().pool(), arg.start_after,
						arg.max_return);
  }
  return true;
}

//  forwarders used by the scrubber backend

ObjectContextRef PrimaryLogScrub::get_object_context(const hobject_t& obj)
{
  return m_pl_pg->get_object_context(obj, false);
}

PrimaryLogPG::OpContextUPtr PrimaryLogScrub::get_mod_op_context(ObjectContextRef obj_ctx)
{
  auto opc = m_pl_pg->simple_opc_create(obj_ctx);
  opc->at_version = m_pl_pg->get_next_version();
  opc->mtime = utime_t(); // do not update time
  return opc;
}

void PrimaryLogScrub::simple_opc_submit(PrimaryLogPG::OpContextUPtr&& op_ctx)
{
  m_pl_pg->simple_opc_submit(std::forward<PrimaryLogPG::OpContextUPtr>(op_ctx));
}

void PrimaryLogScrub::finish_ctx(PrimaryLogPG::OpContext* op_ctx, int log_op_type)
{
  m_pl_pg->finish_ctx(op_ctx, log_op_type);
}

void PrimaryLogScrub::add_to_stats(const object_stat_sum_t& stat)
{
  m_scrub_cstat.add(stat);
}


void PrimaryLogScrub::_scrub_finish()
{
  auto& info = m_pg->info;  ///< a temporary alias

  dout(10) << __func__
	   << " info stats: " << (info.stats.stats_invalid ? "invalid" : "valid")
	   << dendl;

  if (info.stats.stats_invalid) {
    m_pl_pg->recovery_state.update_stats([=](auto& history, auto& stats) {
      stats.stats = m_scrub_cstat;
      stats.stats_invalid = false;
      return false;
    });

    if (m_pl_pg->agent_state)
      m_pl_pg->agent_choose_mode();
  }

  dout(10) << m_mode_desc << " got " << m_scrub_cstat.sum.num_objects << "/"
	   << info.stats.stats.sum.num_objects << " objects, "
	   << m_scrub_cstat.sum.num_object_clones << "/"
	   << info.stats.stats.sum.num_object_clones << " clones, "
	   << m_scrub_cstat.sum.num_objects_dirty << "/"
	   << info.stats.stats.sum.num_objects_dirty << " dirty, "
	   << m_scrub_cstat.sum.num_objects_omap << "/"
	   << info.stats.stats.sum.num_objects_omap << " omap, "
	   << m_scrub_cstat.sum.num_objects_pinned << "/"
	   << info.stats.stats.sum.num_objects_pinned << " pinned, "
	   << m_scrub_cstat.sum.num_objects_hit_set_archive << "/"
	   << info.stats.stats.sum.num_objects_hit_set_archive << " hit_set_archive, "
	   << m_scrub_cstat.sum.num_bytes << "/" << info.stats.stats.sum.num_bytes
	   << " bytes, " << m_scrub_cstat.sum.num_objects_manifest << "/"
	   << info.stats.stats.sum.num_objects_manifest << " manifest objects, "
	   << m_scrub_cstat.sum.num_bytes_hit_set_archive << "/"
	   << info.stats.stats.sum.num_bytes_hit_set_archive << " hit_set_archive bytes."
	   << dendl;

  if (m_scrub_cstat.sum.num_objects != info.stats.stats.sum.num_objects ||
      m_scrub_cstat.sum.num_object_clones != info.stats.stats.sum.num_object_clones ||
      (m_scrub_cstat.sum.num_objects_dirty != info.stats.stats.sum.num_objects_dirty &&
       !info.stats.dirty_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_omap != info.stats.stats.sum.num_objects_omap &&
       !info.stats.omap_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_pinned != info.stats.stats.sum.num_objects_pinned &&
       !info.stats.pin_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_hit_set_archive !=
	 info.stats.stats.sum.num_objects_hit_set_archive &&
       !info.stats.hitset_stats_invalid) ||
      (m_scrub_cstat.sum.num_bytes_hit_set_archive !=
	 info.stats.stats.sum.num_bytes_hit_set_archive &&
       !info.stats.hitset_bytes_stats_invalid) ||
      (m_scrub_cstat.sum.num_objects_manifest !=
	 info.stats.stats.sum.num_objects_manifest &&
       !info.stats.manifest_stats_invalid) ||
      m_scrub_cstat.sum.num_whiteouts != info.stats.stats.sum.num_whiteouts ||
      m_scrub_cstat.sum.num_bytes != info.stats.stats.sum.num_bytes) {
    m_osds->clog->error() << info.pgid << " " << m_mode_desc << " : stat mismatch, got "
			  << m_scrub_cstat.sum.num_objects << "/"
			  << info.stats.stats.sum.num_objects << " objects, "
			  << m_scrub_cstat.sum.num_object_clones << "/"
			  << info.stats.stats.sum.num_object_clones << " clones, "
			  << m_scrub_cstat.sum.num_objects_dirty << "/"
			  << info.stats.stats.sum.num_objects_dirty << " dirty, "
			  << m_scrub_cstat.sum.num_objects_omap << "/"
			  << info.stats.stats.sum.num_objects_omap << " omap, "
			  << m_scrub_cstat.sum.num_objects_pinned << "/"
			  << info.stats.stats.sum.num_objects_pinned << " pinned, "
			  << m_scrub_cstat.sum.num_objects_hit_set_archive << "/"
			  << info.stats.stats.sum.num_objects_hit_set_archive
			  << " hit_set_archive, " << m_scrub_cstat.sum.num_whiteouts
			  << "/" << info.stats.stats.sum.num_whiteouts << " whiteouts, "
			  << m_scrub_cstat.sum.num_bytes << "/"
			  << info.stats.stats.sum.num_bytes << " bytes, "
			  << m_scrub_cstat.sum.num_objects_manifest << "/"
			  << info.stats.stats.sum.num_objects_manifest
			  << " manifest objects, "
			  << m_scrub_cstat.sum.num_bytes_hit_set_archive << "/"
			  << info.stats.stats.sum.num_bytes_hit_set_archive
			  << " hit_set_archive bytes.";
    ++m_shallow_errors;

    if (m_is_repair) {
      ++m_fixed_count;
      m_pl_pg->recovery_state.update_stats([this](auto& history, auto& stats) {
	stats.stats = m_scrub_cstat;
	stats.dirty_stats_invalid = false;
	stats.omap_stats_invalid = false;
	stats.hitset_stats_invalid = false;
	stats.hitset_bytes_stats_invalid = false;
	stats.pin_stats_invalid = false;
	stats.manifest_stats_invalid = false;
	return false;
      });
      m_pl_pg->publish_stats_to_osd();
      m_pl_pg->recovery_state.share_pg_info();
    }
  }
  // Clear object context cache to get repair information
  if (m_is_repair)
    m_pl_pg->object_contexts.clear();
}

static bool doing_clones(const std::optional<SnapSet>& snapset,
			 const vector<snapid_t>::reverse_iterator& curclone)
{
  return snapset && curclone != snapset->clones.rend();
}

PrimaryLogScrub::PrimaryLogScrub(PrimaryLogPG* pg) : PgScrubber{pg}, m_pl_pg{pg} {}

void PrimaryLogScrub::_scrub_clear_state()
{
  dout(15) << __func__ << dendl;
  m_scrub_cstat = object_stat_collection_t();
}

void PrimaryLogScrub::stats_of_handled_objects(const object_stat_sum_t& delta_stats,
					       const hobject_t& soid)
{
  // We scrub objects in hobject_t order, so objects before m_start have already been
  // scrubbed and their stats have already been added to the scrubber. Objects after that
  // point haven't been included in the scrubber's stats accounting yet, so they will be
  // included when the scrubber gets to that object.
  dout(15) << __func__ << " soid: " << soid << " scrub is active? " << is_scrub_active()
	   << dendl;
  if (is_primary() && is_scrub_active()) {
    if (soid < m_start) {
      dout(20) << __func__ << " " << soid << " < [" << m_start << "," << m_end << ")"
	       << dendl;
      m_scrub_cstat.add(delta_stats);
    } else {
      dout(20) << __func__ << " " << soid << " >= [" << m_start << "," << m_end << ")"
	       << dendl;
    }
  }
}
