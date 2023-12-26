// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "./scrub_job.h"
#include "pg_scrubber.h"

using qu_state_t = Scrub::qu_state_t;
using must_scrub_t = Scrub::must_scrub_t;
using ScrubQContainer = Scrub::ScrubQContainer;
using sched_params_t = Scrub::sched_params_t;
using OSDRestrictions = Scrub::OSDRestrictions;
using ScrubJob = Scrub::ScrubJob;


// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

#define dout_subsys ceph_subsys_osd
#undef dout_context
#define dout_context (cct)
#undef dout_prefix
#define dout_prefix _prefix_fn(_dout, this, __func__)

template <class T>
static std::ostream& _prefix_fn(std::ostream* _dout, T* t, std::string fn = "")
{
  return t->gen_prefix(*_dout, fn);
}
// ////////////////////////////////////////////////////////////////////////// //
// ScrubJob

ScrubJob::ScrubJob(
    PgScrubber& scrubber,
    const spg_t& pg)
    : m_scrubber{scrubber}
    , m_pgid{pg}
    , m_shallow_target{pg, scrub_level_t::shallow}
    , m_deep_target{pg, scrub_level_t::deep}
    , m_osd_scrub{m_scrubber.m_osds->get_scrub_services()}
    , m_queue{m_scrubber.m_osds->get_scrub_services().get_scrub_queue()}
{}

void ScrubJob::reset()
{
  ceph_assert(!in_queue());
  m_shallow_target.reset();
  m_deep_target.reset();
}


bool ScrubJob::in_queue() const
{
  return m_shallow_target.in_queue || m_deep_target.in_queue;
}

void ScrubJob::mark_target_dequeued(scrub_level_t scrub_level)
{
  auto& trgt = get_target(scrub_level);
  trgt.clear_queued();
}


std::string_view ScrubJob::registration_state() const
{
  return in_queue() ? "in-queue" : "not-queued";
}


// debug usage only
namespace std {
ostream& operator<<(ostream& out, const ScrubJob& sjob)
{
  return out << fmt::format("{}", sjob);
}
}  // namespace std

void ScrubJob::update_schedule(const Scrub::scrub_schedule_t& adjusted)
{
  schedule = adjusted;
  penalty_timeout = utime_t(0, 0);  // helps with debugging

  // 'updated' is changed here while not holding jobs_lock. That's OK, as
  // the (atomic) flag will only be cleared by select_pg_and_scrub() after
  // scan_penalized() is called and the job was moved to the to_scrub queue.
  updated = true;
  dout(10) << fmt::format(
		  "adjusted: {:s} ({})", schedule.scheduled_at,
		  registration_state())
	   << dendl;
}

std::string ScrubJob::scheduling_state(utime_t now_is, bool is_deep_expected)
    const
{
  // if not in the OSD scheduling queues, not a candidate for scrubbing
  if (state != qu_state_t::registered) {
    return "no scrub is scheduled";
  }

  // if the time has passed, we are surely in the queue
  // (note that for now we do not tell client if 'penalized')
  if (now_is > schedule.scheduled_at) {
    // we are never sure that the next scrub will indeed be shallow:
    return fmt::format("queued for {}scrub", (is_deep_expected ? "deep " : ""));
  }

  return fmt::format(
      "{}scrub scheduled @ {:s}", (is_deep_expected ? "deep " : ""),
      schedule.scheduled_at);
}

std::ostream& ScrubJob::gen_prefix(std::ostream& out, std::string_view fn) const
{
  return out << log_msg_prefix << fn << ": ";
}

// clang-format off
std::string_view ScrubJob::qu_state_text(qu_state_t st)
{
  switch (st) {
    case qu_state_t::not_registered: return "not registered w/ OSD"sv;
    case qu_state_t::registered: return "registered"sv;
    case qu_state_t::unregistering: return "unregistering"sv;
  }
  // g++ (unlike CLANG), requires an extra 'return' here
  return "(unknown)"sv;
}
// clang-format on

void ScrubJob::dump(ceph::Formatter* f) const
{
  f->open_object_section("scrub");
  f->dump_stream("pgid") << pgid;
  f->dump_stream("sched_time") << schedule.scheduled_at;
  f->dump_stream("deadline") << schedule.deadline;
  f->dump_bool("forced",
	       schedule.scheduled_at == PgScrubber::scrub_must_stamp());
  f->close_section();
}
