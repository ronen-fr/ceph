// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:nil -*-
// vim: ts=8 sw=2 sts=2 expandtab

#include "crimson/os/seastore/segment_manager.h"
#include "crimson/os/seastore/segment_manager/block.h"
#include "crimson/os/seastore/logging.h"

#ifdef HAVE_ZNS
#include "crimson/os/seastore/segment_manager/zbd.h"
SET_SUBSYS(seastore_device);
#endif


namespace crimson::os::seastore {

std::ostream& operator<<(std::ostream &out, Segment::segment_state_t s)
{
  using state_t = Segment::segment_state_t;
  switch (s) {
  case state_t::EMPTY:
    return out << "EMPTY";
  case state_t::OPEN:
    return out << "OPEN";
  case state_t::CLOSED:
    return out << "CLOSED";
  default:
    return out << "INVALID_SEGMENT_STATE!";
  }
}


seastar::future<crimson::os::seastore::SegmentManagerRef>
SegmentManager::get_segment_manager(
    const std::string& device,
    device_type_t dtype)
{
  const std::string device_block = device + "/block";
#ifdef HAVE_ZNS
  LOG_PREFIX(SegmentManager::get_segment_manager);
  if (dtype == device_type_t::ZBD) {
    auto file =
        co_await seastar::open_file_dma(device_block, seastar::open_flags::rw);

    uint32_t nr_zones = 0;
    std::exception_ptr ep;
    try {
      co_await file.ioctl(BLKGETNRZONES, (void*)&nr_zones);
    } catch (...) {
      // can't rethrow here, as we need that file closed.
      ep = std::current_exception();
      ERROR("ioctl BLKGETNRZONES failed: {}", ep);
    }

    co_await file.close();
    if (ep) {
      std::rethrow_exception(ep);
    }
    INFO("Found {} zones.", nr_zones);
    if (nr_zones != 0) {
      co_return std::make_unique<segment_manager::zbd::ZBDSegmentManager>(
          device_block);
    }
    co_return std::make_unique<segment_manager::block::BlockSegmentManager>(
        device_block, device_type_t::ZBD);
  }
#endif
  co_return std::make_unique<segment_manager::block::BlockSegmentManager>(
      device_block, dtype);
}
}
