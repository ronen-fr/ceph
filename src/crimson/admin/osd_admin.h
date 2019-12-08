// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <memory>
#include "common/ceph_context.h"

class CephContext;
namespace crimson::osd {
class OSD;
}

namespace crimson::admin {
class OsdAdminImp;

/**
  \brief implementation of the configuration-related 'admin_socket' API of
         (Crimson) OSD

  Main functionality:
  - fetching OSD status data
  - ...
 */
class OsdAdmin {
  std::unique_ptr<crimson::admin::OsdAdminImp> m_imp;

 public:
  OsdAdmin(crimson::osd::OSD*            osd,
           CephContext*                  cct,
           crimson::common::ConfigProxy& conf);
  ~OsdAdmin();
  seastar::future<> register_admin_commands();
  seastar::future<> unregister_admin_commands();
};

}  // namespace crimson::admin
