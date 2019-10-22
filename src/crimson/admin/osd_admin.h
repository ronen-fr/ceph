// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2019 Red Hat Inc.
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once

#include <memory>
#include "common/ceph_context.h"

//namespace ceph; 
namespace ceph::osd {
class OSD;
class OsdAdminImp;

/*!
  \brief implementation of the configuration-related 'admin_socket' API of
         (Crimson) OSD

  Main functionality:
  - ...
 */
class OsdAdmin {
  std::unique_ptr<ceph::osd::OsdAdminImp> m_imp;
public:
  OsdAdmin(ceph::osd::OSD* osd, CephContext* cct, ceph::common::ConfigProxy& conf);
  ~OsdAdmin();
  void unregister_admin_commands();
};

}
