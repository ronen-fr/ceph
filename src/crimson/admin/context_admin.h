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

class ContextConfigAdminImp;

/*!
  \brief implementation of the configuration-related 'admin_socket' API of
         (Crimson) Ceph Context

  Main functionality:
  - manipulating Context-level configuraion
  - process-wide commands ('abort', 'assert')
  - ...
 */
class ContextConfigAdmin {
  std::unique_ptr<ContextConfigAdminImp> m_imp;
public:
  ContextConfigAdmin(CephContext* cct, ceph::common::ConfigProxy& conf);
  ~ContextConfigAdmin();
  seastar::future<> unregister_admin_commands();
};


#if 0
class ContextMiscAdminImp;

/*!
  \brief implementation of the 'admin_socket' API of (Crimson) Ceph Context

  Main functionality:
  - manipulating Context-level configuraion
  - process-wide commands ('abort', 'assert')
  - ...
 */
class ContextConfigAdmin {
  std::unique_ptr<ContextConfigAdminImp> m_imp;
public:
  ContextConfigAdmin(CephContext* cct, ceph::common::ConfigProxy& conf);
  ~ContextConfigAdmin();
  void unregister_admin_commands();
};
#endif



