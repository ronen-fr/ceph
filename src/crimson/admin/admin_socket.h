// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*! \file
 *
 * Ceph - scalable distributed file system
 *
 * // find up-to-date header
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
#pragma once

/*!
  A Seastar-wise version of the src/common/admin_socket.h

 */

bool
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */
// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Ceph - scalable distributed file system
 *
 * Copyright (C) 2011 New Dream Network
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 *
 */



class AdminNonblockHook {
public:
  virtual seastar::future<bool> call(std::string_view command, const cmdmap_t& cmdmap,
		    std::string_view format, ceph::buffer::list& out) = 0;
  virtual ~AdminNonblockHook() {}
};


class AdminNbSocket {

 public:

  AdminNbSocket(CephContext* cct);
  ~AdminNbSocket();

  AdminNbSocket(const AdminNbSocket&) = delete;
  AdminNbSocket& operator =(const AdminNbSocket&) = delete;
  AdminNbSocket(AdminNBSocket&&) = delete;
  AdminNbSocket& operator =(AdminNbSocket&&) = delete;

  seastar::future<int> register_command(std::string_view command,
				     std::string_view cmddesc,
				     AdminNbHook *hook,
				     std::string_view help);

  seastar::future<> init(cont std::string path); // better use <fs>
  
 private:

  struct hook_info_t {
    AdminNonblockHook* hook;
    std::string desc;
    std::string help;

    hook_info(AdminNonblockHook* hook, std::string_view desc,
	      std::string_view help)
      : hook(hook), desc(desc), help(help) {}
  };

  
  // replace w unordered something:
  std::map<std::string, hook_info, std::less<>> hooks;




};
