// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
 * \file fmtlib formatters for some hobject.h classes
 */
#include <fmt/format.h>

#include "common/hobject.h"
#include "include/types_fmt.h"
#include "msg/msg_fmt.h"


template <> struct fmt::formatter<hobject_t> {
  constexpr auto parse(format_parse_context& ctx)
  {
    return ctx.begin();
  }

  template <typename FormatContext> auto format(const hobject_t& ho, FormatContext& ctx)
    {
    // for now - just use the existing operator
    stringstream sst;
    sst << ho;
    return fmt::format_to(ctx.out(), "{}", sst.str());
    }
};


