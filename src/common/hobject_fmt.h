// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
 * \file fmtlib formatters for some hobject.h classes
 */
#include <fmt/compile.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include "common/hobject.h"
#include "msg/msg_fmt.h"


namespace fmt {
template <>
struct formatter<hobject_t> {

  template <typename FormatContext>
  static inline auto
  append_sanitized(FormatContext& ctx, const std::string& in, int sep = 0)
  {
    for (auto i = in.cbegin(); i != in.cend(); ++i) {
      if (*i == '%' || *i == ':' || *i == '/' || *i < 32 || *i >= 127) {
	fmt::format_to(ctx.out(), FMT_COMPILE("%{:02x}"), *i);
      } else {
	fmt::format_to(ctx.out(), FMT_COMPILE("{}"), *i);
      }
    }
    if (sep) {
      fmt::format_to(ctx.out(), FMT_COMPILE("{}"), char(sep));
    }
    return ctx.out();
  }

  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const hobject_t& ho, FormatContext& ctx)
  {
    if (ho == hobject_t{}) {
      return fmt::format_to(ctx.out(), "MIN");
    }

    if (ho.is_max()) {
      return fmt::format_to(ctx.out(), "MAX");
    }

    fmt::format_to(
	ctx.out(), FMT_COMPILE("{}:{:08x}:"), static_cast<uint64_t>(ho.pool),
	ho.get_bitwise_key_u32());
    append_sanitized(ctx, ho.nspace, ':');
    append_sanitized(ctx, ho.get_key(), ':');
    append_sanitized(ctx, ho.oid.name);
    return fmt::format_to(ctx.out(), FMT_COMPILE(":{}"), ho.snap);
  }
};
}  // namespace fmt
