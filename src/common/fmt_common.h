// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once

/**
 * \file default fmtlib formatters for specifically-tagged types
 */
#include <fmt/format.h>
#include <fmt/ranges.h>

//#include "include/rados/rados_types.hpp"

/**
 * Tagging classes that provide support for default fmtlib formatting,
 * by providing either
 * std::string fmt_print() const
 * *or*
 * std::string alt_fmt_print(bool short_format) const
 *
 * Classes matching one of these two concepts might be provided with
 * a default formatter.
 */
template<class T>
concept has_fmt_print = requires(T t) {
  { t.fmt_print() } -> std::same_as<std::string>;
};
template<class T>
concept has_alt_fmt_print = requires(T t) {
  { t.alt_fmt_print(bool{}) } -> std::same_as<std::string>;
};


namespace fmt {

template <has_alt_fmt_print T>
struct formatter<T> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx) {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 's') {
      verbose = false;
      ++it;
    }
    return it;
  }
  template <typename FormatContext>
  auto format(const T& k, FormatContext& ctx) const {
    if (verbose) {
      return fmt::format_to(ctx.out(), "{}", k.alt_fmt_print(true));
    }
    return fmt::format_to(ctx.out(), "{}", k.alt_fmt_print(false));
  }
  bool verbose{true};
};

}  // namespace fmt
