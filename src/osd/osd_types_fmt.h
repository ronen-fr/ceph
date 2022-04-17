// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
#pragma once
/**
 * \file fmtlib formatters for some types.h classes
 */

#include "common/hobject_fmt.h"
#include "osd/osd_types.h"
#include "include/types_fmt.h"

template <>
struct fmt::formatter<osd_reqid_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const osd_reqid_t& req_id, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "{}.{}:{}", req_id.name, req_id.inc,
			  req_id.tid);
  }
};

template <>
struct fmt::formatter<pg_shard_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const pg_shard_t& shrd, FormatContext& ctx)
  {
    if (shrd.is_undefined()) {
      return fmt::format_to(ctx.out(), "?");
    }
    if (shrd.shard == shard_id_t::NO_SHARD) {
      return fmt::format_to(ctx.out(), "{}", shrd.get_osd());
    }
    return fmt::format_to(ctx.out(), "{}({})", shrd.get_osd(), shrd.shard);
  }
};

template <>
struct fmt::formatter<eversion_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const eversion_t& ev, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "{}'{}", ev.epoch, ev.version);
  }
};

template <>
struct fmt::formatter<chunk_info_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const chunk_info_t& ci, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "(len: {} oid: {} offset: {} flags: {})",
			  ci.length, ci.oid, ci.offset,
			  ci.get_flag_string(ci.flags));
  }
};

template <>
struct fmt::formatter<object_manifest_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const object_manifest_t& om, FormatContext& ctx)
  {
    fmt::format_to(ctx.out(), "manifest({}", om.get_type_name());
    if (om.is_redirect()) {
      fmt::format_to(ctx.out(), " {}", om.redirect_target);
    } else if (om.is_chunked()) {
      fmt::format_to(ctx.out(), " {}", om.chunk_map);
    }
    return fmt::format_to(ctx.out(), ")");
  }
};

template <>
struct fmt::formatter<object_info_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const object_info_t& oi, FormatContext& ctx)
  {
    fmt::format_to(ctx.out(), "{}({} {} {} s {} uv {}", oi.soid, oi.version,
		   oi.last_reqid, (oi.flags ? oi.get_flag_string() : ""), oi.size,
		   oi.user_version);
    if (oi.is_data_digest()) {
      fmt::format_to(ctx.out(), " dd {:x}", oi.data_digest);
    }
    if (oi.is_omap_digest()) {
      fmt::format_to(ctx.out(), " od {:x}", oi.omap_digest);
    }

    fmt::format_to(ctx.out(), " alloc_hint [{} {} {}]", oi.expected_object_size,
		   oi.expected_write_size, oi.alloc_hint_flags);

    if (oi.has_manifest()) {
      fmt::format_to(ctx.out(), " {}", oi.manifest);
    }
    return fmt::format_to(ctx.out(), ")");
  }
};

template <>
struct fmt::formatter<pg_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const pg_t& pg, FormatContext& ctx)
  {
    return fmt::format_to(ctx.out(), "{}.{}", pg.pool(), pg.m_seed);
  }
};


template <>
struct fmt::formatter<spg_t> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const spg_t& spg, FormatContext& ctx)
  {
    if (shard_id_t::NO_SHARD == spg.shard.id) {
      return fmt::format_to(ctx.out(), "{}", spg.pgid);
    } else {
      return fmt::format_to(ctx.out(), "{}s{}>", spg.pgid, spg.shard.id);
    }
  }
};

template <>
struct fmt::formatter<ScrubMap::object> {
  constexpr auto parse(format_parse_context& ctx) { return ctx.begin(); }

  template <typename FormatContext>
  auto format(const ScrubMap::object& so, FormatContext& ctx)
  {

    fmt::format_to(ctx.out(),
                   "so{{ sz:{} dd:{} od:{} ",
                   so.size,
                   so.digest,
                   so.digest_present);

    // note the special handling of (1) OI_ATTR and (2) non-printables
    for (auto [k, v] : so.attrs) {
      std::string bkstr{v.raw_c_str(), v.raw_length()};
        if (k == std::string{OI_ATTR}) {
            bkstr = "<<OI_ATTR>>";
        }
      fmt::format_to(ctx.out(), "{{{}:{}({})}} ", k, bkstr, bkstr.length());
    }

    return fmt::format_to(ctx.out(), "}}");
  }
};

template <>
struct fmt::formatter<ScrubMap> {
  template <typename ParseContext>
  constexpr auto parse(ParseContext& ctx)
  {
    auto it = ctx.begin();
    if (it != ctx.end() && *it == 'D') {
      debug_log = true;  // list the objects
      ++it;
    }
    return it;
  }

  template <typename FormatContext>
  auto format(const ScrubMap& smap, FormatContext& ctx)
  {
    fmt::format_to(ctx.out(),
                   "smap{{ valid:{} inc-since:{} #:{}",
                   smap.valid_through,
                   smap.incr_since,
                   smap.objects.size());
    if (debug_log) {
      fmt::format_to(ctx.out(), " objects:");
      for (const auto& [ho, so] : smap.objects) {
        fmt::format_to(ctx.out(), "\n\th.o<{}>:<{}> ", ho, so);
      }
      fmt::format_to(ctx.out(), "\n");
    }
    return fmt::format_to(ctx.out(), "}}");
  }

  bool debug_log{false};
};