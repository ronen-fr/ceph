// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
#include "config_values.h"
#include "common/debug.h"


#include "config.h"
#if WITH_SEASTAR
#include "crimson/common/log.h"
#endif

#include "common/config_proxy.h"

#include "common/config_cacher.h"

#include "common/config_obs_mgr.h"

template<class ConfigObs>
void ObserverMgr<ConfigObs>::add_observer(ConfigObs* observer)
{
  const char **keys = observer->get_tracked_conf_keys();
  dout_emergency(fmt::format("\nRRRaddobserver {} - {}\n", keys[0], "observer"));
  auto ptr = std::make_shared<ConfigObs*>(observer);
  for (const char ** k = keys; *k; ++k) {
    dout_emergency(fmt::format("\tRRRaddobserverk {}\n", *k));
    observers.emplace(*k, ptr);
  }
}

template class ObserverMgr<md_config_obs_t>;



template <typename VT>
  void md_config_cacher_t<VT>::handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed)  {
    dout_emergency(fmt::format("RRRcatcher {} - {} - {}\n", option_name, 999/*conf.get_val<VT>(option_name)*/, changed.count(option_name)));
    if (changed.count(option_name)) {
      value_cache.store(conf.get_val<VT>(option_name));
    }
  }

template <>
  void md_config_cacher_t<long>::handle_conf_change(const ConfigProxy& conf,
                          const std::set<std::string>& changed)  {
//     ldout(g_ceph_context, 3) << __func__ << " RRR " << option_name << " - " << conf.get_val<long>(option_name) <<
// " - " changed.count(option_name) <<
// dendl;
    dout_emergency(fmt::format("RRRcatcher {} - {} - {}\n", option_name, conf.get_val<long>(option_name), changed.count(option_name)));
    if (changed.count(option_name)) {
      value_cache.store(conf.get_val<long>(option_name));
    }
  }



template class md_config_cacher_t<bool>;
//template class md_config_cacher_t<int>;
template class md_config_cacher_t<long>;


template class md_config_cacher_t<unsigned long>;
template class md_config_cacher_t<Option::size_t>;



ConfigValues::set_value_result_t
ConfigValues::set_value(const std::string_view key,
                        Option::value_t&& new_value,
                        int level)
{  
  if (auto p = values.find(key); p != values.end()) {
    auto q = p->second.find(level);
    if (q != p->second.end()) {
      if (new_value == q->second) {
        return SET_NO_CHANGE;
      }
      q->second = std::move(new_value);
    } else {
      p->second[level] = std::move(new_value);
    }
    if (p->second.rbegin()->first > level) {
      // there was a higher priority value; no effect
      return SET_NO_EFFECT;
    } else {
      return SET_HAVE_EFFECT;
    }
  } else {
    values[key][level] = std::move(new_value);
    return SET_HAVE_EFFECT;
  }
}

int ConfigValues::rm_val(const std::string_view key, int level)
{
  auto i = values.find(key);
  if (i == values.end()) {
    return -ENOENT;
  }
  auto j = i->second.find(level);
  if (j == i->second.end()) {
    return -ENOENT;
  }
  bool matters = (j->first == i->second.rbegin()->first);
  i->second.erase(j);
  if (matters) {
    return SET_HAVE_EFFECT;
  } else {
    return SET_NO_EFFECT;
  }
}

std::pair<Option::value_t, bool>
ConfigValues::get_value(const std::string_view name, int level) const
{
  auto p = values.find(name);
  if (p != values.end() && !p->second.empty()) {
    // use highest-priority value available (see CONF_*)
    if (level < 0) {
      return {p->second.rbegin()->second, true};
    } else if (auto found = p->second.find(level);
               found != p->second.end()) {
      return {found->second, true};
    }
  }
  return {Option::value_t{}, false};
}

void ConfigValues::set_logging(int which, const char* val)
{
  int log, gather;
  int r = sscanf(val, "%d/%d", &log, &gather);
  if (r >= 1) {
    if (r < 2) {
      gather = log;
    }
    subsys.set_log_level(which, log);
    subsys.set_gather_level(which, gather);
#if WITH_SEASTAR
    crimson::get_logger(which).set_level(crimson::to_log_level(log));
#endif
  }
}

bool ConfigValues::contains(const std::string_view key) const
{
  return values.count(key);
}
