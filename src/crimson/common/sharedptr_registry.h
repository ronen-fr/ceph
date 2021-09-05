// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#pragma once

#include <map>
#include <memory>
#include <optional>

//#include "common/ceph_mutex.h"


namespace crimson::osd {

/**
 * Provides a registry of shared_ptr<V> indexed by K while
 * the references are alive.
 */
template <class K, class V, class C = std::less<K>> class SharedPtrRegistry {
 public:
  typedef std::shared_ptr<V> VPtr;
  typedef std::weak_ptr<V> WeakVPtr;
  int waiting;

 private:
  // ceph::mutex lock = ceph::make_mutex("SharedPtrRegistry::lock");
  // ceph::condition_variable cond;
  std::map<K, std::pair<WeakVPtr, V*>, C> contents;

  class OnRemoval {
    SharedPtrRegistry<K, V, C>* parent;
    K key;

   public:
    OnRemoval(SharedPtrRegistry<K, V, C>* parent, K key) : parent(parent), key(key) {}
    void operator()(V* to_remove)
    {
      {
	// std::lock_guard l(parent->lock);
	auto i = parent->contents.find(key);
	if (i != parent->contents.end() && i->second.second == to_remove) {
	  parent->contents.erase(i);
	  //parent->cond.notify_all();
	}
      }
      delete to_remove;
    }
  };
  friend class OnRemoval;

 public:
  SharedPtrRegistry() : waiting(0) {}

  bool empty()
  {
    return contents.empty();
  }

  bool get_next(const K& key, std::pair<K, VPtr>* next)
  {
    std::pair<K, VPtr> r;
    {
      // std::lock_guard l(lock);
      VPtr next_val;
      auto i = contents.upper_bound(key);
      while (i != contents.end() && !(next_val = i->second.first.lock()))
	++i;
      if (i == contents.end())
	return false;
      if (next)
	r = std::make_pair(i->first, next_val);
    }
    if (next)
      *next = r;
    return true;
  }


  bool get_next(const K& key, std::pair<K, V>* next)
  {
    VPtr next_val;
    // std::lock_guard l(lock);
    auto i = contents.upper_bound(key);
    while (i != contents.end() && !(next_val = i->second.first.lock()))
      ++i;
    if (i == contents.end())
      return false;
    if (next)
      *next = std::make_pair(i->first, *next_val);
    return true;
  }

  // a 'pipelined-versioned' get_next()
  auto get_next(const K& key) -> std::optional<std::pair<K, V>> const
  {
    VPtr next_val;
    auto i = contents.upper_bound(key);
    while (i != contents.end() && !(next_val = i->second.first.lock()))
      ++i;
    if (i == contents.end())
      return std::nullopt;

    return std::optional<std::pair<K, V>>(std::make_pair<K, V>(std::remove_const_t<K>{i->first}, std::move(*next_val)));
    //return std::optional<std::pair<K, V>>(std::make_pair<K, V>(std::string{i->first}, std::move(*next_val)));
  }

  /*
  auto get_next2(const std::string& key) -> std::optional<std::pair<std::string, boost::optional<::ceph::bufferlist>>>
  {
    shared_ptr< boost::optional<::ceph::bufferlist> > next_val;
    auto i = contents.upper_bound(key);
    while (i != contents.end() && !(next_val = i->second.first.lock()))
      ++i;
    if (i == contents.end())
      return std::nullopt;

    return std::optional<std::pair<const std::string, boost::optional<::ceph::bufferlist>>>(
    	std::make_pair<std::string, boost::optional<::ceph::bufferlist>>(
    	std::string{i->first}, std::move(*next_val)
    	));
  }
   */



  VPtr lookup(const K& key)
  {
    // std::unique_lock l(lock);
    // waiting++;
    // while (1) {
    typename std::map<K, std::pair<WeakVPtr, V*>, C>::iterator i = contents.find(key);
    if (i != contents.end()) {
      VPtr retval = i->second.first.lock();
      if (retval) {
	return retval;
      }
    } else {
      return VPtr();
    }
    // cond.wait(l);
    //}
    // waiting--;
    return VPtr();
  }

  VPtr lookup_or_create(const K& key)
  {
    // std::unique_lock l(lock);
    // waiting++;
    // while (1) {
    auto i = contents.find(key);
    if (i != contents.end()) {
      VPtr retval = i->second.first.lock();
      if (retval) {
	// waiting--;
	return retval;
      }
      //} else {
      //	break;
    }
    // cond.wait(l);
    //}

    V* ptr = new V();
    VPtr retval(ptr, OnRemoval(this, key));
    contents.insert(std::make_pair(key, make_pair(retval, ptr)));
    // waiting--;
    return retval;
  }

  VPtr lookup_or_create(K key)
  {
    // std::unique_lock l(lock);
    // waiting++;
    // while (1) {
    auto i = contents.find(key);
    if (i != contents.end()) {
      VPtr retval = i->second.first.lock();
      if (retval) {
	// waiting--;
	return retval;
      }
      //} else {
      //	break;
    }
    // cond.wait(l);
    //}

    V* ptr = new V();
    VPtr retval(ptr, OnRemoval(this, key));
    contents.insert(std::make_pair(key, make_pair(retval, ptr)));
    // waiting--;
    return retval;
  }

  unsigned size()
  {
    // std::lock_guard l(lock);
    return contents.size();
  }

  void remove(const K& key)
  {
    // std::lock_guard l(lock);
    contents.erase(key);
    // cond.notify_all();
  }

  template <class A> VPtr lookup_or_create(const K& key, const A& arg)
  {
    // std::unique_lock l(lock);
    // waiting++;
    // while (1) {
    auto i = contents.find(key);
    if (i != contents.end()) {
      VPtr retval = i->second.first.lock();
      if (retval) {
	// waiting--;
	return retval;
      }
      //} else {
      // break;
    }
    // cond.wait(l);
    //}
    V* ptr = new V(arg);
    VPtr retval(ptr, OnRemoval(this, key));
    contents.insert(std::make_pair(key, make_pair(retval, ptr)));
    // waiting--;
    return retval;
  }

  friend class SharedPtrRegistryTest;
};

}  // namespace crimson::osd
