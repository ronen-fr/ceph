// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include <iostream>

#include "common/not_before_queue.h"
#include "gtest/gtest.h"

// Just to have a default constructor that sets it to 0
struct test_time_t {
  unsigned time = 0;

  operator unsigned() const { return time; }
  test_time_t() = default;
  test_time_t(unsigned t) : time(t) {}
  test_time_t &operator=(unsigned t) {
    time = t;
    return *this;
  }
};

struct tv_t {
  unsigned not_before = 0;
  unsigned ordering_value = 0;
  unsigned removal_class = 0;

  tv_t() = default;
  tv_t(const tv_t &) = default;
  tv_t(unsigned not_before, unsigned ov, unsigned rc)
    : not_before(not_before), ordering_value(ov), removal_class(rc) {}

  auto to_tuple() const {
    return std::make_tuple(not_before, ordering_value, removal_class);
  }
  bool operator==(const tv_t &rhs) const {
    return to_tuple() == rhs.to_tuple();
  }
};

std::ostream &operator<<(std::ostream &lhs, const tv_t &val) {
  return lhs << val.to_tuple();
}

const unsigned &project_not_before(const tv_t &v) {
  return v.not_before;
}

const unsigned &project_removal_class(const tv_t &v) {
  return v.removal_class;
}

bool operator<(const tv_t &lhs, const tv_t &rhs) {
  return lhs.ordering_value < rhs.ordering_value;
}

class NotBeforeTest : public testing::Test {
 public:
  using queue_t = not_before_queue_t<tv_t, test_time_t>;

  void load_test_data(const std::vector<tv_t> &dt) {
    for (const auto &d : dt) {
      queue.enqueue(d);
    }
  }

 protected:
  queue_t queue;

  void dump() {
    std::cout << "Dumping queue: " << std::endl;
    queue.for_each([](auto v, bool eligible) {
      std::cout << "    item: " << v << ", eligible: " << eligible << std::endl;
    });
  }
};

TEST_F(NotBeforeTest, Basic) {
  tv_t e0{0, 0, 0};
  tv_t e1{0, 1, 0};

  queue.enqueue(e0);
  queue.enqueue(e1);

  ASSERT_EQ(queue.dequeue(), std::make_optional(e0));
  ASSERT_EQ(queue.dequeue(), std::make_optional(e1));
  ASSERT_EQ(queue.dequeue(), std::nullopt);
}

TEST_F(NotBeforeTest, NotBefore) {
  tv_t e0{0, 0, 0};
  tv_t e1{1, 1, 0};
  tv_t e2{1, 2, 0};
  tv_t e3{1, 3, 0};
  tv_t e4{0, 4, 0};
  tv_t e5{0, 5, 0};

  queue.enqueue(e5);
  queue.enqueue(e1);
  queue.enqueue(e3);
  queue.enqueue(e0);
  queue.enqueue(e2);
  queue.enqueue(e4);

  ASSERT_EQ(queue.dequeue(), std::make_optional(e0));
  ASSERT_EQ(queue.dequeue(), std::make_optional(e4));
  ASSERT_EQ(queue.dequeue(), std::make_optional(e5));
  ASSERT_EQ(queue.dequeue(), std::nullopt);

  queue.advance_time(1);

  EXPECT_EQ(queue.dequeue(), std::make_optional(e1));
  EXPECT_EQ(queue.dequeue(), std::make_optional(e2));
  EXPECT_EQ(queue.dequeue(), std::make_optional(e3));
  ASSERT_EQ(queue.dequeue(), std::nullopt);
}

TEST_F(NotBeforeTest, RemoveByClass) {
  tv_t e0{0, 0, 1};
  tv_t e1{1, 1, 0};
  tv_t e2{1, 2, 1};
  tv_t e3{1, 3, 1};
  tv_t e4{0, 4, 1};
  tv_t e5{0, 5, 0};

  queue.enqueue(e5);
  queue.enqueue(e1);
  queue.enqueue(e3);
  queue.enqueue(e0);
  queue.enqueue(e2);
  queue.enqueue(e4);

  ASSERT_EQ(queue.dequeue(), std::make_optional(e0));

  queue.remove_by_class(1u);

  ASSERT_EQ(queue.dequeue(), std::make_optional(e5));
  ASSERT_EQ(queue.dequeue(), std::nullopt);

  queue.advance_time(1u);

  EXPECT_EQ(queue.dequeue(), std::make_optional(e1));
  ASSERT_EQ(queue.dequeue(), std::nullopt);
}

TEST_F(NotBeforeTest, DequeueByPred) {
  // the predicate we'll use is against the removal class
  const auto pred = [](const tv_t &v) { return 0 == (v.removal_class % 2); };

  tv_t e0t{1, 0, 10};
  tv_t e1t{2, 2, 20};
  tv_t e2t{1, 1, 12};
  tv_t e3t{2, 1, 40};
  tv_t e0f{1, 1, 17};
  tv_t e1f{2, 2, 27};
  tv_t e2f{1, 0, 17};
  tv_t e3f{2, 1, 47};

  queue.enqueue(e0f);
  queue.enqueue(e1f);
  queue.enqueue(e2f);
  queue.enqueue(e3f);
  queue.enqueue(e0t);
  queue.enqueue(e1t);
  queue.enqueue(e2t);
  queue.enqueue(e3t);

  // no ready entries
  ASSERT_EQ(queue.dequeue(), std::nullopt);
  ASSERT_EQ(queue.dequeue_by_pred(pred), std::nullopt);

  // advance time to make e0* and e2* ready
  queue.advance_time(1);
  ASSERT_EQ(queue.dequeue_by_pred(pred), std::make_optional(e0t));
  ASSERT_EQ(queue.dequeue_by_pred(pred), std::make_optional(e2t));
  ASSERT_EQ(queue.dequeue_by_pred(pred), std::nullopt);

  // advance time to make e1* and e3* ready
  queue.advance_time(4);
  ASSERT_EQ(queue.dequeue_by_pred(pred), std::make_optional(e3t));
  ASSERT_EQ(queue.dequeue_by_pred(pred), std::make_optional(e1t));
  ASSERT_EQ(queue.dequeue_by_pred(pred), std::nullopt);

  // without the condition?
  ASSERT_EQ(queue.dequeue(), std::make_optional(e2f));
}

namespace {
  // clang-format off
  std::vector<tv_t> by_class_test_data_1{
    {0, 20, 17}, {2, 10, 17},
    {0, 20, 23}, {2, 10, 23},
    {0, 10, 17}, {2, 20, 17},
    {0, 10, 23}, {2, 10, 23},
    {7, 41, 57}, {2, 41, 57},
    {7, 42, 53}, {2, 42, 53},
    {7, 43, 57}, {2, 43, 57},
    {7, 44, 53}, {2, 44, 53}
  };
  // clang-format on
}  // namespace

TEST_F(NotBeforeTest, RemoveIfByClass_eligible) {
  load_test_data(by_class_test_data_1);
  queue.advance_time(1);
  ASSERT_EQ(queue.total_count(), 16);
  ASSERT_EQ(queue.eligible_count(), 4);

  int cnt = queue.remove_if_by_class(17, [](const tv_t &v) { return true; }, 1);
  ASSERT_EQ(cnt, 1);
  cnt = queue.remove_if_by_class(17, [](const tv_t &v) { return true; }, 10);
  ASSERT_EQ(cnt, 3);
  // to be continued...
}




