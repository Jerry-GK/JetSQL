#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, IndexIteratorTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  BasicComparator<int> comparator;
  int page_size = 40;
  int record_count = 100000;
  BPlusTree<int, int, BasicComparator<int>> tree(0, engine.bpm_, comparator, page_size, page_size);
  // Insert and delete record
  for (int i = 1; i <= record_count; i++) {
    tree.Insert(i, i * 100, nullptr);
  }
  for (int i = 2; i <= record_count; i += 2) {
    tree.Remove(i);
  }
  // Search keys
  vector<int> v;
  for (int i = 2; i <= record_count; i += 2) {
    ASSERT_FALSE(tree.GetValue(i, v));
  }
  for (int i = 1; i <= record_count  -1; i += 2) {
    ASSERT_TRUE(tree.GetValue(i, v));
    ASSERT_EQ(i * 100, v[v.size() - 1]);
  }
  // Iterator
  int ans = 1;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter, ans += 2) {
    EXPECT_EQ(ans, (*iter).first);
    EXPECT_EQ(ans * 100, (*iter).second);
  }
}
