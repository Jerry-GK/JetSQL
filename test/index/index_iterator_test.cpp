#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"

static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, IndexIteratorTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  int key_size = 4;
  int leaf_size = (PAGE_SIZE - BPlusTreeLeafPage::GetHeaderSize()) / (sizeof(BLeafEntry) + key_size) ; 
  int internal_size = (PAGE_SIZE - BPlusTreeInternalPage::GetHeaderSize()) / (sizeof(BInternalEntry) + key_size); 
  int record_count = 100000;
  IndexKeyComparator cmp(nullptr);
  BPlusTree tree(0, engine.bpm_, cmp,key_size,leaf_size,internal_size);
  // Insert and delete record
  int v = 0;
  IndexKey * temp = IndexKey::Create(key_size,v);
  for (int i = 1; i <= record_count; i++) {
    temp->SetValue(i);
    tree.Insert(temp, RowId(i * 100), nullptr);
  }
  for (int i = 2; i <= record_count; i += 2) {
    temp->SetValue(i);
    tree.Remove(temp);
  }
  // Search keys
  vector<RowId> r;
  for (int i = 2; i <= record_count; i += 2) {
    temp->SetValue(i);
    ASSERT_FALSE(tree.GetValue(temp, r));
  }
  for (int i = 1; i <= record_count  -1; i += 2) {
    temp->SetValue(i);
    ASSERT_TRUE(tree.GetValue(temp, r));
    ASSERT_EQ(RowId(i * 100), r[r.size() - 1]);
  }
  // Iterator
  int ans = 1;
  for (auto iter = tree.Begin(); iter != tree.End(); ++iter, ans += 2) {
    EXPECT_EQ(ans, (*iter).key.GetValue<int>()  );
    EXPECT_EQ(RowId(ans * 100), (*iter).value);
  }
}
