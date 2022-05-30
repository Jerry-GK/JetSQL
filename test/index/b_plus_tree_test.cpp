#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "utils/tree_file_mgr.h"
#include "utils/utils.h"

#include <cstdint>
#include <iostream>
using namespace std;
static const std::string db_name = "bp_tree_insert_test.db";

TEST(BPlusTreeTests, SampleTest) {
  // Init engine
  DBStorageEngine engine(db_name);
  int key_size = 4;
  int leaf_size = (PAGE_SIZE - BPlusTreeLeafPage::GetHeaderSize()) / (sizeof(BLeafEntry) + key_size) ; 
  int internal_size = (PAGE_SIZE - BPlusTreeInternalPage::GetHeaderSize()) / (sizeof(BInternalEntry) + key_size); 
  // int leaf_size = 20;
  // int internal_size = 40;
  cout << "Leaf size = " << leaf_size << endl;
  cout << "Internal size = " << internal_size << endl;
  // no catalog , treat as integer
  IndexKeyComparator cmp(nullptr);
  BPlusTree tree(0, engine.bpm_, cmp,key_size,leaf_size,internal_size);
  TreeFileManagers mgr("tree_");
  // Prepare data
  const int n = 100000;
  vector<uint32_t> keys;
  vector<RowId> values;
  vector<uint32_t> delete_seq;
  map<uint32_t, RowId> kv_map;
  for (int i = 0; i < n; i++) {
    keys.push_back(i);
    values.push_back(RowId(i));
    delete_seq.push_back(i);
  }
  // Shuffle data
  ShuffleArray(keys);
  ShuffleArray(values);
  ShuffleArray(delete_seq);
  // Map key value
  for (int i = 0; i < n; i++) {
    kv_map[keys[i]] = values[i];
  }
  int k = 5;
  IndexKey * temp = IndexKey::Create(key_size,k);
  for (int i = 0; i < n; i++) {
    temp->SetValue(keys[i]);
    // cout << dec << i << " :Inserting " << temp << endl;
    tree.Insert(temp, values[i]);
    // tree.PrintTree(cout);
    ASSERT_TRUE(tree.Check());
  }
  // tree.PrintTree(cout);
  ASSERT_TRUE(tree.Check());
  cout << "Insert passed." << endl;
  engine.bpm_->get_hit_rate();
  engine.bpm_->ResetCounter();
  vector<RowId> ans;
  for (int i = 0; i < n; i++) {
    temp->SetValue(i);
    tree.GetValue(temp, ans);
    ASSERT_EQ(kv_map[i], ans[i]);
  }
  cout << "GetValue check passed." << endl;
  engine.bpm_->get_hit_rate();
  engine.bpm_->ResetCounter();
  ASSERT_TRUE(tree.Check());
  // Delete half keys
  for (int i = 0; i < n / 2; i++) {
    temp->SetValue(delete_seq[i]);
    // cout << "Removing " << delete_seq[i] << endl;
    tree.Remove(temp);
    // tree.PrintTree(cout);
    ASSERT_TRUE(tree.Check());
  }
  // tree.PrintTree(cout);
  // Check valid
  cout << "Remove finished." << endl;
  engine.bpm_->get_hit_rate();
  engine.bpm_->ResetCounter();
  ans.clear();
  for (int i = 0; i < n / 2; i++) {
    temp->SetValue(delete_seq[i]);
    ASSERT_FALSE(tree.GetValue(temp, ans));
  }
  for (int i = n / 2; i < n; i++) {
    temp->SetValue(delete_seq[i]);
    ASSERT_TRUE(tree.GetValue(temp, ans));
    ASSERT_EQ(kv_map[delete_seq[i]], ans[ans.size() - 1]);
  }
  cout << "Getvalue after remove finished." << endl;
  engine.bpm_->get_hit_rate();
  engine.bpm_->ResetCounter();
  ASSERT_TRUE(tree.Check());
}