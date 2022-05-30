#ifndef MINISQL_B_PLUS_TREE_H
#define MINISQL_B_PLUS_TREE_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <queue>
#include <string>
#include <vector>

#include "common/config.h"
#include "index/index_iterator.h"
#include "index/index_key.h"
#include "page/b_plus_tree_page.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "record/row.h"
#include "transaction/transaction.h"

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */

class BPlusTree {
  using InternalPage = BPlusTreeInternalPage;
  using LeafPage = BPlusTreeLeafPage;
  using BIndexIterator = BPlusTreeIndexIterator;
  using KeyComparator = IndexKeyComparator;
  friend class BPlusTreeIndexIterator;
  friend class BPlusTreeIndex;

 public:
  ~BPlusTree(){
  }
  BPlusTree(KeyComparator cmp) : comparator_(cmp){}
  explicit BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, KeyComparator cmp,size_t keysize , size_t leaf_max_size_ , size_t internal_max_size_);

  void Init(index_id_t index_id, BufferPoolManager *buffer_pool_manager, size_t keysize , size_t leaf_max_size_ , size_t internal_max_size_);
  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const IndexKey *key, const RowId &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const IndexKey *key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const IndexKey *key, std::vector<RowId> &result, Transaction *transaction = nullptr);

  void StartNewTree(const IndexKey *key, const RowId &value);

  BPlusTreeIndexIterator Begin();

  BPlusTreeIndexIterator Begin(const IndexKey *key);

  BPlusTreeIndexIterator FindLastSmallerOrEqual(const IndexKey *key);

  BPlusTreeIndexIterator End();

  // expose for test purpose
  Page *FindLeafPage(const IndexKey &key, bool leftMost = false);

  // used to check whether all pages are unpinned
  bool Check();

  // destroy the b plus tree
  void Destroy();

  bool CheckIntergrity();

 public:
  void PrintTree(std::ostream &out);

 private:
  BPlusTreePage *InternalInsert(BPlusTreePage *destination, const IndexKey *key, const RowId &value, IndexKey **newkey,
                                bool *found, bool *modified);

  int InternalRemove(BPlusTreePage *destination, const IndexKey *key, IndexKey **newKey, bool *modified);

  void InternalDestory(page_id_t page);
  // useless function

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  index_id_t index_id_;
  page_id_t root_page_id_;
  KeyComparator comparator_;
  BufferPoolManager *buffer_pool_manager_;
  int key_size_;
  int leaf_max_size_;
  int internal_max_size_;
};

#endif  // MINISQL_B_PLUS_TREE_H
