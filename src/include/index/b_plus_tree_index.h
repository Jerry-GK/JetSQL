#ifndef MINISQL_B_PLUS_TREE_INDEX_H
#define MINISQL_B_PLUS_TREE_INDEX_H

#include <cstddef>
#include "common/rowid.h"
#include "index/b_plus_tree.h"
#include "index/index.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_leaf_page.h"

using IndexEntry = BLeafEntry;

class BPlusTreeIndex : public Index {
public:
  BPlusTreeIndex(index_id_t index_id, IndexSchema *key_schema, BufferPoolManager *buffer_pool_manager , IndexKeyComparator cmp);

  dberr_t InsertEntry(const Row &key, RowId row_id, Transaction *txn) override;

  dberr_t RemoveEntry(const Row &key, RowId row_id, Transaction *txn) override;

  dberr_t ScanKey(const Row &key, std::vector<RowId> &result, Transaction *txn) override;

  ~BPlusTreeIndex();

  dberr_t Destroy() override;

  void PrintTree();
  BPlusTreeIndexIterator GetBeginIterator();

  BPlusTreeIndexIterator GetBeginIterator(const Row &key);

  BPlusTreeIndexIterator FindLastSmallerOrEqual(const Row &key);

  BPlusTreeIndexIterator GetEndIterator();

 protected:

  void AdjustBufferFor(const Row& row);
  // comparator for key
  // container
  BPlusTree container_;
  key_size_t key_size_;
  char * serialize_buffer_;
  size_t buffer_size_;
};

#endif  // MINISQL_B_PLUS_TREE_INDEX_H
