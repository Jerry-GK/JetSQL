#ifndef MINISQL_B_PLUS_TREE_INDEX_H
#define MINISQL_B_PLUS_TREE_INDEX_H

#include <cstddef>
#include "index/b_plus_tree.h"
#include "index/index.h"

#define BPLUSTREE_INDEX_TYPE BPlusTreeIndex<KeyType, ValueType, KeyComparator>//ValueType must be RowId here

INDEX_TEMPLATE_ARGUMENTS
class   BPlusTreeIndex : public Index {
public:
  BPlusTreeIndex(index_id_t index_id, IndexSchema *key_schema, BufferPoolManager *buffer_pool_manager);

  dberr_t InsertEntry(const Row &key, ValueType row_id, Transaction *txn) override;

  dberr_t RemoveEntry(const Row &key, ValueType row_id, Transaction *txn) override;

  dberr_t ScanKey(const Row &key, std::vector<ValueType> &result, Transaction *txn) override;

  dberr_t Destroy() override;

  void PrintTree() ;
  INDEXITERATOR_TYPE GetBeginIterator();

  INDEXITERATOR_TYPE GetBeginIterator(const KeyType &key);

  INDEXITERATOR_TYPE GetEndIterator();

protected:
  // comparator for key
  KeyComparator comparator_;
  // container
  BPLUSTREE_TYPE container_;
};

#endif //MINISQL_B_PLUS_TREE_INDEX_H
