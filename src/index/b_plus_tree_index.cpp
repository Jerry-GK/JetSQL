#include "index/b_plus_tree_index.h"
#include <cstddef>
#include "index/generic_key.h"

BPlusTreeIndex::BPlusTreeIndex(index_id_t index_id, IndexSchema *key_schema, BufferPoolManager *buffer_pool_manager,IndexKeyComparator cmp)
    : Index(index_id, key_schema),container_(cmp) {
  uint32_t col_count = key_schema_->GetColumnCount();
  uint32_t byte_num = (col_count - 1) / 8 + 1;
  auto cols = key_schema_->GetColumns();
  uint32_t col_size = 0;
  for (auto it : cols) {
    col_size += it->GetLength();
  }
  uint32_t tot_size = byte_num + col_size;  // not good for char(128)
  int leaf_size = (PAGE_SIZE - BPlusTreeLeafPage::GetHeaderSize()) / (sizeof(BLeafEntry) + tot_size); 
  int internal_size = (PAGE_SIZE - BPlusTreeInternalPage::GetHeaderSize()) / (sizeof(BInternalEntry) + tot_size); 
  container_.Init(index_id, buffer_pool_manager, tot_size ,leaf_size ,internal_size);
  key_size_ = tot_size;
}

void BPlusTreeIndex::PrintTree() { container_.PrintTree(cout); }

dberr_t BPlusTreeIndex::InsertEntry(const Row &key, RowId row_id, Transaction *txn) {
  ASSERT(row_id.Get() != INVALID_ROWID.Get(), "Invalid row id for index insert.");
  IndexKey *index_key = IndexKey::SerializeFromKey(key, key_schema_, key_size_);
  bool status = container_.Insert(index_key, row_id, txn);
  delete index_key;
  if (!status) {
    return DB_FAILED;
  }

  return DB_SUCCESS;
}

dberr_t BPlusTreeIndex::RemoveEntry(const Row &key, RowId row_id, Transaction *txn) {
  IndexKey *index_key = IndexKey::SerializeFromKey(key, key_schema_, key_size_);
  container_.Remove(index_key, txn);
  delete index_key;
  return DB_SUCCESS;
}

dberr_t BPlusTreeIndex::ScanKey(const Row &key, vector<RowId> &result, Transaction *txn) {
  IndexKey *index_key = IndexKey::SerializeFromKey(key, key_schema_, key_size_);
  if (container_.GetValue(index_key, result, txn)) {
    delete index_key;
    return DB_SUCCESS;
  }
  delete index_key;
  return DB_KEY_NOT_FOUND;
}

dberr_t BPlusTreeIndex::Destroy() {
  container_.Destroy();
  return DB_SUCCESS;
}

BPlusTreeIndexIterator BPlusTreeIndex::GetBeginIterator() { return container_.Begin(); }

BPlusTreeIndexIterator BPlusTreeIndex::GetBeginIterator(const IndexKey &key) { return container_.Begin(&key); }

BPlusTreeIndexIterator BPlusTreeIndex::FindLastSmaller(const IndexKey &key) { return container_.FindLastSmaller(&key); }

BPlusTreeIndexIterator BPlusTreeIndex::GetEndIterator() { return container_.End(); }
