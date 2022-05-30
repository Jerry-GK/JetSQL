#include "index/b_plus_tree_index.h"
#include <cstddef>
#include <cstring>
#include "index/generic_key.h"
#include "index/index_iterator.h"
#include "record/type_id.h"

BPlusTreeIndex::BPlusTreeIndex(index_id_t index_id, IndexSchema *key_schema, BufferPoolManager *buffer_pool_manager,IndexKeyComparator cmp)
    : Index(index_id, key_schema),container_(cmp) {
  uint32_t col_count = key_schema_->GetColumnCount();
  uint32_t byte_num = (col_count - 1) / 8 + 1;
  auto cols = key_schema_->GetColumns();
  uint32_t col_size = 0;
  for (auto it : cols) {
    col_size += it->GetLength();
    if(it->GetType() == kTypeChar)col_size += 1;
  }
  uint32_t tot_size = byte_num + col_size;  // not good for char(128)
  serialize_buffer_ = nullptr;
  buffer_size_ = tot_size + 1;
  if(serialize_buffer_)delete [] serialize_buffer_;
  serialize_buffer_ = new char[buffer_size_];
  memset(serialize_buffer_,0,buffer_size_);
  int leaf_size = (PAGE_SIZE - BPlusTreeLeafPage::GetHeaderSize()) / (sizeof(BLeafEntry) + tot_size); 
  int internal_size = (PAGE_SIZE - BPlusTreeInternalPage::GetHeaderSize()) / (sizeof(BInternalEntry) + tot_size); 
  container_.Init(index_id, buffer_pool_manager, tot_size ,leaf_size ,internal_size);
  key_size_ = tot_size;
}

BPlusTreeIndex::~BPlusTreeIndex(){
  if(serialize_buffer_)delete [] serialize_buffer_;
}

void BPlusTreeIndex::AdjustBufferFor(const Row& row){
  size_t rowsize = row.GetSerializedSize(this->key_schema_);
  size_t keysize = sizeof(IndexKey) + rowsize;
  if(keysize > buffer_size_){
    if(serialize_buffer_)delete[] serialize_buffer_;
    serialize_buffer_ = new char[2 * keysize];
    buffer_size_ = keysize * 2;
  }
}

void BPlusTreeIndex::PrintTree() { container_.PrintTree(cout); }

dberr_t BPlusTreeIndex::InsertEntry(const Row &key, RowId row_id, Transaction *txn) {
  ASSERT(row_id.Get() != INVALID_ROWID.Get(), "Invalid row id for index insert.");
  AdjustBufferFor(key);
  IndexKey *index_key = IndexKey::SerializeFromKey(serialize_buffer_,key, key_schema_, key_size_);
  bool status = container_.Insert(index_key, row_id, txn);
  if (!status) {
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t BPlusTreeIndex::RemoveEntry(const Row &key, RowId row_id, Transaction *txn) {
  AdjustBufferFor(key);
  IndexKey *index_key = IndexKey::SerializeFromKey(serialize_buffer_,key, key_schema_, key_size_);
  container_.Remove(index_key, txn);
  return DB_SUCCESS;
}

dberr_t BPlusTreeIndex::ScanKey(const Row &key, vector<RowId> &result, Transaction *txn) {
  AdjustBufferFor(key);
  IndexKey *index_key = IndexKey::SerializeFromKey(serialize_buffer_,key, key_schema_, key_size_);
  if (container_.GetValue(index_key, result, txn)) {
    return DB_SUCCESS;
  }
  return DB_KEY_NOT_FOUND;
}

dberr_t BPlusTreeIndex::Destroy() {
  container_.Destroy();
  return DB_SUCCESS;
}

BPlusTreeIndexIterator BPlusTreeIndex::GetBeginIterator() { return container_.Begin(key_schema_); }

BPlusTreeIndexIterator BPlusTreeIndex::GetBeginIterator(const Row &key) {
  AdjustBufferFor(key);
  IndexKey *index_key = IndexKey::SerializeFromKey(serialize_buffer_,key, key_schema_, key_size_);
  return container_.Begin(index_key,key_schema_); 
}

BPlusTreeIndexIterator BPlusTreeIndex::FindLastSmallerOrEqual(const Row &key) {
  AdjustBufferFor(key);
  IndexKey *index_key = IndexKey::SerializeFromKey(serialize_buffer_,key, key_schema_, key_size_);
  return container_.FindLastSmallerOrEqual(index_key,key_schema_); 
}

BPlusTreeIndexIterator BPlusTreeIndex::GetEndIterator() { return container_.End(); }
