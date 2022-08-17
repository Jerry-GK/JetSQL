#include "index/hash_index.h"

HashIndex::HashIndex(index_id_t index_id, IndexSchema *key_schema, BufferPoolManager *buffer_pool_manager, IndexKeyComparator cmp)
        : Index(index_id, key_schema), container_(buffer_pool_manager, cmp, hash_func_)
{
        //get the key size
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
        key_size_ = tot_size;

        //init
        container_.Init(key_size_);
        index_type_ = HASH;
}

HashIndex::~HashIndex() 
{
        if(serialize_buffer_)delete [] serialize_buffer_;
}

void HashIndex::AdjustBufferFor(const Row& row){
  size_t rowsize = row.GetSerializedSize(this->key_schema_);
  key_size_t keysize = sizeof(IndexKey) + rowsize;
  if(keysize > buffer_size_){
    if(serialize_buffer_)delete[] serialize_buffer_;
    serialize_buffer_ = new char[2 * keysize];
    buffer_size_ = keysize * 2;
  }
}

dberr_t HashIndex::InsertEntry(const Row &key, RowId row_id, Transaction *txn)
{
        ASSERT(row_id.Get() != INVALID_ROWID.Get(), "Invalid row id for index insert.");
        AdjustBufferFor(key);
        //generate the index key
        IndexKey *index_key = IndexKey::SerializeFromKey(serialize_buffer_,key, key_schema_, key_size_);

        bool status = container_.Insert(index_key, row_id, txn);
        if (!status) {
        return DB_FAILED;
        }
        return DB_SUCCESS;
}

dberr_t HashIndex::RemoveEntry(const Row &key, RowId row_id, Transaction *txn)
{
        AdjustBufferFor(key);
        IndexKey *index_key = IndexKey::SerializeFromKey(serialize_buffer_,key, key_schema_, key_size_);
        container_.Remove(index_key, row_id, txn);
        return DB_SUCCESS;
}

dberr_t HashIndex::ScanKey(const Row &key, std::vector<RowId> &result, Transaction *txn)
{
        AdjustBufferFor(key);
        IndexKey *index_key = IndexKey::SerializeFromKey(serialize_buffer_,key, key_schema_, key_size_);
        if (container_.GetValue(index_key, result, txn)) {
        return DB_SUCCESS;
        }
        return DB_KEY_NOT_FOUND;
}

dberr_t HashIndex::Destroy() 
{
        return DB_SUCCESS;
}