#ifndef MINISQL_HASH_INDEX_H
#define MINISQL_HASH_INDEX_H

#include <cstddef>
#include "common/rowid.h"
#include "index/index_key.h"
#include "buffer/buffer_pool_manager.h"
#include "index/index.h"
#include "index/extendible_hash_table.h"

class HashIndex : public Index {
public:
  HashIndex(index_id_t index_id, IndexSchema *key_schema, BufferPoolManager *buffer_pool_manager , IndexKeyComparator cmp);

  dberr_t InsertEntry(const Row &key, RowId row_id, Transaction *txn) override;

  dberr_t RemoveEntry(const Row &key, RowId row_id, Transaction *txn) override;

  dberr_t ScanKey(const Row &key, std::vector<RowId> &result, Transaction *txn) override;

  ~HashIndex();

  dberr_t Destroy() override;

protected:
  HashFunction hash_func_;
  void AdjustBufferFor(const Row& row);
  ExtendibleHashTable container_;
  key_size_t key_size_;
  char * serialize_buffer_;
  size_t buffer_size_;
};

#endif  // MINISQL_HASH_INDEX_H
