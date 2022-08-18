#ifndef MINISQL_EXTENDIBLE_HASH_TABLE_H
#define MINISQL_EXTENDIBLE_HASH_TABLE_H

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/config.h"
#include "index/index_key.h"
#include "index/hash_function.h"
#include "page/hash_table_bucket_page.h"
#include "page/hash_table_directory_page.h"
#include "record/row.h"
#include "transaction/transaction.h"
#include "buffer/buffer_pool_manager.h"

using namespace std;

class ExtendibleHashTable {
public:
  using KeyComparator = IndexKeyComparator;

  explicit ExtendibleHashTable(BufferPoolManager *buffer_pool_manager, KeyComparator cmp, HashFunction hash_func);

  void Init(const key_size_t keysize);

  bool Insert(const IndexKey *key, const RowId value, Transaction *transaction = nullptr);

  bool Remove(const IndexKey *key, const RowId value, Transaction *transaction = nullptr);
  
  bool GetValue(const IndexKey *key, vector<RowId>& result, Transaction *transaction = nullptr);

  void Merge(const IndexKey *key, const RowId value, Transaction *transaction = nullptr);

  bool SplitInsert(const IndexKey *key, const RowId value, Transaction *transaction = nullptr);

  uint32_t GetGlobalDepth();

  inline uint32_t Hash(const IndexKey *key);

  //integerity vertification for debug
  bool VerifyIntegrity();

  //key --<hash function>--> hash(key) --<global mask>--> key index --<directory>--> bucket pid 
  inline uint32_t KeyToDirectoryIndex(const IndexKey *key, HashTableDirectoryPage *dir_page);

  inline page_id_t KeyToPageId(const IndexKey *key, HashTableDirectoryPage *dir_page);

  uint32_t Pow(uint32_t base, uint32_t power) const;

private:
  page_id_t directory_page_id_;
  KeyComparator comparator_;
  BufferPoolManager *buffer_pool_manager_;
  key_size_t key_size_;
  HashFunction hash_func_;
};

#endif  // MINISQL_EXTENDIBLE_HASH_TABLE_H
