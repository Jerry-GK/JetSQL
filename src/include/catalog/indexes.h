#ifndef MINISQL_INDEXES_H
#define MINISQL_INDEXES_H

#include <cstdint>
#include <memory>

#include "catalog/table.h"
#include "index/generic_key.h"
#include "index/b_plus_tree_index.h"
#include "record/schema.h"

#define BINDEX_TYPE(i) BPlusTreeIndex<GenericKey<i>,RowId,GenericComparator<i>>

class IndexMetadata {
  friend class IndexInfo;

public:
  static IndexMetadata *Create(const index_id_t index_id, const std::string &index_name,
                               const table_id_t table_id, const std::vector<uint32_t> &key_map,
                               MemHeap *heap);

  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap);

  inline std::string GetIndexName() const { return index_name_; }

  inline table_id_t GetTableId() const { return table_id_; }

  uint32_t GetIndexColumnCount() const { return key_map_.size(); }

  inline const std::vector<uint32_t> &GetKeyMapping() const { return key_map_; }

  inline index_id_t GetIndexId() const { return index_id_; }

private:
  IndexMetadata() = delete;

  explicit IndexMetadata(const index_id_t index_id, const std::string &index_name,
                         const table_id_t table_id, const std::vector<uint32_t> &key_map):
                         index_id_(index_id),index_name_(index_name),table_id_(table_id),key_map_(key_map)
                          {}

private:
  static constexpr uint32_t INDEX_METADATA_MAGIC_NUM = 344528;
  index_id_t index_id_;
  std::string index_name_;
  table_id_t table_id_;
  std::vector<uint32_t> key_map_;  /** The mapping of index key to tuple key */
};

/**
 * The IndexInfo class maintains metadata about a index.
 */
class IndexInfo {
public:
  static IndexInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(IndexInfo));
    return new(buf)IndexInfo();
  }

  ~IndexInfo() {
    delete heap_;
  }

  void Init(IndexMetadata *meta_data, TableInfo *table_info, BufferPoolManager *buffer_pool_manager) {
    // Step1: init index metadata and table info
    meta_data_ = meta_data;
    table_info_ = table_info;
    // Step2: mapping index key to key schema
    key_schema_ = Schema::ShallowCopySchema(table_info->GetSchema(), meta_data->GetKeyMapping(), heap_);
    // Step3: call CreateIndex to create the index
    this->index_ = CreateIndex(buffer_pool_manager);
  }

  inline Index *GetIndex() { return index_; }

  inline std::string GetIndexName() { return meta_data_->GetIndexName(); }

  inline IndexSchema *GetIndexKeySchema() { return key_schema_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline TableInfo *GetTableInfo() const { return table_info_; }

private:
  explicit IndexInfo() : meta_data_{nullptr}, index_{nullptr}, table_info_{nullptr},
                         key_schema_{nullptr}, heap_(new SimpleMemHeap()) {}

  Index *CreateIndex(BufferPoolManager *buffer_pool_manager) {
    // Page *p = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
    uint32_t col_count = key_schema_->GetColumnCount();
    uint32_t byte_num = (col_count - 1) / 8 + 1;
    auto cols = key_schema_->GetColumns();
    uint32_t col_size = 0;
    for(auto it : cols){
      col_size += it->GetLength();
    }
    Index * idx;
    uint32_t tot_size = byte_num + col_size;
    if(tot_size <= 4) idx = ALLOC_P(heap_,BINDEX_TYPE(4))(meta_data_->index_id_,key_schema_,buffer_pool_manager);
    else if(tot_size <= 8) idx = ALLOC_P(heap_,BINDEX_TYPE(8))(meta_data_->index_id_,key_schema_,buffer_pool_manager);
    else if(tot_size <= 16) idx = ALLOC_P(heap_,BINDEX_TYPE(16))(meta_data_->index_id_,key_schema_,buffer_pool_manager);
    else if(tot_size <= 32) idx = ALLOC_P(heap_,BINDEX_TYPE(32))(meta_data_->index_id_,key_schema_,buffer_pool_manager);
    else if(tot_size <= 64) idx = ALLOC_P(heap_,BINDEX_TYPE(64))(meta_data_->index_id_,key_schema_,buffer_pool_manager);
    else if(tot_size <= 128) idx = ALLOC_P(heap_,BINDEX_TYPE(128))(meta_data_->index_id_,key_schema_,buffer_pool_manager);
    else{
      ASSERT(0,"Column size too large to create index on !");
    }
    return idx;
  }

private:
  IndexMetadata *meta_data_;
  Index *index_;
  TableInfo *table_info_;
  IndexSchema *key_schema_;
  MemHeap *heap_;
};

#endif //MINISQL_INDEXES_H
