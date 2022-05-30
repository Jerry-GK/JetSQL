#ifndef MINISQL_TABLE_H
#define MINISQL_TABLE_H

#include <memory>

#include "glog/logging.h"
#include "record/schema.h"
#include "storage/table_heap.h"

class TableMetadata {
  friend class TableInfo;

 public:
  uint32_t SerializeTo(char *buf) const;

  uint32_t GetSerializedSize() const;

  static uint32_t DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap);

  static TableMetadata *Create(table_id_t table_id, std::string table_name, page_id_t root_page_id, uint32_t row_num, TableSchema *schema,
                               MemHeap *heap);

  inline table_id_t GetTableId() const { return table_id_; }

  inline std::string GetTableName() const { return table_name_; }

  inline uint32_t GetFirstPageId() const { return root_page_id_; }

  inline Schema *GetSchema() const { return schema_; }

 private:
  TableMetadata() = delete;
  ~TableMetadata(){
    schema_->~Schema();
  }

  TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, uint32_t row_num, TableSchema *schema);

 private:
  static constexpr uint32_t TABLE_METADATA_MAGIC_NUM = 344528;
  table_id_t table_id_;
  std::string table_name_;
  page_id_t root_page_id_;
  uint32_t row_num_;
  Schema *schema_;
};

/**
 * The TableInfo class maintains metadata about a table.
 */
class TableInfo {
 friend class CatalogManager;
 public:
  static TableInfo *Create(MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableInfo));
    return new (buf) TableInfo();
  }

  ~TableInfo() { 
    table_meta_->~TableMetadata();
    table_heap_->~TableHeap();
    delete heap_; 
  }

  void Init(TableMetadata *table_meta, TableHeap *table_heap) {
    table_meta_ = table_meta;
    table_heap_ = table_heap;
  }

  inline TableHeap *GetTableHeap() const { return table_heap_; }

  inline MemHeap *GetMemHeap() const { return heap_; }

  inline table_id_t GetTableId() const { return table_meta_->table_id_; }

  inline std::string GetTableName() const { return table_meta_->table_name_; }

  inline Schema *GetSchema() const { return table_meta_->schema_; }

  inline page_id_t GetRootPageId() const { return table_meta_->root_page_id_; }

  inline uint32_t GerRowNum() const {return table_meta_->row_num_; };

  inline void SetRowNum(uint32_t row_num) {table_meta_->row_num_ = row_num; }

 private:
  explicit TableInfo() : heap_(new UsedHeap()){};

 private:
  TableMetadata *table_meta_;
  TableHeap *table_heap_;
  MemHeap *heap_; /** store all objects allocated in table_meta and table heap */
};

#endif  // MINISQL_TABLE_H
