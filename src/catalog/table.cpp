#include "catalog/table.h"
#include <cstdint>

uint32_t TableMetadata::SerializeTo(char *buf) const {
  uint32_t *ibuf = reinterpret_cast<uint32_t *>(buf);
  *(ibuf++) = TABLE_METADATA_MAGIC_NUM;
  *(ibuf++) = table_id_;
  char *cbuf = reinterpret_cast<char *>(ibuf);
  for (size_t i = 0; i < table_name_.size(); i++) *(cbuf++) = table_name_[i];
  *(cbuf++) = 0;
  ibuf = reinterpret_cast<uint32_t *>(cbuf);
  *(ibuf++) = root_page_id_;

  uint32_t* rbuf = reinterpret_cast<uint32_t *>(ibuf);
  *(rbuf++) = row_num_;

  uint32_t sz_sch = schema_->SerializeTo(reinterpret_cast<char *>(ibuf));
  return sz_sch + reinterpret_cast<char *>(ibuf) - buf;
}

uint32_t TableMetadata::GetSerializedSize() const {
  return sizeof(TABLE_METADATA_MAGIC_NUM) + sizeof(table_id_) + table_name_.size() + 1 + sizeof(root_page_id_) + sizeof(row_num_) +
         schema_->GetSerializedSize();
}

/**
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta, MemHeap *heap) {
  uint32_t *ibuf = reinterpret_cast<uint32_t *>(buf);
  if (*(ibuf++) != TABLE_METADATA_MAGIC_NUM) return 0;
  // else this is a valid table meta page

  table_id_t table_id_ = *(ibuf++);
  char *cbuf = reinterpret_cast<char *>(ibuf);
  string table_name_ = string(cbuf);
  cbuf += table_name_.size() + 1;
  ibuf = reinterpret_cast<uint32_t *>(cbuf);
  page_id_t root_page_id_ = *(ibuf++);

  uint32_t row_num_ = *(ibuf++);

  Schema *scm_;
  uint32_t sz_scm = Schema::DeserializeFrom(reinterpret_cast<char *>(ibuf),scm_, heap);
  table_meta = Create(table_id_, table_name_, root_page_id_, row_num_, scm_, heap);
  return sz_scm + reinterpret_cast<char *>(ibuf) - buf;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id, uint32_t row_num,
                                     TableSchema *schema, MemHeap *heap) {
  // allocate space for table metadata
  void *buf = heap->Allocate(sizeof(TableMetadata));
  return new (buf) TableMetadata(table_id, table_name, root_page_id, row_num, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, uint32_t row_num, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), row_num_(row_num),schema_(schema) {
  // done ?
}
