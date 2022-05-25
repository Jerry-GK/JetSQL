#include "catalog/indexes.h"
#include <cstddef>
#include <cstdint>

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map, MemHeap *heap) {
  void *buf = heap->Allocate(sizeof(IndexMetadata));
  return new (buf) IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  uint32_t *ibuf = reinterpret_cast<uint32_t *>(buf);
  *(ibuf++) = INDEX_METADATA_MAGIC_NUM;
  *(ibuf++) = index_id_;
  char *cbuf = reinterpret_cast<char *>(ibuf);
  for (size_t i = 0; i < index_name_.size(); i++) *(cbuf++) = index_name_[i];
  *(cbuf++) = 0;
  ibuf = reinterpret_cast<uint32_t *>(cbuf);
  *(ibuf++) = table_id_;
  *(ibuf++) = key_map_.size();
  for (auto it = key_map_.begin(); it != key_map_.end(); it++) *(ibuf++) = *it;
  cbuf = reinterpret_cast<char *>(ibuf);
  return cbuf - buf;
}

uint32_t IndexMetadata::GetSerializedSize() const {
  uint32_t sz_magic = sizeof(INDEX_METADATA_MAGIC_NUM);
  uint32_t sz_indexid = sizeof(index_id_);
  uint32_t sz_name = index_name_.length() + 1;
  uint32_t sz_tableid = sizeof(table_id_);
  uint32_t sz_keymap = sizeof(uint32_t) * key_map_.size();
  return sz_magic + sz_indexid + sz_name + sz_tableid + 1 + sz_keymap;
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta, MemHeap *heap) {
  uint32_t *ibuf = reinterpret_cast<uint32_t *>(buf);
  if (*(ibuf++) != INDEX_METADATA_MAGIC_NUM) return 0;
  // this is a valid index meta data
  index_id_t index_id_ = *(ibuf++);
  char *cbuf = reinterpret_cast<char *>(ibuf);
  string index_name_(cbuf);
  ibuf = reinterpret_cast<uint32_t *>(cbuf + index_name_.size() + 1);
  table_id_t table_id_ = *(ibuf++);
  size_t n_keys = *(ibuf++);
  vector<uint32_t> key_map_;
  for (size_t i = 0; i < n_keys; i++) key_map_.push_back(*(ibuf++));
  cbuf = reinterpret_cast<char *>(ibuf);
  index_meta = Create(index_id_, index_name_, table_id_, key_map_, heap);
  return cbuf - buf;
}