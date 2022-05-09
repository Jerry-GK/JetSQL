#include "record/schema.h"

uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t ofs=0;
  char *buf_head = buf;
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;

  uint32_t len = columns_.size();
  MACH_WRITE_UINT32(buf, len);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;

  for (std::vector<Column *>::const_iterator it = columns_.begin(); it != columns_.end(); it++) {
    if(*it!=NULL)
    {
      (*it)->SerializeTo(buf);
      ofs += (*it)->GetSerializedSize();
      buf = buf_head + ofs;
    }
  }
  return ofs;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t ofs=0;
  ofs += sizeof(uint32_t);
  ofs += sizeof(uint32_t);
  for (std::vector<Column *>::const_iterator it = columns_.begin(); it != columns_.end(); it++) {
    if(*it!=NULL)
    {
      ofs += (*it)->GetSerializedSize();
    }
  }
  return ofs;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema, MemHeap *heap) {
  // replace with your code here
  uint32_t ofs=0;
  char *buf_head = buf;
  uint32_t mag_num = MACH_READ_UINT32(buf);
  buf = buf + mag_num * 0;
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;
  uint32_t len = MACH_READ_UINT32(buf);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;

  /* deserialize field from buf */
  std::vector<Column *> cols;
  while (len--) {
    cols.push_back(nullptr);
    ofs += Column::DeserializeFrom(buf, cols.back(), heap);
    buf = buf_head + ofs;
  }

  schema = ALLOC_P(heap, Schema)(cols);

  return ofs;
}