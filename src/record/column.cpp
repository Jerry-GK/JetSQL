#include "record/column.h"
Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), table_ind_(index),
          nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt :
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat :
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
        : name_(std::move(column_name)), type_(type), len_(length),
          table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other) : name_(other->name_), type_(other->type_), len_(other->len_),
                                      table_ind_(other->table_ind_), nullable_(other->nullable_),
                                      unique_(other->unique_) {}

uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  char *buf_head = buf;
  uint32_t ofs=0;
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;
  //must add str_len into buffer first to record the length!
  uint32_t str_len = name_.length();
  MACH_WRITE_UINT32(buf, str_len);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;
  MACH_WRITE_STRING(buf, name_);
  ofs += MACH_STR_SERIALIZED_SIZE(name_);
  buf = buf_head + ofs;

  MACH_WRITE_UINT32(buf, len_);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;
  MACH_WRITE_TO(TypeId, buf, type_);
  ofs += sizeof(TypeId);
  buf = buf_head + ofs;
  MACH_WRITE_UINT32(buf, table_ind_);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;
  MACH_WRITE_TO(bool, buf, nullable_);
  ofs += sizeof(bool);
  buf = buf_head + ofs;
  MACH_WRITE_TO(bool, buf, unique_);
  ofs += sizeof(bool);
  buf = buf_head + ofs;

  return ofs;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  uint32_t ofs=0;
  ofs += sizeof(uint32_t);
  ofs += sizeof(uint32_t);
  ofs += MACH_STR_SERIALIZED_SIZE(name_);
  ofs += sizeof(uint32_t);
  ofs += sizeof(TypeId);
  ofs += sizeof(uint32_t);
  ofs += sizeof(bool);
  ofs += sizeof(bool);
  return ofs;;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
  /* deserialize field from buf */
  uint32_t ofs = 0;
  char *buf_head = buf;
  uint32_t mag_num = MACH_READ_UINT32(buf);
  buf_head = buf_head + mag_num * 0;//avoid werror
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;
  // must get strlen from buffer first!
  uint32_t str_len = MACH_READ_UINT32(buf);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;

  std::string column_name;
  while (str_len--) 
  {
    column_name.push_back(MACH_READ_FROM(char, buf));
    ofs += 1;
    buf = buf_head + ofs;
  }
  ofs += 4;//end character
  buf = buf_head + ofs;

  uint32_t length=MACH_READ_UINT32(buf);
  ofs+=sizeof(uint32_t);
  buf = buf_head + ofs;
  TypeId type = MACH_READ_FROM(TypeId, buf);
  ofs += sizeof(TypeId);
  buf = buf_head + ofs;
  uint32_t col_ind = MACH_READ_UINT32(buf);
  ofs += sizeof(uint32_t);
  buf = buf_head + ofs;
  
  bool nullable = MACH_READ_FROM(bool, buf);
  ofs += sizeof(bool);
  buf = buf_head + ofs;
  bool unique = MACH_READ_FROM(bool, buf);
  ofs += sizeof(bool);
  buf = buf_head + ofs;

  if (type != TypeId::kTypeChar)
    column = ALLOC_P(heap, Column)(column_name, type, col_ind, nullable, unique);
  else
    column = ALLOC_P(heap, Column)(column_name, type, length, col_ind, nullable, unique);
  return ofs;
}
