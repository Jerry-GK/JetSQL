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
  MACH_WRITE_UINT32(buf, COLUMN_MAGIC_NUM);
  MACH_WRITE_STRING(buf, name_);
  MACH_WRITE_TO(TypeId, buf, type_);
  MACH_WRITE_UINT32(buf, table_ind_);
  MACH_WRITE_TO(bool, buf, nullable_);
  MACH_WRITE_TO(bool, buf, unique_);

  uint32_t ofs=0;
  ofs += sizeof(uint32_t);
  ofs += MACH_STR_SERIALIZED_SIZE(this->name_);
  ofs += sizeof(TypeId);
  ofs += sizeof(uint32_t);
  ofs += sizeof(bool);
  ofs += sizeof(bool);
  return ofs;
}

uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  uint32_t ofs=0;
  ofs += sizeof(uint32_t);
  ofs += MACH_STR_SERIALIZED_SIZE(this->name_);
  ofs += sizeof(TypeId);
  ofs += sizeof(uint32_t);
  ofs += sizeof(bool);
  ofs += sizeof(bool);
  return ofs;;
}

uint32_t Column::DeserializeFrom(char *buf, Column *&column, MemHeap *heap) {
  // replace with your code here
  ASSERT ( column!=nullptr, "Pointer to column is not null in column deserialize." );

  uint32_t mag_num = MACH_READ_UINT32(buf);
  buf = buf + mag_num * 0;
  /* deserialize field from buf */
  std::string column_name = MACH_READ_FROM(std::string, buf);
  TypeId type = MACH_READ_FROM(TypeId, buf);
  uint32_t col_ind = MACH_READ_UINT32(buf);
  bool nullable = MACH_READ_FROM(bool, buf);
  bool unique = MACH_READ_FROM(bool, buf);

  column=ALLOC_P(heap,Column)(column_name, type, col_ind, nullable, unique);

  uint32_t ofs = 0;
  ofs += sizeof(uint32_t);
  ofs += MACH_STR_SERIALIZED_SIZE(column->name_);
  ofs += sizeof(TypeId);
  ofs += sizeof(uint32_t);
  ofs += sizeof(bool);
  ofs += sizeof(bool);
  return ofs;
}
