#include "record/row.h"
#include <iostream>
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {//seg fault: buf is not avalable
  // replace with your code here
  ASSERT(schema->GetColumnCount() == fields_.size(), "Not equal length!");
  uint32_t ofs = 0;
  //generate Null bitmap
  char *buf_head = buf;
  uint32_t len = schema->GetColumnCount();

  uint32_t byte_num = (len - 1) / 8 + 1;
  std::vector<char> bitmaps(byte_num, '\0');
  for (std::vector<char>::iterator it = bitmaps.begin(); it != bitmaps.end();it++)
  {
    MACH_WRITE_TO(char, buf, *it);//segmanetation fault
    ofs += sizeof(char);
    buf = buf_head + ofs;
  }
  for (uint32_t i = 0; i < fields_.size(); i++) {
    fields_[i]->SerializeTo(buf);
    ofs += fields_[i]->GetSerializedSize();
    buf = buf_head+ofs;
    if(fields_[i]->IsNull())
    {
      // set corresponding position in bitmap to 1, rewrite to buffer
      buf_head[i / 8] |= (0x80 >> (i % 8));
    }
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t ofs=0;
  uint32_t len = schema->GetColumnCount();
  uint32_t byte_num = (len - 1) / 8 + 1;
  ofs += byte_num * sizeof(char);
  auto ed = fields_.end();
  for (std::vector<Field *>::const_iterator it = fields_.begin(); it != ed; it++) {
    if(*it!=NULL)
    {
      ofs += (*it)->GetSerializedSize();
    }
  }
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  // replace with your code here
  ASSERT ( heap_!=nullptr, "Pointer to row_heap_ is not null in row deserialize." );
  uint32_t ofs=0;
  char *buf_head = buf;
  /* deserialize field from buf */
  fields_.clear();
  ASSERT(fields_.empty(), "not empty field!");
  uint32_t len = schema->GetColumnCount();
  uint32_t byte_num = (len - 1) / 8 + 1;
  std::vector<char> bitmaps(byte_num, '\0');
  //read bitmap
  for(std::vector<char>::iterator it = bitmaps.begin(); it != bitmaps.end();it++)
  {
    *it = MACH_READ_FROM(char, buf);
    ofs += sizeof(char);
    buf = buf_head + ofs;
  }
  uint32_t field_ind = 0;
  for (auto &col : schema->GetColumns()) {
    fields_.push_back(nullptr);
    bool isNull = false;
    // get isNULL from bitmap
  
    isNull = (bool)(bitmaps[field_ind / 8] & (0x80 >> (field_ind % 8)));
    ofs += Field::DeserializeFrom(buf, col->GetType(), &fields_.back(), isNull, heap_);
    buf = buf_head + ofs;
    field_ind++;
  }
  return ofs;
}

std::ostream& operator<<(std::ostream& os,RowId &r){
  os << r.page_id_ << " " << r.slot_num_ ;
  return os;
}