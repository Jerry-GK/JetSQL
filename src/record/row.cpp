#include "record/row.h"
#include <cstddef>
#include <cstring>
#include <iostream>
#include "record/column.h"
#include "record/types.h"
int row_des_count = 0;

using namespace std;
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {//seg fault: buf is not avalable
  // replace with your code here
  ASSERT(schema->GetColumnCount() == field_count_, "Not equal length!");
  uint32_t ofs = 0;
  //generate Null bitmap
  char *buf_head = buf;
  uint32_t len = schema->GetColumnCount();

  uint32_t byte_num = (len - 1) / 8 + 1;
  // std::vector<char> bitmaps(byte_num, '\0');
  // for (std::vector<char>::iterator it = bitmaps.begin(); it != bitmaps.end();it++)
  // {
  //   MACH_WRITE_TO(char, buf, *it);//segmanetation fault
  //   ofs += sizeof(char);
  //   buf = buf_head + ofs;
  // }
  memset(buf,0,byte_num);
  ofs += byte_num;
  buf += ofs;
  for (uint32_t i = 0; i < field_count_; i++) {
    fields_[i].SerializeTo(buf);
    ofs += fields_[i].GetSerializedSize();
    buf = buf_head+ofs;
    if(fields_[i].IsNull())
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
  // auto ed = fields_.end();
  // for (std::vector<Field *>::const_iterator it = fields_.begin(); it != ed; it++) {
  //   if(*it!=NULL)
  //   {
  //     ofs += (*it)->GetSerializedSize();
  //   }
  // }
  for(size_t i =0;i<field_count_;i++){
    ofs += fields_[i].GetSerializedSize();
  }
  return ofs;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  //Deserialization during index comparasion cost a lot of time!
  ++row_des_count;

  // replace with your code here
  ASSERT ( heap_!=nullptr, "Pointer to row_heap_ is not null in row deserialize." );
  uint32_t ofs=0;
  char *buf_head = buf;
  /* deserialize field from buf */
  // ASSERT(fields_ == nullptr,"Deserializing to an existing filed object!");
  // ASSERT(fields_.empty(), "not empty field!");
  uint32_t len = schema->GetColumnCount();
  uint32_t byte_num = (len - 1) / 8 + 1;
  char * bitmaps = buf;
  // std::vector<char> bitmaps(byte_num, '\0');
  // //read bitmap
  // for(std::vector<char>::iterator it = bitmaps.begin(); it != bitmaps.end();it++)
  // {
  //   *it = MACH_READ_FROM(char, buf);
  //   ofs += sizeof(char);
  //   buf = buf_head + ofs;
  // }
  ofs += byte_num;
  buf = buf_head + ofs;
  uint32_t field_ind = 0;
  int col_num = schema->GetColumnCount();
  this->field_count_ = col_num;
  // fields_.resize(col_num,nullptr);
  auto &cols = schema->GetColumns();
  size_t cols_size = cols.size();
  if(fields_ != nullptr && heap_ != nullptr)heap_->Free(fields_);
  fields_ = reinterpret_cast<Field * >(heap_->Allocate(field_count_  * sizeof(Field)));
  for ( size_t i =0;i< cols_size;i++) {
    bool isNull = false;
    // get isNULL from bitmap
    auto &col = cols[i];
    isNull = (bool)(bitmaps[field_ind / 8] & (0x80 >> (field_ind % 8)));
    ofs += Field::DeserializeFrom(buf, fields_ +i,heap_,col->GetType(), isNull);
    buf = buf_head + ofs;
    field_ind++;
  }
  return ofs;
}

std::ostream& operator<<(std::ostream& os,RowId &r){
  os << r.page_id_ << " " << r.slot_num_ ;
  return os;
}