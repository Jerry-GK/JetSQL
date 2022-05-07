#include "record/row.h"

uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  // replace with your code here
  uint32_t ofs=0;
  for (std::vector<Field *>::const_iterator it = fields_.begin(); it != fields_.end(); it++) {
    if(*it!=NULL)
    {
      (*it)->SerializeTo(buf);
      ofs += (*it)->GetSerializedSize();
    }
  }
  return ofs;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  // replace with your code here
  uint32_t ofs=0;
  for (std::vector<Field *>::const_iterator it = fields_.begin(); it != fields_.end(); it++) {
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
  /* deserialize field from buf */
  //fields_.clear();
  for(auto &col: schema->GetColumns())
  {
    fields_.push_back(nullptr);
    ofs += Field::DeserializeFrom(buf,col->GetType(),&fields_.back(),col->IsNullable(),heap_);
  }
  return ofs;
}
