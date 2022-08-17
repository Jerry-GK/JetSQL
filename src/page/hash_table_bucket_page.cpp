#include "page/hash_table_bucket_page.h"

void HashTableBucketPage::Init(key_size_t key_size)
{
  key_size_ = key_size;
  memset(slot_data_, 0, PAGE_SIZE - sizeof(key_size_t));
}

bool HashTableBucketPage::GetValue(const IndexKey* key, IndexKeyComparator cmp, std::vector<RowId> *result)
{
  for(uint32_t i=0; i<GetMaxSlotNum(); i++)
  {
    if(!IsReadable(i))
    {
      if(!IsOccupied(i))
        break;
      continue;
    }
    IndexKey* key_copy = IndexKey::Create(key->keysize); //avoid compile error
    memcpy(key_copy->value, key->value, key->keysize);
    IndexKey* key_target = IndexKey::Create(KeyAt(i)->keysize);
    memcpy(key_target->value, KeyAt(i)->value, KeyAt(i)->keysize);

    //cout<<"testing "<<i<<" key.size = "<<key->keysize<<" key target size = "<<key_target->keysize<<endl;
    if(cmp(key_copy, key_target)==0)
    {
      result->push_back(ValueAt(i));
    }
  }
  return !result->empty();
}

bool HashTableBucketPage::Insert(const IndexKey* key, RowId value, IndexKeyComparator cmp)
{
  if(IsFull())
  {
    cout<<"full"<<endl;
    return false;
  }
  std::vector<RowId> result;
  if(GetValue(key,cmp, &result)==true)//duplicate element in bucket
  {
    cout<<"dup"<<endl;
    return false;
  }

  for(uint32_t i=0; i<GetMaxSlotNum(); i++)
  {
    if(!IsReadable(i))
    {
      IndexKey* key_ptr = reinterpret_cast<IndexKey*>(slot_data_ + GetKeyOffset(i));
      RowId* rid_ptr = reinterpret_cast<RowId*>(slot_data_ + GetValueOffset(i));
      key_ptr->keysize = key->keysize;
      memcpy(key_ptr->value, key->value, key->keysize);
      *rid_ptr = value;
      
      SetOccupied(i, 1);
      SetReadable(i, 1);
      break;
    }
  }
  return true;
}

bool HashTableBucketPage::Remove(const IndexKey* key, RowId value, IndexKeyComparator cmp)
{
  for(uint32_t i=0; i<GetMaxSlotNum(); i++)
  {
    const IndexKey* key_copy = key; //avoid compile error
    const IndexKey* key_target = KeyAt(i);
    if(IsReadable(i) && cmp(key_copy, key_target)==0 && ValueAt(i)==value)
    {
      SetReadable(i, 0);
      return true;
    }
  }
  return false;
}

const IndexKey* HashTableBucketPage::KeyAt(uint32_t bucket_idx) const 
{
  const IndexKey* key = reinterpret_cast<const IndexKey*>(slot_data_ + GetKeyOffset(bucket_idx));
  return key;
}

RowId HashTableBucketPage::ValueAt(uint32_t bucket_idx) const 
{
  const RowId* rid = reinterpret_cast<const RowId*>(slot_data_ + GetValueOffset(bucket_idx));
  return *rid;
}

void HashTableBucketPage::RemoveAt(uint32_t bucket_idx) 
{
  SetOccupied(bucket_idx, 1);
  SetReadable(bucket_idx, 0);
}

bool HashTableBucketPage::IsOccupied(uint32_t bucket_idx) const 
{
  return (bool)*(slot_data_ + GetOccupiedOffset(bucket_idx));
}

void HashTableBucketPage::SetOccupied(uint32_t bucket_idx, int bit)
{
  bool* occupied = reinterpret_cast<bool*>(slot_data_ + GetOccupiedOffset(bucket_idx));
  *occupied = (bool)bit;
}

bool HashTableBucketPage::IsReadable(uint32_t bucket_idx) const 
{
  return (bool)*(slot_data_ + GetReadableOffset(bucket_idx));
}

void HashTableBucketPage::SetReadable(uint32_t bucket_idx, int bit)
{
  bool* readable = reinterpret_cast<bool*>(slot_data_ + GetReadableOffset(bucket_idx));
  *readable = (bool)bit;
}

uint32_t HashTableBucketPage::NumReadable()
{
  uint32_t readable_num = 0;
  for(uint32_t i=0;i<GetMaxSlotNum();i++)
  {
    if(IsReadable(i))
      readable_num++;
  }
  return readable_num;
}

bool HashTableBucketPage::IsFull()
{
  return NumReadable()==GetMaxSlotNum();
}

bool HashTableBucketPage::IsEmpty()
{
  return NumReadable()==0;
}

void HashTableBucketPage::PrintBucket()
{

}
