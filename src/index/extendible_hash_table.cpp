#include "index/extendible_hash_table.h"

ExtendibleHashTable::ExtendibleHashTable(BufferPoolManager *buffer_pool_manager, KeyComparator cmp, HashFunction hash_func)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(cmp),hash_func_(hash_func)
{

}

void ExtendibleHashTable::Init(const key_size_t key_size)
{
    key_size_ = key_size;

    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->NewPage(directory_page_id_)->GetData());

    //two buckets initially
    page_id_t bucket_pid_0;
    page_id_t bucket_pid_1;
    auto bucket_page_0 = reinterpret_cast<HashTableBucketPage *>(buffer_pool_manager_->NewPage(bucket_pid_0));
    auto bucket_page_1 = reinterpret_cast<HashTableBucketPage *>(buffer_pool_manager_->NewPage(bucket_pid_1));
    bucket_page_0->Init(key_size_);
    bucket_page_1->Init(key_size_);

    dir_page->SetBucketPageId(0, bucket_pid_0);
    dir_page->SetLocalDepth(0, 1);
    dir_page->SetBucketPageId(1, bucket_pid_1);
    dir_page->SetLocalDepth(1, 1);

    dir_page->IncrGlobalDepth();
    dir_page->SetPageId(directory_page_id_);

    buffer_pool_manager_->UnpinPage(bucket_pid_1, true);
    buffer_pool_manager_->UnpinPage(bucket_pid_0, true);
    buffer_pool_manager_->UnpinPage(directory_page_id_, true);
}

bool ExtendibleHashTable::Insert(const IndexKey *key, const RowId value, Transaction *transaction)
{
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
    
    page_id_t bucket_pid = KeyToPageId(key, dir_page);
    auto bucket_page = reinterpret_cast<HashTableBucketPage *>
        (buffer_pool_manager_->FetchPage(bucket_pid)->GetData());

    bool suc = false;
    if(bucket_page->IsFull())//split
    {
        buffer_pool_manager_->UnpinPage(bucket_pid, false);
        buffer_pool_manager_->UnpinPage(directory_page_id_, false);
        return SplitInsert(key, value, transaction);
    }
    else//insert directly
    {
        //cout<<"directly insert to bucket "<<KeyToDirectoryIndex(key, dir_page)<<endl;
        suc = bucket_page->Insert(key, value, comparator_);
    }
    buffer_pool_manager_->UnpinPage(bucket_pid, suc);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return suc;
}

bool ExtendibleHashTable::SplitInsert(const IndexKey *key, const RowId value, Transaction *transaction)
{
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
    bool success = false;
    bool grown = false;
    //cout<<"split insert bucket "<<KeyToDirectoryIndex(key, dir_page)<<endl;

    //loop to insert with possible split. terminate only when directly nsert into a non-full bucket
    while(true)
    {
        page_id_t bucket_pid = KeyToPageId(key, dir_page);
        uint32_t bucket_idx = KeyToDirectoryIndex(key, dir_page);
        auto bucket_page = reinterpret_cast<HashTableBucketPage *>
            (buffer_pool_manager_->FetchPage(bucket_pid)->GetData());
        ASSERT(bucket_page!=nullptr, "null page");

        if(bucket_page->IsFull())//bucket needs to spilt. 
        {
            //if full becket locak depth equals global depth, needs to grow
            if(dir_page->GetLocalDepth(bucket_idx) == dir_page->GetGlobalDepth())
            {
                //the size of bucket pointer array doubles
                dir_page->IncrGlobalDepth();
                grown = true;
                //cout<<"grown global depth = "<<dir_page->GetGlobalDepth()<<endl;
            }

        
            dir_page->IncrLocalDepth(bucket_idx);
            uint32_t split_bucket_idx = dir_page->GetSplitImageIndex(bucket_idx);
            //cout<<"split idx = "<<split_bucket_idx<<endl;

            page_id_t split_bucket_pid;// new page id for split
            HashTableBucketPage* split_page = nullptr;
            if(true)
            {
                split_page = reinterpret_cast<HashTableBucketPage *>
                    (buffer_pool_manager_->NewPage(split_bucket_pid));
                split_page->Init(key_size_);
            }
            else
            {
                split_bucket_pid = dir_page->GetBucketPageId(split_bucket_idx);
                split_page = reinterpret_cast<HashTableBucketPage *>
                    (buffer_pool_manager_->FetchPage(split_bucket_pid));
            }
            
            dir_page->SetBucketPageId(split_bucket_idx, split_bucket_pid);
            //the new split bucket has the same depth with the original bucket(after increase, 1 more than before split)
            dir_page->SetLocalDepth(split_bucket_idx, dir_page->GetLocalDepth(bucket_idx));

            //reallocate key-value pairs(slots) in the original bucket and its new split bucket
            uint32_t read_num = 0;
            const uint32_t readable_num = bucket_page->NumReadable();
            const int max_slot_num = bucket_page->GetMaxSlotNum();
            for(uint32_t i=0; i<max_slot_num; i++)
            {
                if(bucket_page->IsReadable(i))
                {
                    auto key = bucket_page->KeyAt(i);
                    uint32_t target_bucket_idx = Hash(key) & (Pow(2, dir_page->GetLocalDepth(bucket_idx)) - 1);
                    if((target_bucket_idx ^ split_bucket_idx) == 0)//need to transfer to the split page
                    {
                        // remove from the original bucket and insert the new split bucket
                        auto val = bucket_page->ValueAt(i);
                        split_page->Insert(key, val, comparator_);
                        bucket_page->RemoveAt(i);
                    }

                    read_num++;
                    if(read_num==readable_num)
                        break;
                }
            }
            buffer_pool_manager_->UnpinPage(split_bucket_pid, true);
            buffer_pool_manager_->UnpinPage(bucket_pid, true);

            //rearrange the new coming directory information if growns
            if(grown)
            {
                uint32_t old_global_depth = dir_page->GetLocalDepth(bucket_idx) - 1;
                for(uint32_t i = Pow(2, old_global_depth); i<dir_page->Size(); i++)
                {
                    if(i == split_bucket_idx)
                    {
                        continue;
                    }
                    uint32_t old_idx = i & Pow(2, old_global_depth) - 1;
                    dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(old_idx));
                    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(old_idx));
                }
            }

        }//after split, try to insert again (another while cycle)
        else
        {
            // the bucket is not full, so we can insert the key-value directly.
            buffer_pool_manager_->UnpinPage(bucket_pid, true);
            success = bucket_page->Insert(key, value, comparator_);
            break;
        }
    }  

    buffer_pool_manager_->UnpinPage(directory_page_id_, grown);
    return success;
}

bool ExtendibleHashTable::Remove(const IndexKey *key, const RowId value, Transaction *transaction)
{

}

bool ExtendibleHashTable::Merge(const IndexKey *key, const RowId value, Transaction *transaction)
{

}

bool ExtendibleHashTable::GetValue(const IndexKey *key, vector<RowId>& result, Transaction *transaction)
{
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
    
    page_id_t bucket_pid = KeyToPageId(key, dir_page);
    auto bucket_page = reinterpret_cast<HashTableBucketPage *>
        (buffer_pool_manager_->FetchPage(bucket_pid)->GetData());
    
    bool suc = bucket_page->GetValue(key, comparator_, &result);
    buffer_pool_manager_->UnpinPage(bucket_pid, false);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return suc;
}

uint32_t ExtendibleHashTable::GetGlobalDepth()
{
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
    uint32_t ret = dir_page->GetGlobalDepth();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return ret;
}   

void ExtendibleHashTable::VerifyIntegrity()
{
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
}

inline uint32_t ExtendibleHashTable::Hash(const IndexKey *key)
{
    return static_cast<uint32_t>(hash_func_.GetHash(key));
}


//key --<hash function>--> hash(key) --<global mask>--> key index --<directory>--> bucket pid 
inline uint32_t ExtendibleHashTable::KeyToDirectoryIndex(const IndexKey *key, HashTableDirectoryPage *dir_page)
{
    return Hash(key) & dir_page->GetGlobalDepthMask();//get the mod
}

inline page_id_t ExtendibleHashTable::KeyToPageId(const IndexKey *key, HashTableDirectoryPage *dir_page)
{
    return dir_page->GetBucketPageId(KeyToDirectoryIndex(key ,dir_page));
}

uint32_t ExtendibleHashTable::Pow(uint32_t base, uint32_t power) const {
  return static_cast<uint32_t>(std::pow(static_cast<long double>(base), static_cast<long double>(power)));
}