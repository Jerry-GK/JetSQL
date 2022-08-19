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

    ASSERT(VerifyIntegrity(), "vertification failed");
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
        bool split_suc = SplitInsert(key, value, transaction);
        ASSERT(VerifyIntegrity(), "vertification failed");
        return split_suc;
    }
    else//insert directly
    {
        //cout<<"directly insert to bucket "<<KeyToDirectoryIndex(key, dir_page)<<endl;
        suc = bucket_page->Insert(key, value, comparator_);
    }
    buffer_pool_manager_->UnpinPage(bucket_pid, suc);
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    ASSERT(VerifyIntegrity(), "vertification failed");
    return suc;
}

bool ExtendibleHashTable::SplitInsert(const IndexKey *key, const RowId value, Transaction *transaction)
{
    //cout<<"split insert"<<endl;
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
    bool success = false;
    bool grown = false;
    //cout<<"split insert bucket "<<KeyToDirectoryIndex(key, dir_page)<<endl;

    //loop to insert with possible split. terminate only when directly nsert into a non-full bucket
    while(true)
    {
        grown = false;
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

            page_id_t split_bucket_pid;// new page for split
            HashTableBucketPage* split_page = nullptr;
            split_page = reinterpret_cast<HashTableBucketPage *>
                (buffer_pool_manager_->NewPage(split_bucket_pid));
            //cout<<"new split bucket page "<<split_bucket_pid<<endl;
            split_page->Init(key_size_);
            
            dir_page->SetBucketPageId(split_bucket_idx, split_bucket_pid);
            //the new split bucket has the same depth with the original bucket(after increase, 1 more than before split)
            dir_page->SetLocalDepth(split_bucket_idx, dir_page->GetLocalDepth(bucket_idx));

            //some of pointers to original bucket which increases its local depth, might point to split bucket now
            for(uint32_t i = 0;i<Pow(2, dir_page->GetGlobalDepth() - (grown?1:0) );i++)
            {
                if(i==split_bucket_idx)
                    continue;
                if(dir_page->GetBucketPageId(i)==bucket_pid)
                {  
                    //no need to change <=> i and bucket_idx are the same after being masked by local depth
                    uint32_t local_mask = dir_page->GetLocalDepthMask(bucket_idx);

                    if( !( (i & local_mask) == (bucket_idx & local_mask) ) )
                    {
                        dir_page->SetBucketPageId(i, split_bucket_pid);
                    }
                    //reset local depth
                    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(bucket_idx));
                }
            }

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

            //rearrange the new coming directory information if grown
            if(grown)
            {
                uint32_t old_global_depth = dir_page->GetGlobalDepth() - 1;
                for(uint32_t i = Pow(2, old_global_depth); i<dir_page->Size(); i++)
                {
                    if(i == split_bucket_idx)
                    {
                        continue;
                    }
                    uint32_t new_idx = i & (Pow(2, old_global_depth) - 1);
                    dir_page->SetBucketPageId(i, dir_page->GetBucketPageId(new_idx));
                    dir_page->SetLocalDepth(i, dir_page->GetLocalDepth(new_idx));
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
    //cout<<"remove"<<endl;
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
    
    page_id_t bucket_pid = KeyToPageId(key, dir_page);
    auto bucket_page = reinterpret_cast<HashTableBucketPage *>
        (buffer_pool_manager_->FetchPage(bucket_pid)->GetData());

    bool suc = false;
    suc = bucket_page->Remove(key, value, comparator_);

    if(suc && bucket_page->IsEmpty())//empty bucket after remove, do merge
    {
        buffer_pool_manager_->UnpinPage(bucket_pid, suc);
        Merge(key, value, transaction);
    }
    else
        buffer_pool_manager_->UnpinPage(bucket_pid, suc);

    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    //ASSERT(suc, "remove failed!");
    ASSERT(VerifyIntegrity(), "vertification failed");
    return suc;
}

void ExtendibleHashTable::Merge(const IndexKey *key, const RowId value, Transaction *transaction)
{
    //cout<<"merge"<<endl;
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());

    for(uint32_t i = 0; ; i++) 
    {
        if(i >= dir_page->Size())//must be here since directory size might change
            break;
        
        auto bucket_pid = dir_page->GetBucketPageId(i);
        auto bucket_page = reinterpret_cast<HashTableBucketPage *>
            (buffer_pool_manager_->FetchPage(bucket_pid)->GetData());
        if(bucket_page->IsEmpty() && dir_page->GetLocalDepth(i) > 1)
        {
            auto split_bucket_idx = dir_page->GetSplitImageIndex(i);
            //1.need to decrease local depth
            if(dir_page->GetLocalDepth(split_bucket_idx) == dir_page->GetLocalDepth(i))
            {
                //delete page
                buffer_pool_manager_->UnpinPage(bucket_pid, true);
                buffer_pool_manager_->DeletePage(bucket_pid);
                //cout<<"del page "<<bucket_pid<<endl;

                dir_page->DecrLocalDepth(i);
                dir_page->DecrLocalDepth(split_bucket_idx);
                //merge the empty bucket to image bucket
                page_id_t merge_bucket_pid = dir_page->GetBucketPageId(split_bucket_idx);
                dir_page->SetBucketPageId(i, merge_bucket_pid);

                //reallocate elements not in the merge bucket since its depth has been decreased
                for (uint32_t j = 0; j < dir_page->Size(); j++) 
                {
                    if (j == i || j == split_bucket_idx) {
                        continue;
                    }
                    auto cur_bucket_page_id = dir_page->GetBucketPageId(j);
                    if (cur_bucket_page_id == bucket_pid || cur_bucket_page_id == merge_bucket_pid) {
                        dir_page->SetLocalDepth(j, dir_page->GetLocalDepth(i));
                        dir_page->SetBucketPageId(j, merge_bucket_pid);
                    }
                }

            }
            //delete page but no need to decrease depth for image bucket
            else
            {
                //cout<<"not decrease local depth"<<endl;
                //delete page
                buffer_pool_manager_->UnpinPage(bucket_pid, true);
            }
            //shrink the global depth if need
            if(dir_page->CanShrink())
            {
                dir_page->DecrGlobalDepth();
                //cout<<"shrink to depth "<<dir_page->GetGlobalDepth()<<endl;
            }
        }
        else
            buffer_pool_manager_->UnpinPage(bucket_pid, true);
    }
    buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true);
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
    ASSERT(VerifyIntegrity(), "vertification failed");
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

inline uint32_t ExtendibleHashTable::Hash(const IndexKey *key)
{
    return hash_func_.GetHash(key);
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

bool ExtendibleHashTable::VerifyIntegrity() 
{
    auto dir_page = reinterpret_cast<HashTableDirectoryPage *>
        (buffer_pool_manager_->FetchPage(directory_page_id_)->GetData());
    bool suc = dir_page->VerifyIntegrity();
    buffer_pool_manager_->UnpinPage(directory_page_id_, false);
    return suc;
}