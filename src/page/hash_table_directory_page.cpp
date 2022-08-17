
#include "page/hash_table_directory_page.h"

page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; }

void HashTableDirectoryPage::SetPageId(page_id_t page_id) { page_id_ = page_id; }

page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx)
{
    return bucket_page_ids_[bucket_idx];
}

void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id)
{
    bucket_page_ids_[bucket_idx] = bucket_page_id;
}

//return the index of the coming split image bucket page
uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx)
{
    auto high_bits = GetLocalHighBit(bucket_idx);
    uint32_t split_image_index = (bucket_idx & (Pow(2, GetLocalDepth(bucket_idx) - 1) - 1)) | high_bits;
    return split_image_index & GetLocalDepthMask(bucket_idx);
}

uint32_t HashTableDirectoryPage::GetGlobalDepthMask()
{
    return Pow(2, global_depth_) - 1;
}

uint32_t HashTableDirectoryPage::GetLocalDepthMask(uint32_t bucket_idx)
{
    return Pow(2, local_depths_[bucket_idx]) - 1;
}

uint32_t HashTableDirectoryPage::GetGlobalDepth()
{
    return global_depth_;
}

void HashTableDirectoryPage::IncrGlobalDepth()
{
    global_depth_++;
    if(Pow(2, global_depth_)>MAX_HASH_BUCKET_PAGE_NUM)
    {
        cout<<"[Overflow]: Too many reserved bucket pages(more than 512) for hash index!"<<endl;
        exit(-1);
    }
}

void HashTableDirectoryPage::DecrGlobalDepth()
{
    global_depth_--;
}

bool HashTableDirectoryPage::CanShrink()
{
    //can shrink only if no local depth equals global depth
    for(uint32_t i=0; i<Size(); i++)
    {
        if(local_depths_[i]==global_depth_)
            return false;
    }
    return true;
}

uint32_t HashTableDirectoryPage::Size()
{
    return Pow(2, global_depth_);
}

uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx)
{
    return local_depths_[bucket_idx];
}

void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth)
{
    local_depths_[bucket_idx] = local_depth;
}

void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx)
{
    local_depths_[bucket_idx]++;
}

void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx)
{
    local_depths_[bucket_idx]--;
}

uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx)
{
    auto local_depth = GetLocalDepth(bucket_idx);
    return ((bucket_idx >> (local_depth - 1)) + 1) << (local_depth - 1);
}


uint32_t HashTableDirectoryPage::Pow(uint32_t base, uint32_t power) const
{
    return static_cast<uint32_t>(std::pow(static_cast<long double>(base), static_cast<long double>(power)));
}