#include "buffer/buffer_pool_manager.h"
#include "buffer/lru_replacer_old.h"
#include "buffer/clock_replacer.h"
#include "common/config.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include <iostream>

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
        : pool_size_(pool_size), disk_manager_(disk_manager) {
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);
  }
  hit_num=0;
  miss_num = 0;
}

BufferPoolManager::~BufferPoolManager() {
  for (auto page: page_table_) {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  frame_id_t fid;
  auto it = page_table_.find(page_id);
  bool is_free_frame = false;
  if(it != page_table_.end()){
    frame_id_t fid = it->second;
    replacer_->Pin(fid);
    Page * r = pages_ + fid;
    r->pin_count_ += 1;
    hit_num++;
    return r;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  if(!free_list_.empty()){
    fid = free_list_.back();
    free_list_.pop_back();
    is_free_frame = true;
    hit_num++;
  }
  else
  {
    miss_num++;
    std::cout << "miss num = " << miss_num << std::endl;
    if(!replacer_->Victim(&fid))
    {
      return nullptr;
    }
  }

  Page * p = pages_ + fid;
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  page_id_t old_pid = p->page_id_;
  p->WLatch();
  if(p->is_dirty_)disk_manager_->WritePage(old_pid, p->data_);
  p->WUnlatch();
  p->RLatch();
  // 3.     Delete R from the page table and insert P.
  if(!is_free_frame){ 
    it = page_table_.find(old_pid);
    page_table_.erase(it);
  }
  page_table_[page_id] = fid;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  p->pin_count_ = 1;
  p->is_dirty_ = 0;
  p->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, p->data_);
  p->RUnlatch();
  
  return p;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if(free_list_.size() == 0 && page_table_.size() == 0)return nullptr;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t fid;
  bool is_free_frame = false;
  if(!free_list_.empty()){
    fid = free_list_.back();
    free_list_.pop_back();
    is_free_frame = true;
  }else if(!replacer_->Victim(&fid))return nullptr;
  Page * p = pages_ + fid;
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  page_id_t old_pid = p->page_id_;
  p->WLatch();
  if(p->is_dirty_)disk_manager_->WritePage(old_pid, p->data_);
  p->WUnlatch();
  p->RLatch();
  // 3.     Delete R from the page table and insert P.
  if(!is_free_frame){
    auto it = page_table_.find(old_pid);
    page_table_.erase(it);
  }
  
  p->pin_count_ = 1;
  p->is_dirty_ = 0;
  page_id_t newpage = disk_manager_->AllocatePage();
  p->page_id_ = newpage;
  p->ResetMemory();
  page_table_[newpage] = fid;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = newpage;
  p->RUnlatch();
  return p;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()) {
    disk_manager_->DeAllocatePage(page_id);
    return true;
  }
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t fid = it->second;
  if(pages_[fid].pin_count_)return false;
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto &p = pages_[fid];
  disk_manager_->DeAllocatePage(p.page_id_);
  p.pin_count_ = 0;
  p.is_dirty_ = 0;
  p.page_id_ = INVALID_PAGE_ID;
  replacer_->Pin(fid);
  free_list_.emplace_back(fid);
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  auto it = page_table_.find(page_id);
  if(it == page_table_.end())return false;
  frame_id_t fid = it->second;
  Page &p = pages_[fid];
  p.is_dirty_ |= is_dirty;
  if(p.pin_count_) p.pin_count_--;
  if(p.pin_count_ == 0)replacer_->Unpin(fid);
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  auto it = page_table_.find(page_id);
  if(it == page_table_.end())return false;
  frame_id_t fid = it->second;
  Page &p = pages_[fid];
  p.is_dirty_ = 0;
  disk_manager_->WritePage(page_id, p.data_);
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  int next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) {
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) {
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}

double BufferPoolManager::get_hit_rate()
{
  if(hit_num+miss_num==0)
    return 0;
  double hit_rate = (double)(hit_num) / (hit_num + miss_num);
  std::cout << "hit = " << hit_num << "  total = " << miss_num + hit_num << "  hit rate = "<<hit_rate<<endl;
  return hit_rate;
}
