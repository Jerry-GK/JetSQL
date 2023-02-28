#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  pages_ = new Page[pool_size_];
  latch_.lock();
  if(CUR_REPLACER_TYPE == LRU)
    replacer_ = new LRUReplacer(pool_size_);
  else if(CUR_REPLACER_TYPE == LRU == CLOCK)
    replacer_ = new ClockReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++) {
    free_list_.emplace_back(i);      
  }
  hit_num = 0;
  miss_num = 0;
  latch_.unlock();
}

BufferPoolManager::~BufferPoolManager() {
  latch_.lock();
  FlushAll();
  delete[] pages_;
  delete replacer_;
  latch_.unlock();
}

bool BufferPoolManager::FlushAll() {
  latch_.lock();
  for (auto page : page_table_) {
    if (pages_[page.second].is_dirty_)
      if (!FlushPage(page.first)) return false;
  }
  latch_.unlock();
  return true;
}

Page *BufferPoolManager::FetchPage(page_id_t page_id, bool to_write) {
  latch_.lock();
  //std::cout<<"New "<<page_id<<std::endl;  
  // the page is free ,you cannot fetch it!
  if (IsPageFree(page_id)) return nullptr;
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  frame_id_t fid;
  auto it = page_table_.find(page_id);
  bool is_free_frame = false;
  if (it != page_table_.end()) {
    frame_id_t fid = it->second;

    replacer_->Pin(fid);
    Page *r = pages_ + fid;
    ASSERT(r->page_id_ == page_id, "Inconsistent map!");
    r->pin_count_ += 1;
    hit_num++;

    if(USING_LOG && to_write)
    {
      is_new_map_[page_id]=false;
      if(is_new_map_.size()>1)
      {
        //cout<<"map size >= 2 after fetch page "<<r->page_id_<<", size = "<<is_new_map_.size()<<endl;
        //ASSERT(false, "stop1");
      }

      char old_data[PAGE_SIZE];
      memcpy(old_data, r->GetData(), PAGE_SIZE);
      old_data_map_[page_id] = old_data;
    }
    //latch accouding to write intention
    if(to_write)
    {
      r->WLatch();
    }
    else
    {
      r->RLatch();
    }

    latch_.unlock();
    return r;
  }
  miss_num++;
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
    is_free_frame = true;
  } else {
    miss_num++;
    if (!replacer_->Victim(&fid)) {
      latch_.unlock();
      return nullptr;
    }
  }

  Page *p = pages_ + fid;
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.

  // 3.     Delete R from the page table and insert P.
  if (!is_free_frame) {
    page_id_t old_pid = p->page_id_;
    ASSERT(old_pid != INVALID_PAGE_ID, "invalid page id");
    p->WLatch();
    if (p->is_dirty_) disk_manager_->WritePage(old_pid, p->data_);
    p->WUnlatch();
    it = page_table_.find(old_pid);
    page_table_.erase(it);
  }
  p->RLatch();
  page_table_[page_id] = fid;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  p->pin_count_ = 1;
  p->is_dirty_ = 0;
  p->page_id_ = page_id;
  disk_manager_->ReadPage(page_id, p->data_);
  p->RUnlatch();

  if(USING_LOG && to_write)
  {
    is_new_map_[page_id] = false;
    if(is_new_map_.size()>1)
    {
      //cout<<"map size >= 2 after fetch page "<<p->page_id_<<", size = "<<is_new_map_.size()<<endl;
      //ASSERT(false, "stop2");
    }

    char old_data[PAGE_SIZE];
    memcpy(old_data, p->GetData(), PAGE_SIZE);
    old_data_map_[page_id] = old_data;
  }
  //latch accouding to write intention
  if(to_write)
  {
    p->WLatch();
  }
  else
  {
    p->RLatch();
  }

  latch_.unlock();
  return p;
}

Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  latch_.unlock();

  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  if (free_list_.size() == 0 && page_table_.size() == 0) return nullptr;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t fid;
  bool is_free_frame = false;
  miss_num++;
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
    is_free_frame = true;
  } else {
    if (!replacer_->Victim(&fid)) {
      latch_.unlock();
      return nullptr;
    }
  }

  Page *p = pages_ + fid;
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.

  // 3.     Delete R from the page table and insert P.
  page_id_t newpage = disk_manager_->AllocatePage();
  if (!is_free_frame) {
    page_id_t old_pid = p->page_id_;
    ASSERT(old_pid != INVALID_PAGE_ID, "invalid page id");
    p->WLatch();
    if (p->is_dirty_) disk_manager_->WritePage(old_pid, p->data_);
    p->WUnlatch();
    auto it = page_table_.find(old_pid);
    page_table_.erase(it);
  }
  p->RLatch();
  p->pin_count_ = 1;
  p->is_dirty_ = 1;
  p->page_id_ = newpage;
  p->ResetMemory();
  page_table_[newpage] = fid;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  page_id = newpage;
  p->RUnlatch();
  // ASSERT(page_id != 0,"Newing page 0");

  if(USING_LOG)
  {
    is_new_map_[page_id] = true;
    if(is_new_map_.size()>1)
    {
      //cout<<"map size >= 2 after new page "<<p->page_id_<<", size = "<<is_new_map_.size()<<endl;
      //ASSERT(false, "stop3");
    }
    
    char old_data[PAGE_SIZE];
    memset(old_data, 0, PAGE_SIZE);
    old_data_map_[page_id] = old_data;
  }
  //new page has the intention to be written
  p->WLatch();

  //std::cout<<"New "<<page_id<<std::endl;
  latch_.unlock();
  return p;
}

bool BufferPoolManager::DeletePage(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return false.
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    Page *p = FetchPage(page_id, false);
    if(p==nullptr)
    {
      latch_.unlock();
      return false;
    }
    //disk_manager_->DeAllocatePage(page_id);
    //return true;
    UnpinPage(page_id, false);
  }
  it = page_table_.find(page_id);
  ASSERT(it!=page_table_.end(), "not fetched!");

  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  frame_id_t fid = it->second;
  if (pages_[fid].pin_count_) {
    ASSERT(0, "Delete page failed!");
    latch_.unlock();
    return false;
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  auto &p = pages_[fid];
  p.WLatch();
  page_table_.erase(it);
  disk_manager_->DeAllocatePage(p.page_id_);
  p.pin_count_ = 0;
  p.is_dirty_ = 0;
  p.page_id_ = INVALID_PAGE_ID;
  p.WUnlatch();
  replacer_->Pin(fid);
  free_list_.emplace_back(fid);

  //4. add log record
  if(USING_LOG)
  {
    txn_id_t tid = cur_txn_==nullptr?INVALID_TXN_ID:cur_txn_->GetTid();
    LogRecord* append_rec = new LogRecord(DELETE, log_manager_->GetMaxLSN()+1, tid, page_id, 
        p.GetData(), nullptr, INVALID_EXTEND_ID, nullptr, nullptr);
    log_manager_->AddRecord(append_rec);
  }

  latch_.unlock();
  return true;
}

bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, bool sure) {
  latch_.lock();
  //std::cout<<"Unpin "<<page_id<<std::endl;
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) 
  {
    latch_.unlock();
    return false;
  }
  frame_id_t fid = it->second;
  Page &p = pages_[fid];
  p.is_dirty_ |= is_dirty;
  if (p.pin_count_) p.pin_count_--;
  if (p.pin_count_ == 0) replacer_->Unpin(fid);
  
  //add log record
  if(USING_LOG && is_dirty)
  {
    ASSERT(is_new_map_.find(page_id)!=is_new_map_.end(), "Unpin not matched!");
    bool is_new_top = is_new_map_[page_id];
    is_new_map_.erase(page_id);

    ASSERT(old_data_map_.find(page_id)!=old_data_map_.end(), "Unpin not matched!");
    char *old_data = old_data_map_[page_id].data_;
    old_data_map_.erase(page_id);

    txn_id_t tid = cur_txn_==nullptr?INVALID_TXN_ID:cur_txn_->GetTid();
    LogRecordType type = (is_new_top)?NEW:WRITE;
    LogRecord* append_rec = new LogRecord(type, log_manager_->GetMaxLSN()+1, tid, page_id, 
        old_data, p.GetData(), INVALID_EXTEND_ID, nullptr, nullptr);
    log_manager_->AddRecord(append_rec);
  }

  //unlatch according to is_dirty. if not sure (variable for is_dirty), do wunlatch as well
  if(is_dirty || !sure)
  {
    p.WUnlatch();
  }
  else
  {
    p.RUnlatch();
  }
  latch_.unlock();
  return true;
}

bool BufferPoolManager::FlushPage(page_id_t page_id) {
  latch_.lock();
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) 
  {
    latch_.unlock();
    return false;
  }
  frame_id_t fid = it->second;
  Page &p = pages_[fid];
  p.is_dirty_ = 0;
  disk_manager_->WritePage(page_id, p.data_);
  latch_.unlock();
  return true;
}

page_id_t BufferPoolManager::AllocatePage() {
  latch_.lock();
  int next_page_id = disk_manager_->AllocatePage();
  latch_.unlock();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(page_id_t page_id) { 
  latch_.lock();
  disk_manager_->DeAllocatePage(page_id); 
  latch_.unlock();
}

bool BufferPoolManager::IsPageFree(page_id_t page_id) { 
  latch_.lock();
  latch_.unlock();
  return disk_manager_->IsPageFree(page_id); 
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned() {
  latch_.lock();
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].pin_count_ != 0) {
      res = false;
      LOG(ERROR) << "frame " << i << " page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
      //ASSERT(false ,"unpin error");
    }
  }
  latch_.unlock();
  return res;
}

void BufferPoolManager::ResetCounter() {
  latch_.lock();
  hit_num = 0;
  miss_num = 0;
  latch_.unlock();
}

double BufferPoolManager::get_hit_rate() {
  latch_.lock();
  if (hit_num + miss_num == 0) return 0;
  double hit_rate = (double)(hit_num) / (hit_num + miss_num);
  std::cout << "hit = " << hit_num << "  total = " << miss_num + hit_num << "  hit rate = " << hit_rate << endl;
  latch_.unlock();
  return hit_rate;
}
