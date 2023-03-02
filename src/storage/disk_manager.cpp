#include <sys/stat.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <ostream>
#include <stdexcept>

#include "buffer/lru_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "page/page.h"
#include "storage/disk_manager.h"

static uint32_t bitmap_capacity = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
inline extent_id_t getSectionId(page_id_t logical_page_id) { return logical_page_id / bitmap_capacity; }
inline uint32_t getPageOffset(page_id_t logical_page_id) { return logical_page_id % bitmap_capacity; }
inline page_id_t getSectionMetaPageId(page_id_t extent_id) { return 1 + extent_id * (1 + bitmap_capacity); }
inline page_id_t getLogicalPageId(extent_id_t extent_id, uint32_t page_offset) {
  return extent_id * bitmap_capacity + page_offset;
}


DiskManager::DiskManager(const std::string &db_file, LogManager* log_manager) : 
file_name_(db_file), log_manager_(log_manager){
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  closed = false;
  diskmeta_page_ = new Page();
  ReadPhysicalPage(META_PAGE_ID, diskmeta_page_->GetData());
  if(CUR_REPLACER_TYPE == LRU)
    replacer_ = new LRUReplacer(BUFFER_SIZE);
  else if(CUR_REPLACER_TYPE == CLOCK)
    replacer_ = new ClockReplacer(BUFFER_SIZE);
  for (size_t i = 0; i < BUFFER_SIZE; i++) free_list_.emplace_back(i);

  bitmap_page_cache_ = new Page[BUFFER_SIZE];
  cur_txn_ = nullptr;
}

bool DiskManager::FlushAllMeta() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    for (auto it : page_table_) WritePhysicalPage(getSectionMetaPageId(it.first), bitmap_page_cache_[it.second].GetData());
    WritePhysicalPage(META_PAGE_ID, this->diskmeta_page_->GetData());
  }
  return true;
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  FlushAllMeta();
  db_io_.close();
  closed = true;
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  if (IsPageFree(logical_page_id)) return;
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  if (IsPageFree(logical_page_id)) return;
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  //special condition: target pid while redo new or undo delete
  // if(target_pid!=INVALID_PAGE_ID)
  // {
  //   extent_id_t extent_id = getSectionId(target_pid);
  //   uint32_t page_offset = getPageOffset(target_pid);
  // }

  // find a free section
  Page* mp = FetchDiskMetaPage(true);
  DiskFileMetaPage *disk_meta = reinterpret_cast<DiskFileMetaPage *>(mp);
  extent_id_t extent_id = disk_meta->next_unfull_extent;
  page_id_t allocated_id = 0;
  ASSERT(extent_id < DiskFileMetaPage::GetMaxNumExtents(), "Invalid next_unfull_extent in DiskMetaPage.");
  if (extent_id < disk_meta->num_extents_) {  // this next_free_section is not full indeed
    // find a free page in this section
    // load the page meta data
    Page *p = FetchBitmapPage(extent_id, true);
    BitmapPage<PAGE_SIZE> *bmp_meta = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(p->data_);
    uint32_t page_offset = 0;
    if (bmp_meta->AllocatePage(page_offset)) {
      allocated_id = getLogicalPageId(extent_id, page_offset);
      disk_meta->extent_used_page_[extent_id] += 1;
      disk_meta->num_allocated_pages_ += 1;
      if (disk_meta->extent_used_page_[extent_id] == bitmap_capacity) {
        // this extent is full. Find a new unfull extent.
        disk_meta->next_unfull_extent = disk_meta->num_extents_;
        for (uint32_t i = 0; i < disk_meta->GetExtentNums(); i++) {
          if (disk_meta->extent_used_page_[i] < bitmap_capacity) {
            disk_meta->next_unfull_extent = i;
            break;
          }
        }
      }
      p->is_dirty_ = 1;
      UnpinBitmapPage(extent_id, true);
      UnpinDiskMetaPage(true);
    } else {
      ASSERT(0, "Allocate Extent page failed.");
    }
  } else {  // the next_free_section is full, indicates that all allocated sections are full
            // allocate a new section
    ASSERT(disk_meta->num_extents_ < DiskFileMetaPage::GetMaxNumExtents(), "All Extents are full.");
    extent_id = disk_meta->num_extents_;
    Page *p = FetchBitmapPage(extent_id, true);
    BitmapPage<PAGE_SIZE> *ext_meta = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(p->data_);
    uint32_t page_offset;
    if (ext_meta->AllocatePage(page_offset)) {
      disk_meta->num_extents_ += 1;
      disk_meta->extent_used_page_[extent_id] = 1;
      disk_meta->num_allocated_pages_ += 1;
      allocated_id = getLogicalPageId(extent_id, page_offset);
      p->is_dirty_ = 1;
      UnpinBitmapPage(extent_id, true);
      UnpinDiskMetaPage(true);
    } else {
      ASSERT(0, "Allocate Extent page failed.");
    }
  }
  return allocated_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  // firstly check if this page is allocated
  extent_id_t extId = getSectionId(logical_page_id);
  // ReadPhysicalPage(0, DiskMetaPage);
  Page* mp = FetchDiskMetaPage(true);
  DiskFileMetaPage *disk_meta = reinterpret_cast<DiskFileMetaPage *>(mp);
  // if that section is full , now it is free
  if (disk_meta->num_allocated_pages_ && disk_meta->extent_used_page_[extId]) {
    Page *p = FetchBitmapPage(extId, true);
    BitmapPage<PAGE_SIZE> *ext_meta = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(p->data_);
    uint32_t page_offset = getPageOffset(logical_page_id);
    if (ext_meta->DeAllocatePage(page_offset) && disk_meta->extent_used_page_[extId]) {
      disk_meta->num_allocated_pages_ -= 1;
      disk_meta->extent_used_page_[extId] -= 1;
      p->is_dirty_ = 1;
    }else{
      ASSERT(0,"dealloc page failed.");
    }
    UnpinBitmapPage(extId, true);
    UnpinDiskMetaPage(true);
  }else{
    ASSERT(0,"dealloc page failed.");
  }
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  Page* mp = FetchDiskMetaPage(false);
  DiskFileMetaPage *disk_meta = reinterpret_cast<DiskFileMetaPage *>(mp);
  extent_id_t ext_id = getSectionId(logical_page_id);
  if (disk_meta->extent_used_page_[ext_id] == bitmap_capacity) 
  {
    UnpinDiskMetaPage(false);
    return false;
  }
  Page *meta_page = FetchBitmapPage(ext_id, false);
  BitmapPage<PAGE_SIZE> *ext_meta = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(meta_page->GetData());
  if (ext_meta->IsPageFree(getPageOffset(logical_page_id))) 
  {
    UnpinBitmapPage(ext_id, false);
    UnpinDiskMetaPage(false);
    return true;
  }
  UnpinBitmapPage(ext_id, false);
  UnpinDiskMetaPage(false);
  return false;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  page_id_t extent_id = logical_page_id / bitmap_capacity;
  return 1 + getSectionMetaPageId(extent_id) + getPageOffset(logical_page_id);
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}

Page *DiskManager::FetchBitmapPage(extent_id_t extent_id, bool to_write) {
  latch_.lock();
  auto it = page_table_.find(extent_id);
  frame_id_t fid;
  Page *p;
  bool is_free_frame = false;
  if (it != page_table_.end()) {
    fid = it->second;
    p = bitmap_page_cache_ + fid;
    if(USING_LOG && to_write)
    {
      char old_data[PAGE_SIZE];
      memcpy(old_data, p->GetData(), PAGE_SIZE);
      old_bitmappage_map_[extent_id] = old_data;
    }
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
  if (!free_list_.empty()) {
    fid = free_list_.back();
    free_list_.pop_back();
    is_free_frame = true;
  } else if (!replacer_->Victim(&fid))
    ASSERT(0, "Failed to victim a page.");
  p = bitmap_page_cache_ + fid;
  page_id_t old_pid = p->page_id_;
  if (p->is_dirty_) {
    p->WLatch();
    WritePhysicalPage(getSectionMetaPageId(old_pid), p->data_);
    p->WUnlatch();
    p->is_dirty_ = 0;
  }
  if (!is_free_frame) {
    it = page_table_.find(old_pid);
    page_table_.erase(it);
  }
  page_table_[extent_id] = fid;

  p->WLatch();
  p->pin_count_ = 1;
  p->page_id_ = extent_id;
  p->is_dirty_ = 0;
  ReadPhysicalPage(getSectionMetaPageId(extent_id), p->data_);
  p->WUnlatch();

  if(USING_LOG && to_write)
  {
    char old_data[PAGE_SIZE];
    memcpy(old_data, p->GetData(), PAGE_SIZE);
    old_bitmappage_map_[extent_id] = old_data;
  }
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


bool DiskManager::UnpinBitmapPage(extent_id_t extent_id, bool is_dirty) {
  latch_.lock();
  auto it = page_table_.find(extent_id);
  if (it == page_table_.end()) 
  {
    latch_.unlock();
    return false;
  }
  frame_id_t fid = it->second;
  Page &p = bitmap_page_cache_[fid];
  p.is_dirty_ |= is_dirty;
  if (p.pin_count_) p.pin_count_--;
  if (p.pin_count_ == 0) replacer_->Unpin(fid);

  if(USING_LOG && is_dirty)
  {
    ASSERT(old_bitmappage_map_.find(extent_id)!=old_bitmappage_map_.end(), "Unpin not matched!");
    char *old_data = old_bitmappage_map_[extent_id].data_;
    old_bitmappage_map_.erase(extent_id);

    txn_id_t tid = cur_txn_==nullptr?INVALID_TXN_ID:cur_txn_->GetTid();
    LogRecordType type = BITMAP_WRITE;
    LogRecord* append_rec = new LogRecord(type, log_manager_->GetMaxLSN()+1, tid, INVALID_PAGE_ID, 
        old_data, p.GetData(), extent_id, nullptr, nullptr);
    //log_manager_->AddRecord(append_rec);
    //std::cout<<"adding bitmap record"<<std::endl;
  }

  if(is_dirty)
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

Page *DiskManager::FetchDiskMetaPage(bool to_write) {
  latch_.lock();
  if(USING_LOG && to_write)
  {
    char old_data[PAGE_SIZE];
    memcpy(old_data, diskmeta_page_->GetData(), PAGE_SIZE);
    old_diskmeta_data_ = old_data;
  }
  latch_.unlock();
  return diskmeta_page_;
}

bool DiskManager::UnpinDiskMetaPage(bool is_dirty){
  latch_.lock();
  if(USING_LOG && is_dirty)
  {
      char *old_data = old_diskmeta_data_.data_;
      txn_id_t tid = cur_txn_==nullptr?INVALID_TXN_ID:cur_txn_->GetTid();
      LogRecordType type = DISKMETA_WRITE;
      LogRecord* append_rec = new LogRecord(type, log_manager_->GetMaxLSN()+1, tid, INVALID_PAGE_ID, 
          old_data, diskmeta_page_->GetData(), INVALID_FRAME_ID, nullptr, nullptr);
      //log_manager_->AddRecord(append_rec);
      //std::cout<<"adding diskmeta record"<<std::endl;
  }
  latch_.unlock();
  return true;
}

DiskManager::~DiskManager() {
  if (!closed) {
    Close();
  }
  delete[] bitmap_page_cache_;
  delete diskmeta_page_;
  delete replacer_; 
}
