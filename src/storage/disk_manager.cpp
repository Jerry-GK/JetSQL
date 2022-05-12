#include <cstdint>
#include <cstring>
#include <ostream>
#include <stdexcept>
#include <iostream>
#include <sys/stat.h>

#include "common/config.h"
#include "common/macros.h"
#include "glog/logging.h"
#include "page/bitmap_page.h"
#include "storage/disk_manager.h"

static uint32_t bitmap_capacity = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
inline page_id_t getSectionId(page_id_t logical_page_id){
  return logical_page_id / bitmap_capacity;
}
inline uint32_t getPageOffset(page_id_t logical_page_id){
  return logical_page_id % bitmap_capacity;
}
inline page_id_t getSectionMetaPageId(page_id_t extent_id){
  return 1 + extent_id * (1 + bitmap_capacity);
}
inline page_id_t getLogicalPageId(uint32_t extent_id,uint32_t page_offset){
  return extent_id * bitmap_capacity + page_offset;
}
DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
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
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

page_id_t DiskManager::AllocatePage() {
  
  //find a free section
  DiskFileMetaPage * disk_meta = reinterpret_cast<DiskFileMetaPage * >(meta_data_);
  uint32_t extent_id = disk_meta->next_unfull_extent;
  page_id_t allocated_id = 0;
  char ext_meta_buffer[PAGE_SIZE];
  ASSERT(extent_id <  DiskFileMetaPage::GetMaxNumExtents(), "Invalid next_unfull_extent in DiskMetaPage.");
  if( extent_id < disk_meta->num_extents_ ){ // this next_free_section is not full indeed 
    // find a free page in this section
    // load the page meta data
    ReadPhysicalMetaPage(extent_id, ext_meta_buffer);
    BitmapPage<PAGE_SIZE> * bmp_meta = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(ext_meta_buffer);
    uint32_t page_offset = 0;
    if(bmp_meta->AllocatePage(page_offset)){
      allocated_id = getLogicalPageId(extent_id, page_offset);
      disk_meta->extent_used_page_[extent_id] += 1;
      disk_meta->num_allocated_pages_ += 1;
      if(disk_meta->extent_used_page_[extent_id] == bitmap_capacity){
        // this extent is full. Find a new unfull extent.
        disk_meta->next_unfull_extent = disk_meta->num_extents_;
        for(uint32_t i = 0;i < disk_meta->GetExtentNums();i ++){
          if(disk_meta->extent_used_page_[i] <  bitmap_capacity){
            disk_meta->next_unfull_extent = i;
            break;
          }
        }
      }
      WritePhysicalMetaPage(extent_id, ext_meta_buffer);
    }else{
      ASSERT(0,"Allocate Extent page failed.");
    }
  }else{  // the next_free_section is full, indicates that all allocated sections are full
          // allocate a new section
    ASSERT(disk_meta->num_extents_ < DiskFileMetaPage::GetMaxNumExtents(),"All Extents are full.");
    extent_id = disk_meta->num_extents_;
    ReadPhysicalMetaPage(extent_id, ext_meta_buffer);
    BitmapPage<PAGE_SIZE> * ext_meta = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(ext_meta_buffer);
    uint32_t page_offset;
    if(ext_meta->AllocatePage(page_offset)){
      disk_meta->num_extents_ += 1;
      disk_meta->extent_used_page_[extent_id] = 1;
      disk_meta->num_allocated_pages_ += 1;
      allocated_id = getLogicalPageId(extent_id, page_offset);
      WritePhysicalMetaPage(extent_id, ext_meta_buffer);
    }else{
      ASSERT(0,"Allocate Extent page failed.");
    }
  }
  return allocated_id;
}

void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  //firstly check if this page is allocated
  uint32_t extId = getSectionId(logical_page_id);
  char ext_meta_buffer[PAGE_SIZE];
  //ReadPhysicalPage(0, DiskMetaPage);
  DiskFileMetaPage * disk_meta = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  //if that section is full , now it is free
  if(disk_meta->num_allocated_pages_ && disk_meta->extent_used_page_[extId]){
    ReadPhysicalMetaPage(extId, ext_meta_buffer);
    BitmapPage<PAGE_SIZE> * ext_meta = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(ext_meta_buffer);
    uint32_t page_offset = getPageOffset(logical_page_id);
    if(ext_meta->DeAllocatePage(page_offset) && disk_meta->extent_used_page_[extId]){
      disk_meta->num_allocated_pages_ -= 1;
      disk_meta->extent_used_page_[extId] -= 1;
      WritePhysicalMetaPage(extId, ext_meta_buffer);
      WritePhysicalPage(0, meta_data_);
    }
  }
}

bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  DiskFileMetaPage * disk_meta = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  uint32_t ext_id = getSectionId(logical_page_id);
  if(disk_meta->extent_used_page_[ext_id] == bitmap_capacity)return false;
  char ext_meta_buffer[PAGE_SIZE];
  BitmapPage<PAGE_SIZE> * ext_meta = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(ext_meta_buffer);
  if(ext_meta->IsPageFree(getPageOffset(logical_page_id)))return true;
  return false;
}

page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  page_id_t extent_id = logical_page_id / bitmap_capacity;
  return getSectionMetaPageId(extent_id) + getPageOffset(logical_page_id);
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

void DiskManager::UpdateBufferPriority(uint32_t index){
  for(uint32_t i =0;i< BUFFER_SIZE;i++)meta_buffer_tlb_[i].priority_ += 1;
  if(index < BUFFER_SIZE)meta_buffer_tlb_[index].priority_ = 0;
}

void DiskManager::ReadPhysicalMetaPage(uint32_t extent_id,char *page_data){
  uint32_t meta_physical_pageid = getSectionMetaPageId(extent_id);
  for(uint32_t i =0;i< BUFFER_SIZE;i++){
    if(meta_buffer_tlb_[i].index_ == extent_id && meta_buffer_tlb_[i].present_){
      // tlb hit
      UpdateBufferPriority(i);
      std::memcpy(page_data,meta_buffer_pool_[i],PAGE_SIZE);
      return;
    }
  }
  // tlb miss  
  uint32_t idx= 0 ;
  uint32_t pr = meta_buffer_tlb_[0].priority_;
  for(uint32_t i =0 ;i<BUFFER_SIZE;i++){
    // search for not present pages first
    if(!meta_buffer_tlb_[i].present_){
      meta_buffer_tlb_[i].present_ = 1;
      meta_buffer_tlb_[i].index_ = extent_id;
      UpdateBufferPriority(i);
      ReadPhysicalPage(meta_physical_pageid,meta_buffer_pool_[i]);
      std::memcpy(page_data,meta_buffer_pool_[i],PAGE_SIZE);
      return;
    }
    // search for the least recently used page
    if(meta_buffer_tlb_[i].priority_ > pr){
      idx = i;
      pr = meta_buffer_tlb_[i].priority_;
    }
  }
  UpdateBufferPriority(idx);
  if(meta_buffer_tlb_[idx].dirty_ ) WritePhysicalPage(getSectionMetaPageId(meta_buffer_tlb_[idx].index_), page_data);
  meta_buffer_tlb_[idx].present_ = 1;
  meta_buffer_tlb_[idx].index_ = extent_id;
  ReadPhysicalPage(meta_physical_pageid, meta_buffer_pool_[idx]);
  std::memcpy(page_data,meta_buffer_pool_[idx],PAGE_SIZE);
}

void DiskManager::WritePhysicalMetaPage(uint32_t extent_id,char *page_data){
  for(uint32_t i =0;i< BUFFER_SIZE;i++){
    if(meta_buffer_tlb_[i].index_ == extent_id && meta_buffer_tlb_[i].present_){
      // tlb hit
      UpdateBufferPriority(i);
      std::memcpy(meta_buffer_pool_[i],page_data,PAGE_SIZE);
      meta_buffer_tlb_[i].dirty_ = 1;
      return;
    }
  }
  // tlb miss  
  uint32_t idx= 0 ;
  uint32_t pr = meta_buffer_tlb_[0].priority_;
  for(uint32_t i =0 ;i<BUFFER_SIZE;i++){
    // search for not present pages first
    if(!meta_buffer_tlb_[i].present_){
      meta_buffer_tlb_[i].present_ = 1;
      meta_buffer_tlb_[i].index_ = extent_id;
      meta_buffer_tlb_[i].dirty_ = 1;
      UpdateBufferPriority(i);
      std::memcpy(meta_buffer_pool_[i],page_data,PAGE_SIZE);
      return;
    }
    // search for the least recently used page
    if(meta_buffer_tlb_[i].priority_ > pr){
      idx = i;
      pr = meta_buffer_tlb_[i].priority_;
    }
  }
  UpdateBufferPriority(idx);
  if(meta_buffer_tlb_[idx].dirty_ ) WritePhysicalPage(getSectionMetaPageId(meta_buffer_tlb_[idx].index_), page_data);
  meta_buffer_tlb_[idx].present_ = 1;
  meta_buffer_tlb_[idx].index_ = extent_id;
  meta_buffer_tlb_[idx].dirty_ = 1;
  std::memcpy(meta_buffer_pool_[idx],page_data,PAGE_SIZE);
}