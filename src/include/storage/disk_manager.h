#ifndef DISK_MGR_H
#define DISK_MGR_H

#include <atomic>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include "buffer/replacer.h"
#include "buffer/lru_replacer.h"
#include "buffer/clock_replacer.h"
#include "common/config.h"
#include "common/macros.h"
#include "page/bitmap_page.h"
#include "page/disk_file_meta_page.h"
#include "transaction/log_manager.h"

/**
 * DiskManager takes care of the allocation and de allocation of pages within a database. It performs the reading and
 * writing of pages to and from disk, providing a logical file layer within the context of a database management system.
 *
 * Disk page storage format: (Free Page BitMap Size = PAGE_SIZE * 8, we note it as N)
 * | Meta Page | Free Page BitMap 1 | Page 1 | Page 2 | ....
 *      | Page N | Free Page BitMap 2 | Page N+1 | ... | Page 2N | ... |
 */

class Page;

class PageData
{
public:
  PageData(){}
  PageData(char *data)
  {
    memcpy(data_, data, PAGE_SIZE);
  }
  char data_[PAGE_SIZE];
};

class DiskManager {
public:
  explicit DiskManager(const std::string &db_file, LogManager* log_manager);

  ~DiskManager();

  /**
   * Read page from specific page_id
   * Note: page_id = 0 is reserved for disk meta page
   */
  void ReadPage(page_id_t logical_page_id, char *page_data);

  /**
   * Write data to specific page
   * Note: page_id = 0 is reserved for disk meta page
   */
  void WritePage(page_id_t logical_page_id, const char *page_data);

  /**
   * Get next free page from disk
   * @return logical page id of allocated page
   */
  page_id_t AllocatePage();

  /**
   * Free this page and reset bit map
   */
  void DeAllocatePage(page_id_t logical_page_id);

  /**
   * Return whether specific logical_page_id is free
   */
  bool IsPageFree(page_id_t logical_page_id);

  /**
   * Flush all meta(disk meta and bitmap pages) in memory
   */
  bool FlushAllMeta();

   /**
   * Flush abd close the file
   */
  void Close();

  Page* FetchBitmapPage(extend_id_t extent_id, bool to_write);

  bool UnpinBitmapPage(extend_id_t extent_id, bool is_dirty);

  Page *FetchDiskMetaPage(bool to_write);

  bool UnpinDiskMetaPage(bool is_dirty);

  void SetTxn(Transaction* txn) { cur_txn_ = txn; }

  static constexpr size_t BITMAP_SIZE = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();

private:

  static constexpr size_t BUFFER_SIZE = 1024;
  /**
   * Helper function to get disk file size
   */
  int GetFileSize(const std::string &file_name);

  /**
   * Read physical page from disk
   */
  void ReadPhysicalPage(page_id_t physical_page_id, char *page_data);


  /*************************************
   * The Read/Write operation of meta page should be buffered too, so i add this here
   *************************************/
  Page* FetchBitmapPage(extend_id_t extent_id);

  /**
   * Write data to physical page in disk
   */
  void WritePhysicalPage(page_id_t physical_page_id, const char *page_data);

  /**
   * Map logical page id to physical page id
   */
  page_id_t MapPageId(page_id_t logical_page_id);
  

private:
  // stream to write db file
  std::fstream db_io_;
  std::string file_name_;
  // with multiple buffer pool instances, need to protect file access
  std::recursive_mutex db_io_latch_;
  bool closed{false};

  //pool for bitmap pages
  std::unordered_map<extend_id_t, frame_id_t> page_table_;
  std::list<frame_id_t> free_list_;
  Replacer * replacer_;
  LogManager* log_manager_;
  std::unordered_map<extend_id_t, PageData> old_bitmappage_map_;
  PageData old_diskmeta_data_;
  Transaction * cur_txn_;
  //std::list<frame_id_t> free_list_;  we do not need this free list at all. it is slow. Just use map.

  Page* diskmeta_page_;
  Page *bitmap_page_cache_;
  std::recursive_mutex latch_;

};

#endif