#ifndef MINISQL_BUFFER_POOL_MANAGER_H
#define MINISQL_BUFFER_POOL_MANAGER_H

#include <list>
#include <mutex>
#include <unordered_map>
#include <stack>

#include "buffer/lru_replacer.h"
#include "buffer/clock_replacer.h"
#include "common/config.h"
#include "page/disk_file_meta_page.h"
#include "page/page.h"
#include "storage/disk_manager.h"
#include "transaction/log_manager.h"

using namespace std;

class PageData
{
public:
  PageData(char *data)
  {
    memcpy(data_, data, PAGE_SIZE);
  }
  char data_[PAGE_SIZE];
};

class BufferPoolManager {
 public:
  explicit BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager);

  ~BufferPoolManager();

  Page *FetchPage(page_id_t page_id);

  bool UnpinPage(page_id_t page_id, bool is_dirty);

  bool FlushPage(page_id_t page_id);

  bool FlushAll();

  Page *NewPage(page_id_t &page_id);

  bool DeletePage(page_id_t page_id);

  bool IsPageFree(page_id_t page_id);

  bool CheckAllUnpinned();

  // add my hit rate check function
  double get_hit_rate();
  void ResetCounter();

  void SetTxn(Transaction* txn) {cur_txn_ = txn;}

  int GetStackSize()
  {
    return is_new_stack_.size();
  }

 private:
  /**
   * Allocate new page (operations like create index/table) For now just keep an increasing counter
   */
  page_id_t AllocatePage();

  /**
   * Deallocate page (operations like drop index/table) Need bitmap in header page for tracking pages
   */
  void DeallocatePage(page_id_t page_id);

 private:
  size_t pool_size_;                                      // number of pages in buffer pool
  Page *pages_;                                           // array of pages
  DiskManager *disk_manager_;                             // pointer to the disk manager.
  
  LogManager *log_manager_;                               // pointer to the log manager(added)

  std::unordered_map<page_id_t, frame_id_t> page_table_;  // to keep track of pages
  Replacer *replacer_;                                    // to find an unpinned page for replacement
  std::list<frame_id_t> free_list_;                       // to find a free page for replacement
  recursive_mutex latch_;                                 // to protect shared data structure

  Transaction * cur_txn_;//currenct transaction that occupies the buffer pool 

  stack<PageData> old_data_stack_;
  stack<bool> is_new_stack_;

  int hit_num;
  int miss_num;
};

#endif  // MINISQL_BUFFER_POOL_MANAGER_H
