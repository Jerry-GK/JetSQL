#ifndef MINISQL_INSTANCE_H
#define MINISQL_INSTANCE_H

#include <iostream>
#include <memory>
#include <string>
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/dberr.h"
#include "common/Thread_Share.h"
#include "storage/disk_manager.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"
#include "transaction/log_manager.h"
#include "transaction/lock_manager.h"

extern std::unordered_map<std::string, Thread_Share> global_SharedMap;
extern std::recursive_mutex global_shared_latch;

class DBStorageEngine {
 public:
  explicit DBStorageEngine(std::string db_name, bool init = true, uint32_t buffer_pool_size = DEFAULT_BUFFER_POOL_SIZE)
      : db_name_(db_name), init_(init) {

    db_file_name_ = "../doc/db/" + db_name + ".db";

    // Allocate static page for db storage engine
    if (init) { 
      LogManager* logMgr = nullptr;
      if(USING_LOG)
        logMgr = new LogManager(db_name);
      DiskManager* diskMgr = new DiskManager(db_file_name_, logMgr);
      BufferPoolManager* BPMgr = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, diskMgr, logMgr);
      LockManager* lockMgr = nullptr;

      global_shared_latch.lock();
      disk_mgr_ = diskMgr;
      log_mgr_ =logMgr;
      bpm_ = BPMgr;
      lock_mgr_ = lockMgr;
      global_shared_latch.unlock();

      //initialize meta pages
      page_id_t id_cmeta;
      page_id_t id_iroots;
      // Page *p1 = nullptr, *p2 = nullptr;
      ASSERT(bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Catalog meta page not free.");
      ASSERT(bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Header page not free.");
      if (bpm_->IsPageFree(CATALOG_META_PAGE_ID)) {
        bpm_->NewPage(id_cmeta);
        bpm_->UnpinPage(CATALOG_META_PAGE_ID, true);
      }
      if (bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID)) {
        bpm_->NewPage(id_iroots);
        bpm_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
      }
      // ASSERT(p1 != nullptr && id_cmeta == CATALOG_META_PAGE_ID, "Failed to allocate catalog meta page.");
      // ASSERT(p2 != nullptr && id_iroots == INDEX_ROOTS_PAGE_ID, "Failed to allocate header page.")

      global_shared_latch.lock();
      CatalogManager* cataMgr = new CatalogManager(BPMgr, lockMgr, logMgr, true);
      catalog_mgr_ = cataMgr;
      //insert shared resource map
      global_SharedMap.insert(make_pair(db_name, Thread_Share(diskMgr, BPMgr, logMgr, catalog_mgr_, lock_mgr_)));
      global_shared_latch.unlock();

    } else {
      global_shared_latch.lock();
      disk_mgr_ = global_SharedMap[db_name_].diskMgr_;
      log_mgr_ = global_SharedMap[db_name_].logMgr_;
      bpm_ = global_SharedMap[db_name_].BPMgr_;
      catalog_mgr_ = global_SharedMap[db_name_].cataMgr_;
      lock_mgr_ = global_SharedMap[db_name_].lockMgr_;
      global_shared_latch.unlock();
      // ASSERT(!bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Invalid catalog meta page.");
      // ASSERT(!bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Invalid header page.");
    }

    // Initialize not-shared components
    txn_mgr_ = new TransactionManager(bpm_, disk_mgr_, log_mgr_);

    //do recover if exists
    if(!init)
    {
      if(USING_LOG)
        txn_mgr_->Recover();
    }
    if(!init)
      catalog_mgr_->LoadFromBuffer();
  }

  ~DBStorageEngine() {
    if(USING_LOG)
    {
      bpm_->FlushAll();
      disk_mgr_->FlushAllMeta();
      txn_mgr_->CheckPoint();
    }

    // delete catalog_mgr_;
    // delete lock_mgr_;
    delete txn_mgr_;
  }

 public:
  DiskManager *disk_mgr_;
  BufferPoolManager *bpm_;
  CatalogManager *catalog_mgr_;
  LogManager *log_mgr_;
  LockManager *lock_mgr_;
  TransactionManager *txn_mgr_;

  std::string db_file_name_;
  std::string db_name_;
  bool init_;
};

#endif  // MINISQL_INSTANCE_H
