#ifndef MINISQL_INSTANCE_H
#define MINISQL_INSTANCE_H

#include <iostream>
#include <memory>
#include <string>
#include "buffer/buffer_pool_manager.h"
#include "catalog/catalog.h"
#include "common/config.h"
#include "common/dberr.h"
#include "storage/disk_manager.h"
#include "transaction/transaction.h"
#include "transaction/transaction_manager.h"
#include "transaction/log_manager.h"
#include "transaction/lock_manager.h"

class DBStorageEngine {
 public:
  explicit DBStorageEngine(std::string db_name, bool init = true, uint32_t buffer_pool_size = DEFAULT_BUFFER_POOL_SIZE)
      : db_name_(db_name), init_(init) {
    // Init database file if needed
    // if (init_) {
    //   remove(db_name_.c_str());
    // }
    
    db_file_name_ = "../doc/db/" + db_name + ".db";

    disk_mgr_ = new DiskManager(db_file_name_);
    log_mgr_ = new LogManager(db_name_);
    bpm_ = new BufferPoolManager(buffer_pool_size, disk_mgr_, log_mgr_);

    // Allocate static page for db storage engine
    if (init) {  // strange assert bugs
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
      // ASSERT(p2 != nullptr && id_iroots == INDEX_ROOTS_PAGE_ID, "Failed to allocate header page.");
    } else {
      ASSERT(!bpm_->IsPageFree(CATALOG_META_PAGE_ID), "Invalid catalog meta page.");
      ASSERT(!bpm_->IsPageFree(INDEX_ROOTS_PAGE_ID), "Invalid header page.");
    }

    // Initialize components
    lock_mgr_ = nullptr;
    txn_mgr_ = new TransactionManager(bpm_, log_mgr_);
    catalog_mgr_ = new CatalogManager(bpm_, lock_mgr_, log_mgr_, init); 
    //do recover if exists
    if(!init)
    {
      txn_mgr_->Recover();
      catalog_mgr_->LoadFromBuffer();
    }
  }

  ~DBStorageEngine() {
    delete catalog_mgr_;
    txn_mgr_->CheckPoint();

    delete bpm_;
    delete disk_mgr_;
    
    // delete log_mgr_;
    // delete lock_mgr_;
    // delete txn_mgr_;
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
