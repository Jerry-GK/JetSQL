#ifndef MINISQL_THREAD_SHARE_H
#define MINISQL_THREAD_SHARE_H

#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "transaction/log_manager.h"
#include "catalog/catalog.h"
#include "transaction/lock_manager.h"

class Thread_Share
{
public:
  Thread_Share()
  :diskMgr_(nullptr), BPMgr_(nullptr), logMgr_(nullptr), cataMgr_(nullptr), lockMgr_(nullptr) {}
  Thread_Share(DiskManager* diskMgr, BufferPoolManager* BPMgr, LogManager* logMgr, 
    CatalogManager* cataMgr, LockManager* lockMgr)
  :diskMgr_(diskMgr), BPMgr_(BPMgr), logMgr_(logMgr), cataMgr_(cataMgr), lockMgr_(lockMgr) {}
public:
  DiskManager* diskMgr_;
  BufferPoolManager* BPMgr_;
  LogManager* logMgr_;
  CatalogManager* cataMgr_;
  LockManager* lockMgr_;
};

#endif //MINISQL_THREAD_SHARE_H
