#ifndef THREAD_SHARE_H
#define THREAD_SHARE_H

#include "storage/disk_manager.h"
#include "buffer/buffer_pool_manager.h"
#include "transaction/log_manager.h"

class Thread_Share
{
public:
  Thread_Share()
  :diskMgr_(nullptr), BPMgr_(nullptr), logMgr_(nullptr){}
  Thread_Share(DiskManager* diskMgr, BufferPoolManager* BPMgr, LogManager* logMgr)
  :diskMgr_(diskMgr), BPMgr_(BPMgr), logMgr_(logMgr){}
public:
  DiskManager* diskMgr_;
  BufferPoolManager* BPMgr_;
  LogManager* logMgr_;
};

#endif //THREAD_SHARE_H
