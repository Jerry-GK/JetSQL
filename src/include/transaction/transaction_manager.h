#ifndef MINISQL_TRANSACTION_MANAGER_H
#define MINISQL_TRANSACTION_MANAGER_H

#include <unordered_map>
#include <unordered_set>
#include "transaction/transaction.h"
#include "buffer/buffer_pool_manager.h"
#include "transaction/log_manager.h"
#include "storage/disk_manager.h"

class TransactionManager {
public:
    explicit TransactionManager(BufferPoolManager* buf_mgr, DiskManager* disk_mgr, LogManager* log_mgr):
        next_tid_(0),buf_mgr_(buf_mgr), disk_mgr_(disk_mgr), log_mgr_(log_mgr)
        {
            att_ = new ActiveTransactionTable;
        }

    ~TransactionManager()
    {
        // for(auto txn : this->txn_map_)
        // {
        //     delete txn.second;
        // }
        delete att_;
    }

    //do recover
    void Recover();

    //begin a transaction
    Transaction *Begin(Transaction *txn = nullptr);
    
    //Commit a transaction.
    void Commit(Transaction *txn);

    //Abort a transaction
    void Abort(Transaction *txn);

    //undo a record
    void Undo(LogRecord* rec);
    
    //redo a record
    void Redo(LogRecord* rec);

    //checkpoint
    void CheckPoint();

private:
    txn_id_t next_tid_;
    ActiveTransactionTable* att_;
    //The transaction map is a global list of all the running transactions in the system
    //std::unordered_map<txn_id_t, Transaction *> txn_map_;
    BufferPoolManager* buf_mgr_;
    DiskManager* disk_mgr_;
    LogManager* log_mgr_;
};


#endif  // MINISQL_TRANSACTION_MANAGER_H
