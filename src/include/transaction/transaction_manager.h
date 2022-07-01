#ifndef MINISQL_TRANSACTION_MANAGER_H
#define MINISQL_TRANSACTION_MANAGER_H

#include <unordered_map>
#include "transaction/transaction.h"
#include "buffer/buffer_pool_manager.h"
#include "transaction/log_manager.h"

class TransactionManager {
public:
    explicit TransactionManager(BufferPoolManager* buf_mgr, LogManager* log_mgr):
        next_tid_(0),buf_mgr_(buf_mgr),log_mgr_(log_mgr){}

    ~TransactionManager()
    {
        // for(auto txn : this->txn_map_)
        // {
        //     delete txn.second;
        // }
    }

    //begin a transaction
    Transaction *Begin(Transaction *txn = nullptr);
    
    //Commit a transaction.
    void Commit(Transaction *txn);

    //Abort a transaction
    void Abort(Transaction *txn);

    //undo a record
    void Undo(Transaction* txn, LogRecord* rec);
    
    //redo a record
    void Redo(Transaction* txn, LogRecord* rec);

private:
    txn_id_t next_tid_;
    //The transaction map is a global list of all the running transactions in the system
    //std::unordered_map<txn_id_t, Transaction *> txn_map_;
    BufferPoolManager* buf_mgr_;
    LogManager* log_mgr_;
};


#endif  // MINISQL_TRANSACTION_MANAGER_H
