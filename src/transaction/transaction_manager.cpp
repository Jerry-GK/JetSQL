#include "transaction/transaction_manager.h"

//undo a record
void TransactionManager::Undo(Transaction* txn, LogRecord* rec)
{
    ASSERT(txn!=nullptr&&rec!=nullptr, "Null parameter for Undo!");
    ASSERT(txn->GetTid() == rec->GetTid(), "Undo tid not matched!");
    //to be implemented
}

//redo a record
void TransactionManager::Redo(Transaction* txn, LogRecord* rec)
{
    ASSERT(txn!=nullptr&&rec!=nullptr, "Null parameter for Redo!");
    ASSERT(txn->GetTid() == rec->GetTid(), "Redo tid not matched!");
    //to be implemented
}

//begin a transaction
Transaction* TransactionManager::Begin(Transaction *txn)
{
    if(txn==nullptr)
    {
        std::cout<<"txn "<<next_tid_<<" begin"<<std::endl;
        txn = new Transaction(next_tid_++);
        //txn_map_.insert(std::make_pair(next_tid_-1, txn));
    }
    LogRecord* append_rec = new LogRecord(BEGIN);
    log_mgr_->AddRecord(append_rec);
    delete append_rec;

    return txn;
}

//Commit a transaction.
void TransactionManager::Commit(Transaction *txn)
{
    std::cout<<"txn "<<txn->GetTid()<<" commit"<<std::endl;
    txn->SetState(TransactionState::COMMITTED);

    LogRecord* append_rec = new LogRecord(COMMIT);
    log_mgr_->AddRecord(append_rec);
    delete append_rec;
}

//Abort a transaction
void TransactionManager::Abort(Transaction *txn)
{
    std::cout<<"txn "<<txn->GetTid()<<" abort"<<std::endl;
    txn->SetState(TransactionState::ABORTED);

    //using effect(not used)
    //recover page new_pid to old page
    //use reverse iterator to undo reversely
    // for(std::vector<TransactionEffect*>::reverse_iterator rit = txn->effects_.rbegin();rit!=txn->effects_.rend();rit++)
    // {
    //     Page* p = buf_mgr_->FetchPage((*rit)->GetPid());
    //     p->WLatch();
    //     p->CopyBy((*rit)->GetData());
    //     p->WUnlatch();
    //     buf_mgr_->UnpinPage(p->GetPageId(), true);
    // }

    //use log to rollback
    lsn_t cur_lsn = log_mgr_->GetMaxLSN();
    LogRecord* rec = new LogRecord;
    while(cur_lsn!=0)
    {
        log_mgr_->GetRecord(rec, cur_lsn);
        if(rec->GetTid() == txn->GetTid())//record belongs to transaction txn
        {
            Undo(txn, rec);
        }
        cur_lsn--;
    }
    delete rec;

    LogRecord* append_rec = new LogRecord(ABORT);
    log_mgr_->AddRecord(append_rec);
    delete append_rec;

    // txn_map.erase(txn->GetTid());
    // delete txn;
}
