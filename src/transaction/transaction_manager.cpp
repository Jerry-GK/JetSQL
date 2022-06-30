#include "transaction/transaction_manager.h"

//begin a transaction
Transaction* TransactionManager::Begin(Transaction *txn)
{
    if(txn==nullptr)
    {
        std::cout<<"txn "<<next_tid_<<" begin"<<std::endl;
        txn = new Transaction(next_tid_++);
        txn_map_.insert(std::make_pair(next_tid_-1, txn));
    }
    return txn;
}

//Commit a transaction.
void TransactionManager::Commit(Transaction *txn)
{
    std::cout<<"txn "<<txn->GetTid()<<" commit"<<std::endl;
    txn->SetState(TransactionState::COMMITTED);

    // txn_map.erase(txn->GetTid());
    // delete txn;
}

//Abort a transaction
void TransactionManager::Abort(Transaction *txn)
{
    std::cout<<"txn "<<txn->GetTid()<<" abort"<<std::endl;
    txn->SetState(TransactionState::ABORTED);

    //recover page new_pid to old page
    //use reverse iterator to undo reversely
    for(std::vector<TransactionEffect*>::reverse_iterator rit = txn->effects_.rbegin();rit!=txn->effects_.rend();rit++)
    {
        Page* p = buf_mgr_->FetchPage((*rit)->GetPid());
        p->WLatch();
        p->CopyBy((*rit)->GetData());
        p->WUnlatch();
        buf_mgr_->UnpinPage(p->GetPageId(), true);
    }

    // txn_map.erase(txn->GetTid());
    // delete txn;
}
