#include "transaction/transaction_manager.h"

void TransactionManager::Recover()
{
    log_mgr_->ShowAllRecords();
    cout<<"---------Doing recover using log----------"<<endl;
    //find lsn of the last checkpoint
    lsn_t last_cp_lsn = INVALID_LSN;
    lsn_t max_lsn = log_mgr_->GetMaxLSN();
    txn_id_t max_tid = INVALID_TXN_ID;
    LogRecord* rec = new LogRecord;
    lsn_t cur_lsn = max_lsn;
    while(cur_lsn!=0)
    {
        log_mgr_->GetRecord(rec, cur_lsn);
        if(rec->GetRecordType()==CHECK_POINT)
        {
            last_cp_lsn = cur_lsn;
            break;
        }
        cur_lsn--;
    }
    ASSERT(rec->GetRecordType()==CHECK_POINT, "not get the checkpoint record!");
    ASSERT(rec->GetATT()!=nullptr, "check point without active transaction table!");

    att_ = new ActiveTransactionTable(*rec->GetATT());

    cout<<"----Analysis Pass----"<<endl;
    //generate undo list and redo list
    unordered_set<txn_id_t> undo_list(att_->GetTable());
    unordered_set<txn_id_t> redo_list;
    cur_lsn = last_cp_lsn;
    while(cur_lsn<=max_lsn)
    {  
        log_mgr_->GetRecord(rec, cur_lsn);
        if(rec->GetRecordType()!=COMMIT&&rec->GetRecordType()!=CHECK_POINT
            &&undo_list.find(rec->GetTid())==undo_list.end()&&rec->GetTid()!=INVALID_TXN_ID)
        {
            undo_list.insert(rec->GetTid());
        }
        if(rec->GetRecordType()==COMMIT)
        {
            if(undo_list.find(rec->GetTid())!=undo_list.end())
                undo_list.erase(rec->GetTid());
            redo_list.insert(rec->GetTid());
        }
        if(rec->GetTid()>max_tid)
            max_tid = rec->GetTid();
        cur_lsn++;
    }
    //show 
    cout<<"[Undo list(tid)]: ";
    for(auto tid : undo_list)
        cout<<tid<<" ";
    cout<<endl;
    cout<<"[Redo list(tid)]: ";
    for(auto tid : redo_list)
        cout<<tid<<" ";
    cout<<endl;
    //scan the log before checkpoint and get the minimum lsn of the record whose tid is in undolist
    //get the maximum tid at the same time
    lsn_t min_undo_lsn = last_cp_lsn;
    cur_lsn = last_cp_lsn - 1;
    //txn_id_t max_tid = 0;
    while(cur_lsn >= 1)
    {
        log_mgr_->GetRecord(rec, cur_lsn);
        if(undo_list.find(rec->GetTid())!=undo_list.end())
            min_undo_lsn = rec->GetLSN();
        if(rec->GetTid()>max_tid)
            max_tid = rec->GetTid();
        cur_lsn--;
    }
    cout<<"min undo lsn = "<<min_undo_lsn<<endl;
    
    //do undo
    cout<<"----Undo Pass----"<<endl;
    cur_lsn = max_lsn;
    while(cur_lsn>=min_undo_lsn)
    {
        log_mgr_->GetRecord(rec, cur_lsn);
        if(undo_list.find(rec->GetTid())!=undo_list.end())
            Undo(rec);
        cur_lsn--;
    }
    //add abort record for each transaction in undo list
    for(auto tid : undo_list)
    {
        LogRecord* append_rec = new LogRecord(log_mgr_->GetMaxLSN()+1, tid, ABORT);
        log_mgr_->AddRecord(append_rec);
        delete append_rec;
        att_->GetTable().erase(tid);
    }

    //do redo
    cout<<"----Redo Pass-----"<<endl;
    cur_lsn = last_cp_lsn;
    while(cur_lsn<=max_lsn)
    {
        log_mgr_->GetRecord(rec, cur_lsn);
        if(redo_list.find(rec->GetTid())!=redo_list.end())
            Redo(rec);
        cur_lsn++;
    }

    //assign next_tid
    next_tid_ = max_tid + 1;
    delete rec;
}

//undo a record
void TransactionManager::Undo(LogRecord* rec)
{
    ASSERT(rec!=nullptr, "Null parameter for Undo!");

    std::cout<<"Undo: ";
    log_mgr_->ShowRecord(rec->GetLSN());

    if(rec->GetType()==WRITE)//undo modification on page
    {
        Page *p = buf_mgr_->FetchPage(rec->GetPid());
        if(p!=nullptr)
        {
            p->CopyBy(rec->GetOldData());
            buf_mgr_->UnpinPage(p->GetPageId(), true);
        }
    }
    else if(rec->GetType()==NEW)//undo new page (delete page)
    {
        buf_mgr_->DeletePage(rec->GetPid());
    }
    else if(rec->GetType()==DELETE)//undo deletion of a page (recreate)
    {
        page_id_t pid = INVALID_PAGE_ID;
        Page *p = buf_mgr_->NewPage(pid);//pid might be different with rec->GetPid(). Will this cause problem?
        if(pid!=rec->GetPid())
        {
            cout<<"different pid after undo deletion of page, new = "<<pid<<" !"<<endl;
            log_mgr_->ReplacePid(rec->GetPid(), pid);
        }
        p->CopyBy(rec->GetOldData());
        buf_mgr_->UnpinPage(p->GetPageId(), true);
    }
}

//redo a record
void TransactionManager::Redo(LogRecord* rec)
{
    ASSERT(rec!=nullptr, "Null parameter for Redo!");

    std::cout<<"Redo: ";
    log_mgr_->ShowRecord(rec->GetLSN());
    if(rec->GetType()==WRITE)//redo modification on page
    {
        Page *p = buf_mgr_->FetchPage(rec->GetPid());
        p->CopyBy(rec->GetNewData());
        buf_mgr_->UnpinPage(p->GetPageId(), true);
    }
    else if(rec->GetType()==NEW)//redo new page (check and recreate if not exists)
    {
        Page *p = buf_mgr_->FetchPage(rec->GetPid());
        if(p==nullptr)//need to recreate
        {
            page_id_t pid = INVALID_PAGE_ID;
            Page *p = buf_mgr_->NewPage(pid);//pid might be different with rec->GetPid(). Will this cause problem?
            if(pid!=rec->GetPid())
                cout<<"different pid after undo deletion of page, new = "<<pid<<" !"<<endl;
            p->CopyBy(rec->GetNewData());
            buf_mgr_->UnpinPage(p->GetPageId(), true);
        }
    }
    else if(rec->GetType()==DELETE)//redo deletion of a page (check and redelete if exists)
    {
        Page *p = buf_mgr_->FetchPage(rec->GetPid());
        if(p!=nullptr)//need to redelete
        {
            buf_mgr_->DeletePage(p->GetPageId());
        }
    }
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

    LogRecord* append_rec = new LogRecord(log_mgr_->GetMaxLSN()+1, txn->GetTid(), BEGIN);
    log_mgr_->AddRecord(append_rec);
    delete append_rec;

    att_->AddTxn(txn);
    return txn;
}

//Commit a transaction.
void TransactionManager::Commit(Transaction *txn)
{
    std::cout<<"txn "<<txn->GetTid()<<" commit"<<std::endl;
    txn->SetState(TransactionState::COMMITTED);

    LogRecord* append_rec = new LogRecord(log_mgr_->GetMaxLSN()+1, txn->GetTid(), COMMIT);
    log_mgr_->AddRecord(append_rec);
    delete append_rec;
    
    att_->DelTxn(txn);
}

//Abort a transaction
void TransactionManager::Abort(Transaction *txn)
{
    std::cout<<"txn "<<txn->GetTid()<<" abort"<<std::endl;
    txn->SetState(TransactionState::ABORTED);

    //use log to rollback
    lsn_t cur_lsn = log_mgr_->GetMaxLSN();
    cout<<"abort txn "<<txn->GetTid()<<endl;
    LogRecord* rec = new LogRecord;
    while(cur_lsn!=0)//scan until the first log record
    {
        log_mgr_->GetRecord(rec, cur_lsn);
        //cout<<"cur_lsn = "<<cur_lsn<<" record tid = "<<rec->GetTid()<<" transaction tid = "<<txn->GetTid()<<endl;
        if(rec->GetTid() == txn->GetTid())//record belongs to transaction txn
        {
            Undo(rec);
        }
        cur_lsn--;
    }
    delete rec;

    LogRecord* append_rec = new LogRecord(log_mgr_->GetMaxLSN()+1, txn->GetTid(), ABORT);
    log_mgr_->AddRecord(append_rec);
    delete append_rec;

    att_->DelTxn(txn);    
}

void TransactionManager::CheckPoint()
{
    LogRecord* append_rec = new LogRecord(CHECK_POINT, log_mgr_->GetMaxLSN()+1, INVALID_TXN_ID, INVALID_PAGE_ID, nullptr, nullptr, att_, nullptr);
    log_mgr_->AddRecord(append_rec);
    delete append_rec;
}