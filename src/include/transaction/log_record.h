#ifndef MINISQL_LOG_RECORD_H
#define MINISQL_LOG_RECORD_H

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_set>
#include "common/macros.h"
#include "common/config.h"
#include "transaction/transaction.h"

using namespace std;

class DPT_Record
{
public:
    explicit DPT_Record(page_id_t pid, lsn_t page_lsn, lsn_t rec_lsn):pid_(pid),page_lsn_(page_lsn),rec_lsn_(rec_lsn){}
private:
    page_id_t pid_;
    lsn_t page_lsn_;
    lsn_t rec_lsn_;
};
class DirtyPageTable
{
public:
    void AddRecord(page_id_t pid, lsn_t page_lsn, lsn_t rec_lsn)
    {
        dpt_records_.push_back(DPT_Record(pid, page_lsn, rec_lsn));
    }
private:
    std::vector<DPT_Record> dpt_records_;
};

class ActiveTransactionTable
{
public:
    ActiveTransactionTable()
    {
        // cout<<"att created!"<<endl;
        // tids_.reserve(100);
    }
    void AddTxn(Transaction* txn) {tids_.insert(txn->GetTid()); }
    void DelTxn(Transaction* txn) 
    {
        tids_.erase(txn->GetTid());
    }
    std::unordered_set<txn_id_t>& GetTable(){return tids_; }
private:
    std::unordered_set<txn_id_t> tids_;
};

/*
WRITE: old_data_ != nullptr, new_data_ != nullptr
NEW: old_data_ == nullptr, new_data_ != nullptr
DELETE: old_data_ != nullptr, new_data_ == nullptr
BEGIN/COMMIT/ABORT/CHECKPOINT: old_data_ == nullptr, new_data_ == nullptr
dpt_ != nullptr if and only if type_ = CHECK_POINT
*/
enum LogRecordType{INVALID_RECORD_TYPE, WRITE, NEW, DELETE, BEGIN, COMMIT, ABORT, CHECK_POINT, 
                    BITMAP_WRITE, DISKMETA_WRITE};

class LogRecord {
public:
    explicit LogRecord(lsn_t lsn = INVALID_LSN, txn_id_t tid = INVALID_TXN_ID, LogRecordType type = INVALID_RECORD_TYPE)
    {
        type_ = type;
        lsn_ = lsn;
        tid_ = tid;
        pid_ = INVALID_PAGE_ID;
        old_data_ = nullptr;
        new_data_ = nullptr;
        extend_id_ = INVALID_EXTEND_ID;
        att_ = nullptr;
        dpt_ = nullptr;
    }

    explicit LogRecord(LogRecordType type, lsn_t lsn, txn_id_t tid, page_id_t pid,
         char* old_data, char* new_data, extend_id_t extend_id = INVALID_EXTEND_ID , 
         ActiveTransactionTable* att = nullptr, DirtyPageTable* dpt=nullptr)
        :type_(type), lsn_(lsn), tid_(tid), pid_(pid), extend_id_(extend_id)
    {
        if(old_data == nullptr)
            old_data_ = nullptr;
        else
        {
            old_data_ = new char[PAGE_SIZE];
            memcpy(old_data_, old_data, PAGE_SIZE);
        }
        if(new_data == nullptr)
            new_data_ = nullptr;
        else
        {
            new_data_ = new char[PAGE_SIZE];
            memcpy(new_data_, new_data, PAGE_SIZE);
        }
        if(dpt==nullptr)
            dpt_ = nullptr;
        else
            dpt_ = new DirtyPageTable(*dpt);
        if(att==nullptr)
            att_ = nullptr;
        else
            att_ = new ActiveTransactionTable(*att);
    }
    ~LogRecord()
    {
        delete[] old_data_;
        delete[] new_data_;
        delete dpt_;
        delete att_;
    }

    LogRecordType GetRecordType(){ return type_; }
    lsn_t GetLSN() { return lsn_; }
    txn_id_t GetTid() { return tid_; }
    page_id_t GetPid() { return pid_; }
    extend_id_t GetEid() { return extend_id_; }
    void SetPid(page_id_t pid) { pid_ = pid; }
    char* GetOldData() { return old_data_; }
    char* GetNewData() { return new_data_; }
    ActiveTransactionTable* GetATT() { return att_; }
    DirtyPageTable* GetDPT() { return dpt_; }
    LogRecordType GetType() {return type_;}
    static std::string GetTypeStr(LogRecordType t)
    {
        std::string type = "unknwon";
        if(t==BEGIN)
            type = "begin";
        else if(t==COMMIT)
            type = "commit";
        else if(t==ABORT)
            type = "abort";
        else if(t==CHECK_POINT)
            type = "checkpoint";
        else if(t==NEW)
            type = "new";
        else if(t==WRITE)
            type = "write";
        else if(t==DELETE)
            type = "delete";
        return type;
    }
 
    uint32_t SerializeTo(char* buf) const;
    uint32_t GetSerializedSize() const;
    uint32_t DeSerializeFrom(char* buf);

private:
    LogRecordType type_;
    lsn_t lsn_;
    txn_id_t tid_; //the transaction id of the the log
    page_id_t pid_; //INVALID if no old page
    char *old_data_; //null if no old data
    char *new_data_; //null if no new data
    extend_id_t extend_id_; //used for bitmap page
    ActiveTransactionTable* att_;//null if not a check point
    DirtyPageTable* dpt_;//null if not a check point

    static constexpr uint32_t LOG_MAGIC_NUM = 37182;
};

#endif //MINISQL_LOG_RECORD_H
