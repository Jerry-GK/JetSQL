#ifndef MINISQL_LOG_MANAGER_H
#define MINISQL_LOG_MANAGER_H

#include <iostream>
#include "transaction/log_record.h"
#include "transaction/log_io_manager.h"

using namespace std;

class LogManager {
public:
    explicit LogManager(string db_name);
    ~LogManager()
    {
        delete log_io_mgr_;
    }

    void AddRecord(LogRecord* record);//add a record into stable storage(disk) directly
    string GetLogFileName(){ return log_io_mgr_->GetLogFileName(); }
    void GetRecord(LogRecord* log_rec, lsn_t lsn);
    lsn_t GetMaxLSN();  
    void ShowAllRecords();
    void ShowRecord(lsn_t lsn);
    
    void ReplacePid(pid_t old_pid, pid_t new_pid);

    // static lsn_t next_lsn_;//initialized to be 0

    //log structure in disk (assume MAX_RECORD_NUMBER = 4096):
    //| LOG NUM(MAXLSN) | OFS0 | SIZE0 | OFS1 | SIZE1 | ... | OFS4096 | SIZE4096 | LOG0 |     LOG 1    | ... | LOG 4096 |
    //                  |                                                        |
    //              META_OFFSET                                              DATA_OFFSET
    //OFS+SIZE is of fixed length; LOG is of variant length 
    static constexpr size_t LOG_NUM_OFFSET = 0;
    static constexpr size_t LOG_NUM_SIZE = sizeof(lsn_t);
    static constexpr size_t META_OFFSET = LOG_NUM_SIZE;
    static constexpr size_t OFS_SIZE = sizeof(ofs_t);
    static constexpr size_t SIZE_SIZE = sizeof(size_t);
    static constexpr size_t MAX_REC_NUM = MAX_RECORD_NUMBER;
    static constexpr size_t DATA_OFFSET = META_OFFSET + MAX_REC_NUM * (OFS_SIZE + SIZE_SIZE);

private:
    char log_buf[3*PAGE_SIZE];//buffer for interaction with disk
    char meta_buf[1024];//meta buffer
    LogIOManager* log_io_mgr_;
};

#endif //MINISQL_LOG_MANAGER_H
