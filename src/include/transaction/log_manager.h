#ifndef MINISQL_LOG_MANAGER_H
#define MINISQL_LOG_MANAGER_H

#include <iostream>
#include <fstream>
#include "transaction/log_record.h"

using namespace std;

class LogManager {
public:
    LogManager(string db_name);
    void AddRecord(LogRecord* record);//add a record into stable storage(disk) directly
    string GetLogFileName(){ return log_file_name_; }
    void GetRecord(LogRecord* log_rec, lsn_t lsn);
    lsn_t GetMaxLSN();

private:
    char buf[3*PAGE_SIZE];//buffer for interaction with disk
    string log_file_name_;
    fstream log_io_;
};

#endif //MINISQL_LOG_MANAGER_H
