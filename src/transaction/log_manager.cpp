#include "transaction/log_manager.h"

LogManager::LogManager(string db_name)
{
    log_file_name_ = "../doc/log/"+db_name + ".log";
    log_io_.open(log_file_name_, std::ios::in);
    if (!log_io_.is_open()) {
        // create the meta file
        log_io_.open(log_file_name_, std::ios::out);
        log_io_.close();
        return;
    }
}

void LogManager::AddRecord(LogRecord* record)
{
    //increase lsn 
    LogRecord::next_lsn_++;
    //open file
    log_io_.open(log_file_name_, std::ios::app);
    if(!log_io_.is_open())
    {
        std::cout << "[Exception]: No log file \"" + log_file_name_ + "\"!\n";
        return;
    }
    //serialize to buf
    record->SerializeTo(buf);
    //append the content of buf to disk, update meta message
    //??
    //to be implemented
    log_io_.close();

    return;
}

void LogManager::GetRecord(LogRecord* log_rec, lsn_t lsn)
{
    //buf <- right position for record<lsn>
    //??
    //to be implemented

    log_rec->DeSerializeFrom(buf);
}

lsn_t LogManager::GetMaxLSN()
{
    //buf <- right position
    //??
    //to be implemented

    return 0;
}