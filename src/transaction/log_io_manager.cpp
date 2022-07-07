#include "transaction/log_io_manager.h"

LogIOManager::LogIOManager(string log_file_name, bool* exists_file)
{
    log_file_name_ = log_file_name;
    log_io_.open(log_file_name_, std::ios::binary | std::ios::in | std::ios::out);
    // directory or file does not exist
    if (!log_io_.is_open()) {
        log_io_.clear();
        // create a new file
        log_io_.open(log_file_name_, std::ios::binary | std::ios::trunc | std::ios::out);
        log_io_.close();
        // reopen with original mode
        log_io_.open(log_file_name_, std::ios::binary | std::ios::in | std::ios::out);
        if (!log_io_.is_open()) {
            throw std::exception();
        }
        *exists_file = false;
    }
    else
        *exists_file = true;
}

LogIOManager::~LogIOManager()
{
    if(log_io_.is_open())
        log_io_.close();
}

void LogIOManager::ReadData(char* buf, ofs_t ofs, size_t size) //disk[ofs->ofs+size-1] -> buf[0->size-1]
{
    ASSERT(log_io_.is_open(), "log file not open!");
    // set read cursor to offset
    log_io_.seekp(ofs);
    log_io_.read(buf, size);
    // if file ends before reading PAGE_SIZE
    int read_count = log_io_.gcount();
    //cout<<"readcount = "<< read_count <<" readsize = "<<size<<endl;
    ASSERT(read_count==size ,"Log readcount not equal to readsize!\n");
    memset(buf + read_count, 0, size - read_count);
}

void LogIOManager::WriteData(char* buf, ofs_t ofs, size_t size) //buf[0->size-1] -> disk[ofs->ofs+size-1]
{
    ASSERT(log_io_.is_open(), "log file not open!");
    // set write cursor to offset
    log_io_.seekp(ofs);
    log_io_.write(buf, size);
    //check for I/O error
    if (log_io_.bad()) {
        cout<<"[Exception]: Log readcount not equal to readsize!"<<endl;
        return;
    }

    // needs to flush to keep disk file in sync
    log_io_.flush();
}

