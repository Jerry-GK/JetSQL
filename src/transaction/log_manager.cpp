#include "transaction/log_manager.h"

LogManager::LogManager(string db_name)
{
    log_file_name_ = "../doc/log/log_"+db_name+".txt";
    log_io_.open(log_file_name_, std::ios::in);
    if (!log_io_.is_open()) {
        // create the meta file
        log_io_.open(log_file_name_, std::ios::out);
        log_io_.close();
        return;
    }
}

void LogManager::write(string record)
{
    log_io_.open(log_file_name_, std::ios::app);
    if(!log_io_.is_open())
    {
        std::cout << "[Exception]: No log file \"" + log_file_name_ + "\"!\n";
        return;
    }
    log_io_<<record<<std::endl;
    log_io_.close();
    return;
}