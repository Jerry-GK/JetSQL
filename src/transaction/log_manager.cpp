#include "transaction/log_manager.h"

LogManager::LogManager()
{
    log_file_name_ = "../doc/log/minisql_log_0.txt";
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
        std::cout << "[Exception]: No log file!\n";
        return;
    }
    log_io_<<record<<std::endl;
    log_io_.close();
    return;
}