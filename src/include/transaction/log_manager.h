#ifndef MINISQL_LOG_MANAGER_H
#define MINISQL_LOG_MANAGER_H

#include <iostream>
#include <fstream>

using namespace std;

/**
 * LogManager maintains a separate thread that is awakened whenever the
 * log buffer is full or whenever a timeout happens.
 * When the thread is awakened, the log buffer's content is written into the disk log file.
 *
 * Implemented by student self
 */
class LogManager {
public:
    LogManager();
    void write(string record);

private:
    string log_file_name_;
    fstream log_io_;
};

#endif //MINISQL_LOG_MANAGER_H
