#include <iostream>
#include <fstream>
#include "common/macros.h"
using namespace std;
typedef uint32_t ofs_t;

class LogIOManager
{
public:
    explicit LogIOManager(string log_file_name, bool* exists_file);
    ~LogIOManager();

    string GetLogFileName(){ return log_file_name_; }
    void ReadData(char* buf, ofs_t ofs, size_t size); //disk[ofs->ofs+size-1] -> buf[0->size-1]
    void WriteData(char* buf, ofs_t ofs, size_t size); //buf[0->size-1] -> disk[ofs->ofs+size-1]

private:
    string log_file_name_;
    fstream log_io_;
};