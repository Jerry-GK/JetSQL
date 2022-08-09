#include "transaction/log_manager.h"

LogManager::LogManager(string db_name)
{
    string log_file_name = "../doc/log/"+db_name + ".log";
    bool exists_file = false;
    log_io_mgr_ = new LogIOManager(log_file_name, &exists_file);
    //initialize the log file
    if(!exists_file)
    {
        lsn_t log_num = 0;
        MACH_WRITE_TO(lsn_t, meta_buf, log_num);
        log_io_mgr_->WriteData(meta_buf, 0, LOG_NUM_SIZE);
    }
}

void LogManager::AddRecord(LogRecord* record)
{
    //serialize to buf
    record->SerializeTo(log_buf);
    size_t new_record_size = record->GetSerializedSize();

    //get the log_num(max lsn)
    log_io_mgr_->ReadData(meta_buf, 0, LOG_NUM_SIZE);
    lsn_t log_num = *reinterpret_cast<lsn_t*> (meta_buf);
    //get the ofs for new log record
    ofs_t cur_ofs = DATA_OFFSET;
    if(log_num >= 1)
    {
        log_io_mgr_->ReadData(meta_buf, META_OFFSET+(log_num-1)*(OFS_SIZE+SIZE_SIZE), OFS_SIZE);
        ofs_t last_ofs = *reinterpret_cast<ofs_t*> (meta_buf);
        log_io_mgr_->ReadData(meta_buf, META_OFFSET+(log_num-1)*(OFS_SIZE+SIZE_SIZE)+OFS_SIZE, SIZE_SIZE);
        ofs_t last_size = *reinterpret_cast<size_t*> (meta_buf);
        cur_ofs = last_ofs + last_size;
    }
    //write ofs,size message
    MACH_WRITE_TO(ofs_t, meta_buf, cur_ofs);
    MACH_WRITE_TO(size_t, meta_buf+sizeof(ofs_t), new_record_size);
    log_io_mgr_->WriteData(meta_buf, META_OFFSET+log_num*(OFS_SIZE+SIZE_SIZE), OFS_SIZE+SIZE_SIZE);

    //write log
    log_io_mgr_->WriteData(log_buf, cur_ofs, new_record_size);

    //increase lsn
    log_num++;
    MACH_WRITE_TO(lsn_t, meta_buf, log_num);
    log_io_mgr_->WriteData(meta_buf, 0, LOG_NUM_SIZE);

    //output
    string type = "unknwon";
    LogRecordType t = record->GetRecordType();
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
    cout<<"Add log record: < lsn = "<<record->GetLSN()<<",  tid = "<<record->GetTid()<<",  pid = "<<record->GetPid()
    <<",  type = "<<type<<" >"<<endl;
    return;
}

void LogManager::GetRecord(LogRecord* log_rec, lsn_t lsn)
{
    //get log offset and size
    log_io_mgr_->ReadData(meta_buf, META_OFFSET+(lsn-1)*(OFS_SIZE+SIZE_SIZE), OFS_SIZE);
    ofs_t log_ofs = MACH_READ_FROM(ofs_t, meta_buf);
    log_io_mgr_->ReadData(meta_buf, META_OFFSET+(lsn-1)*(OFS_SIZE+SIZE_SIZE)+OFS_SIZE, SIZE_SIZE);
    size_t log_size = MACH_READ_FROM(size_t, meta_buf);

    //read the log record
    log_io_mgr_->ReadData(log_buf, log_ofs, log_size);
    log_rec->DeSerializeFrom(log_buf);

    //output
    string type = "unknwon";
    LogRecordType t = log_rec->GetRecordType();
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
    // cout<<"Get log record: <lsn = "<<lsn<<",  tid = "<<log_rec->GetTid()<<",  pid = "<<log_rec->GetPid()
    //     <<",  type = "<<type<<">"<<endl;
}

void LogManager::ShowRecord(lsn_t lsn)
{
    LogRecord* log_rec = new LogRecord;
    //get log offset and size
    log_io_mgr_->ReadData(meta_buf, META_OFFSET+(lsn-1)*(OFS_SIZE+SIZE_SIZE), OFS_SIZE);
    ofs_t log_ofs = MACH_READ_FROM(ofs_t, meta_buf);
    log_io_mgr_->ReadData(meta_buf, META_OFFSET+(lsn-1)*(OFS_SIZE+SIZE_SIZE)+OFS_SIZE, SIZE_SIZE);
    size_t log_size = MACH_READ_FROM(size_t, meta_buf);

    //read the log record
    log_io_mgr_->ReadData(log_buf, log_ofs, log_size);
    log_rec->DeSerializeFrom(log_buf);

    //output
    string type = "unknwon";
    LogRecordType t = log_rec->GetRecordType();
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
    cout<<"Log record: < lsn = "<<lsn<<",  tid = "<<log_rec->GetTid()<<",  pid = "<<log_rec->GetPid()
        <<",  type = "<<type<<" >"<<endl;
    delete log_rec;
}

lsn_t LogManager::GetMaxLSN()
{
    log_io_mgr_->ReadData(meta_buf, 0, LOG_NUM_SIZE);
    lsn_t log_num = MACH_READ_FROM(lsn_t, meta_buf);
    return log_num;
}

void LogManager::ShowAllRecords()
{
    cout<<"All records in log:"<<endl;
    lsn_t cur_lsn = 1;
    LogRecord* rec = new LogRecord;
    while(cur_lsn<=GetMaxLSN())
    {
        ShowRecord(cur_lsn);
        cur_lsn++;
    }
    cout<<endl;
    delete rec;
}