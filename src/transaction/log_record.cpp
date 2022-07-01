#include "transaction/log_record.h"

lsn_t LogRecord::next_lsn_ = 0;

uint32_t LogRecord::SerializeTo(char* buf) const
{
    char *buf_head = buf;
    uint32_t ofs=0;
    MACH_WRITE_TO(LogRecordType, buf, type_);
    ofs += sizeof(LogRecordType);
    buf = buf_head + ofs;

    MACH_WRITE_TO(lsn_t, buf, lsn_);
    ofs += sizeof(lsn_t);
    buf = buf_head + ofs;

    MACH_WRITE_TO(txn_id_t, buf, tid_);
    ofs += sizeof(txn_id_t);
    buf = buf_head + ofs;    

    MACH_WRITE_TO(page_id_t, buf, pid_);
    ofs += sizeof(page_id_t);
    buf = buf_head + ofs;

    MACH_WRITE_TO(lsn_t, buf, lsn_);
    ofs += sizeof(lsn_t);
    buf = buf_head + ofs;

    bool old_data_exists = false;
    if(old_data_!=nullptr)
        old_data_exists = true;
    MACH_WRITE_TO(bool, buf, old_data_exists);
    if(old_data_exists)
    {       
        MACH_WRITE_TO_FROM(buf, old_data_, PAGE_SIZE);
        ofs += PAGE_SIZE;
        buf = buf_head + ofs;
    }

    bool new_data_exists = false;
    if(new_data_!=nullptr)
        new_data_exists = true;
    MACH_WRITE_TO(bool, buf, new_data_exists);
    if(new_data_exists)
    {       
        MACH_WRITE_TO_FROM(buf, new_data_, PAGE_SIZE);
        ofs += PAGE_SIZE;
        buf = buf_head + ofs;
    }
    return ofs;
}

uint32_t LogRecord::GetSerializedSize() const //no use yet
{
    return 0;
}

uint32_t LogRecord::DeSerializeFrom(char* buf)
{
    /* deserialize field from buf */
    uint32_t ofs = 0;
    char* buf_head = buf;
    type_ = MACH_READ_FROM(LogRecordType, buf);
    ofs += sizeof(LogRecordType);
    buf = buf_head + ofs;

    lsn_ = MACH_READ_FROM(lsn_t, buf);
    ofs += sizeof(lsn_t);
    buf = buf_head + ofs;

    tid_ = MACH_READ_FROM(txn_id_t, buf);
    ofs += sizeof(txn_id_t);
    buf = buf_head + ofs;

    pid_ = MACH_READ_FROM(page_id_t, buf);
    ofs += sizeof(page_id_t);
    buf = buf_head + ofs;
 
    bool old_data_exist = MACH_READ_FROM(bool, buf);
    ofs += sizeof(bool);
    buf = buf_head + ofs;
    if(old_data_exist)
    {
        uint32_t size = PAGE_SIZE;
        ASSERT(old_data_ == nullptr, "deserialize to an non-empty log record for old data!");
        old_data_ = new char[PAGE_SIZE];
        memcpy(old_data_, buf, PAGE_SIZE);
        ofs += PAGE_SIZE;
        buf = buf_head + ofs;
    }
    else
        old_data_ = nullptr;

    bool new_data_exist = MACH_READ_FROM(bool, buf);
    ofs += sizeof(bool);
    buf = buf_head + ofs;
    if(new_data_exist)
    {
        uint32_t size = PAGE_SIZE;
        ASSERT(new_data_ == nullptr, "deserialize to an non-empty log record for new data!");
        new_data_ = new char[PAGE_SIZE];
        memcpy(new_data_, buf, PAGE_SIZE);
        ofs += PAGE_SIZE;
        buf = buf_head + ofs;
    }
    else
        new_data_ = nullptr;

    return ofs;
}