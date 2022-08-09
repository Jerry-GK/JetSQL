#include "transaction/log_record.h"

uint32_t LogRecord::SerializeTo(char* buf) const
{
    char *buf_head = buf;
    uint32_t ofs=0;

    MACH_WRITE_TO(uint32_t, buf, LOG_MAGIC_NUM);
    ofs += sizeof(uint32_t);
    buf = buf_head + ofs;   

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

    bool old_data_exists = false;
    if(old_data_!=nullptr)
        old_data_exists = true;
    MACH_WRITE_TO(bool, buf, old_data_exists);
    ofs += sizeof(bool);
    buf = buf_head + ofs;

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
    ofs += sizeof(bool);
    buf = buf_head + ofs;

    if(new_data_exists)
    {       
        MACH_WRITE_TO_FROM(buf, new_data_, PAGE_SIZE);
        ofs += PAGE_SIZE;
        buf = buf_head + ofs;
    }

    //information about active transaction table
    bool att_exists = false;
    if(att_!=nullptr)
        att_exists = true;
    MACH_WRITE_TO(bool, buf, att_exists);
    ofs += sizeof(bool);
    buf = buf_head + ofs;

    if(att_exists)
    {
        uint32_t txn_num = att_->GetTable().size();
        MACH_WRITE_TO(uint32_t, buf, txn_num);
        ofs += sizeof(uint32_t);
        buf = buf_head + ofs;

        for(auto tid : att_->GetTable())
        {
            MACH_WRITE_TO(txn_id_t, buf, tid);
            ofs += sizeof(txn_id_t);
            buf = buf_head + ofs;
        }
    }

    //dpt not concerned yet
    return ofs;
}

uint32_t LogRecord::GetSerializedSize() const
{
    uint32_t ofs = 0;
    ofs += sizeof(LOG_MAGIC_NUM);
    ofs += sizeof(LogRecordType);
    ofs += sizeof(lsn_t);
    ofs += sizeof(txn_id_t);
    ofs += sizeof(page_id_t);
    ofs += sizeof(bool);
    if(old_data_!=nullptr)
        ofs += PAGE_SIZE;
    ofs += sizeof(bool);
    if(new_data_!=nullptr)
        ofs += PAGE_SIZE;
    ofs += sizeof(bool);
    
    if(att_!=nullptr)
        ofs += sizeof(uint32_t) + att_->GetTable().size()*sizeof(txn_id_t);
    return ofs;
}

uint32_t LogRecord::DeSerializeFrom(char* buf)
{
    /* deserialize field from buf */
    uint32_t ofs = 0;
    char* buf_head = buf;

    uint32_t mag_num = MACH_READ_FROM(uint32_t, buf);
    ASSERT(mag_num == LOG_MAGIC_NUM, "Deserialize at incorrect position!");
    ofs += sizeof(uint32_t);
    buf = buf_head + ofs;

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
        new_data_ = new char[PAGE_SIZE];
        memcpy(new_data_, buf, PAGE_SIZE);
        ofs += PAGE_SIZE;
        buf = buf_head + ofs;
    }
    else
        new_data_ = nullptr;

    bool att_exists = MACH_READ_FROM(bool, buf);
    ofs += sizeof(bool);
    buf = buf_head + ofs;

    if(att_exists)
    {
        uint32_t txn_num = MACH_READ_FROM(uint32_t, buf);
        ofs += sizeof(uint32_t);
        buf = buf_head + ofs;
        
        att_ = new ActiveTransactionTable;

        for(int i = 0;i<txn_num;i++)
        {
            txn_id_t tid = INVALID_TXN_ID;
            tid = MACH_READ_FROM(txn_id_t, buf);
            att_->GetTable().insert(tid);
            ofs += sizeof(txn_id_t);
            buf = buf_head + ofs;
        }
    }
    else
    {
        delete att_;
        att_ = nullptr;
    }

    //dpt not concerned yet
    return ofs;
}