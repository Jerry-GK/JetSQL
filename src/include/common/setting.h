#ifndef MINISQL_SETTING_H
#define MINISQL_SETTING_H

#include <cstdint>
#include <cstring>

//DBMS settings that can be changed according to needs

// add DBMS mode
//SAFE: using log for recovery and transaction rollback
//FAST: does not support recovery and transaction, but faster performance without writing log
enum DBMS_MODE {FAST, SAFE};

//replacer type
enum REPLACER_TYPE {LRU, CLOCK};

//index type
enum INDEX_TYPE {BPTREE, HASH};

static bool LATER_INDEX_AVAILABLE = true;//if building an index on an non empty table is allowed
static DBMS_MODE CUR_DBMS_MODE = FAST;
static bool USING_LOG = (CUR_DBMS_MODE!=FAST);
static REPLACER_TYPE CUR_REPLACER_TYPE = LRU;
static INDEX_TYPE DEFAULT_INDEX_TYPE = BPTREE;

static uint32_t THREAD_NUM = 1; //multithread has some concurrency problems yet
static bool DO_PAGE_LATCH = true;

#endif  // MINISQL_SETTING_H
