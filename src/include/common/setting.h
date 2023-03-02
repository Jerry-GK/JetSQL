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


static constexpr uint32_t THREAD_MAXNUM = 1; //maybe multithread
static constexpr bool DO_PAGE_LATCH = true; 
static constexpr bool USING_EXE_LATCH = true; //executor latch(low concurrency but safe)
static constexpr bool TEST_CONC = false && (THREAD_MAXNUM > 1);

#endif  // MINISQL_SETTING_H
