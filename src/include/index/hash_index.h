#ifndef MINISQL_HASH_INDEX_H
#define MINISQL_HASH_INDEX_H

#include <cstddef>
#include "common/rowid.h"
#include "index/index_key.h"
#include "buffer/buffer_pool_manager.h"
#include "index/index.h"

//using IndexEntry = BLeafEntry;

class HashIndex : public Index {
 public:

};

#endif  // MINISQL_HASH_INDEX_H
