#ifndef MINISQL_HASH_FUNCTION_H
#define MINISQL_HASH_FUNCTION_H

#include <cstdint>
#include "index/index_key.h"

class HashFunction {
 public:
  /**
   * @param key the key to be hashed
   * @return the hashed value
   */
  virtual uint64_t GetHash(const IndexKey* key) {
    //uint64_t hash[2] = {0, 0};
    //murmur3::MurmurHash3_x64_128(reinterpret_cast<const void *>(&key), static_cast<int>(sizeof(KeyType)), 0,
                                 //reinterpret_cast<void *>(&hash));
    //return hash[0];

    uint64_t hash = 0;
    for(int i=0;i<key->keysize;i++)
    {
      hash += (uint64_t)key->value[i];
    }
    return hash;
  }
};


#endif //MINISQL_HASH_FUNCTION_H