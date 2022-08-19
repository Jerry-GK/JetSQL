#ifndef MINISQL_HASH_FUNCTION_H
#define MINISQL_HASH_FUNCTION_H

#include <cstdint>
#include "index/index_key.h"

class HashFunction 
{
 private:
  uint32_t MurmurHash3(const void *key, unsigned int len);
  uint32_t SumHash(const void *key, unsigned int len);

 public:
  /**
   * @param key the key to be hashed
   * @return the hashed value
   */
  virtual uint32_t GetHash(const IndexKey* key) {
    
    uint32_t hash_value = MurmurHash3(reinterpret_cast<const void *>(key->value), key->keysize);
    return hash_value;
  }
};


#endif //MINISQL_HASH_FUNCTION_H