#include "index/hash_function.h"

uint32_t HashFunction::MurmurHash3(const void *key, uint32_t len)
{
    const uint32_t m = 378551;
    const int r = 24;
    const int seed = 97;
    uint32_t h = seed ^ len;
    // Mix 4 bytes at a time into the hash
    const unsigned char *data = (const unsigned char *)key;
    while (len >= 4) 
    {
       uint32_t k = *(uint32_t *)data;
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
        data += 4;
        len -= 4;
    }
    // Handle the last few bytes of the input array
    switch (len) 
    {
        case 3:
        h ^= data[2] << 16;
        case 2:
        h ^= data[1] << 8;
        case 1:
        h ^= data[0];
        h *= m;
    };
    // Do a few final mixes of the hash to ensure the last few
    // bytes are well-incorporated.
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

uint32_t HashFunction::SumHash(const void *key, uint32_t len)
{
    uint32_t hash_value = 0;
    for(uint32_t i=0; i<len; i++)
    {
        hash_value += (int)*(reinterpret_cast<const char*>(key) + i);
    }
    return hash_value;
}