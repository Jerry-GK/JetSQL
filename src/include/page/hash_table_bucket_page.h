#ifndef MINISQL_HASH_TABLE_BUCKET_PAGE_H
#define MINISQL_HASH_TABLE_BUCKET_PAGE_H

#include <cstddef>
#include <cstring>
#include <utility>
#include <vector>

#include "index/index_key.h"
#include "page/page.h"

/**
 * Store indexed key and and value together within bucket page. Supports
 * non-unique keys.
 *
 * Bucket page format (keys are stored in order):
 *  ---------------------------------------
 * | key_size_ | SLOT[1] | ...... | SLOT[N]
 *  ---------------------------------------
 * SLOT[i] = KEY[i] + VALUE[i] + OCCUPIED[i] + READABLE[i]
 * 
 *  Here '+' means concatenation.
 *  The above format omits the space required for the occupied_ and
 *  readable_ arrays. More information is in storage/page/hash_table_page_defs.h.
 *
 */

struct HashSlot {
  bool occupied;
  bool readable;
  RowId value;
  IndexKey key;
  HashSlot(IndexKey *k, RowId v) {
    value = v;
    key.keysize = k->keysize;
    memcpy(key.value, k->value, k->keysize);
  }
  HashSlot(HashSlot &another) {
    value = another.value;
    key.keysize = another.key.keysize;
    memcpy(key.value, another.key.value, key.keysize);
  }
  HashSlot &operator=(HashSlot &another) {
    value = another.value;
    key.keysize = another.key.keysize;
    memcpy(key.value, another.key.value, key.keysize);
    return *this;
  }
  void SetKey(const IndexKey *k) {
    key.keysize = k->keysize;
    memcpy(key.value, k->value, k->keysize);
  }
  void SetValue(RowId r) { value = r; }
  void SetOccupied(bool val) { occupied = val; }
  void SetReadable(bool val) { readable = val; } 
};

//implementation without template
class HashTableBucketPage {
public:
  // Delete all constructor / destructor to ensure memory safety
  HashTableBucketPage() = delete;

  //set the key size
  void Init(key_size_t key_size);

  /**
   * Scan the bucket and collect values that have the matching key
   *
   * @return true if at least one key matched
   */
  bool GetValue(const IndexKey* key, IndexKeyComparator cmp, std::vector<RowId> *result);

  /**
   * Attempts to insert a key and value in the bucket.  Uses the occupied_
   * and readable_ arrays to keep track of each slot's availability.
   *
   * @param key key to insert
   * @param value value to insert
   * @return true if inserted, false if duplicate KV pair or bucket is full
   */
  bool Insert(const IndexKey* key, RowId value, IndexKeyComparator cmp);

  /**
   * Removes a key and value.
   *
   * @return true if removed, false if not found
   */
  bool Remove(const IndexKey* key, RowId value, IndexKeyComparator cmp);

  /**
   * Gets the key at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the key at
   * @return key at index bucket_idx of the bucket
   */
  const IndexKey* KeyAt(uint32_t bucket_idx) const;

  /**
   * Gets the value at an index in the bucket.
   *
   * @param bucket_idx the index in the bucket to get the value at
   * @return value at index bucket_idx of the bucket
   */
  RowId ValueAt(uint32_t bucket_idx) const;

  /**
   * Remove the KV pair at bucket_idx
   */
  void RemoveAt(uint32_t bucket_idx);

  /**
   * Returns whether or not an index is occupied (key/value pair or tombstone)
   *
   * @param bucket_idx index to look at
   * @return true if the index is occupied, false otherwise
   */
  bool IsOccupied(uint32_t bucket_idx) const;

  /**
   * SetOccupied - Updates the bitmap to indicate that the entry at
   * bucket_idx is occupied.
   *
   * @param bucket_idx the index to update
   * @param bit the bit that you want to set, 0 or 1
   */
  void SetOccupied(uint32_t bucket_idx, int bit);

  /**
   * Returns whether or not an index is readable (valid key/value pair)
   *
   * @param bucket_idx index to lookup
   * @return true if the index is readable, false otherwise
   */
  bool IsReadable(uint32_t bucket_idx) const;

  /**
   * SetReadable - Updates the bitmap to indicate that the entry at
   * bucket_idx is readable.
   *
   * @param bucket_idx the index to update
   * @param bit the bit that you want to set, 0 or 1
   */
  void SetReadable(uint32_t bucket_idx, int bit);

  /**
   * @return the number of readable elements, i.e. current size
   */
  uint32_t NumReadable();

  /**
   * @return whether the bucket is full
   */
  bool IsFull();

  /**
   * @return whether the bucket is empty
   */
  bool IsEmpty();

  /**
   * Prints the bucket's occupancy information
   */
  void PrintBucket();


  /**
   * Set the bit at the specified position
   *
   * @param which which bit you want to update
   * @param bit the bit yout want to set, 0 or 1
   */
  char GetMask(int which, int bit) const {
    char mask = 0;
    if (bit == 0) {
      mask = static_cast<char>(~(1 << which));
    } else {
      mask = static_cast<char>(1 << which);
    }
    return mask;
  }

  //what is the index key size?
  uint32_t GetIndexKeySize() const
  {
    return sizeof(IndexKey) + key_size_;
  }

  uint32_t GetSlotSize() const
  {
    return GetIndexKeySize() + sizeof(RowId) + 2 * sizeof(bool);
  }

  uint32_t GetPairSize() const
  {
    return GetIndexKeySize() + sizeof(RowId);
  }

  uint32_t GetMaxSlotNum() const
  {
    return (PAGE_SIZE - sizeof(key_size_)) / GetSlotSize();
  }

  uint32_t GetKeyOffset(uint32_t idx) const
  {
    return idx * GetSlotSize();
  }

  uint32_t GetValueOffset(uint32_t idx) const
  {
    return GetKeyOffset(idx) + GetIndexKeySize();
  }

  uint32_t GetOccupiedOffset(uint32_t idx) const
  {
    return GetKeyOffset(idx) + GetPairSize();
  }

  uint32_t GetReadableOffset(uint32_t idx) const
  {
    return GetKeyOffset(idx) + GetPairSize() + sizeof(bool);
  }

private:
  key_size_t key_size_;
  char slot_data_[0];
};

#endif  // MINISQL_HASH_TABLE_BUCKET_PAGE_H
