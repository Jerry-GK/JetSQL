#ifndef MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
#define MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H

#include <cstddef>
#include <cstring>
#include <queue>
#include "common/config.h"
#include "index/index_key.h"
#include "page/b_plus_tree_page.h"

#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(pair<IndexKey, page_id_t>)) - 1)

struct BInternalEntry {
  page_id_t value;
  IndexKey key;
  BInternalEntry(IndexKey *k, page_id_t v) {
    value = v;
    key.keysize = k->keysize;
    memcpy(key.value, k->value, k->keysize);
  }
  BInternalEntry(BInternalEntry &another) {
    value = another.value;
    key.keysize = another.key.keysize;
    memcpy(key.value, another.key.value, key.keysize);
  }
  BInternalEntry &operator=(BInternalEntry &another) {
    value = another.value;
    key.keysize = another.key.keysize;
    memcpy(key.value, another.key.value, key.keysize);
    return *this;
  }
  void SetKey(const IndexKey *k) {
    key.keysize = k->keysize;
    memcpy(key.value, k->value, k->keysize);
  }
  void SetValue(page_id_t r) { value = r; }
};

/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 */
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id, key_size_t key_size, size_t max_size);

  IndexKey *KeyAt(int index) const;

  page_id_t ValueAt(int index) const;

  static constexpr size_t GetHeaderSize() { return sizeof(BPlusTreeInternalPage); }

  size_t GetEntrySize() const { return sizeof(BInternalEntry) + GetKeySize(); }

  BInternalEntry *EntryAt(int index);

  void Remove(int index);

  char *GetData() { return data_; }

 private:
  char data_[0];
};

#endif  // MINISQL_B_PLUS_TREE_INTERNAL_PAGE_H
