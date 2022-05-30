#ifndef MINISQL_B_PLUS_TREE_LEAF_PAGE_H
#define MINISQL_B_PLUS_TREE_LEAF_PAGE_H

/**
 * b_plus_tree_leaf_page.h
 *
 * Store indexed key and record id(record id = page id combined with slot id,
 * see include/common/rid.h for detailed implementation) together within leaf
 * page. Only support unique key.

 * Leaf page format (keys are stored in order):
 *  ----------------------------------------------------------------------
 * | HEADER | KEY(1) + RID(1) | KEY(2) + RID(2) | ... | KEY(n) + RID(n)
 *  ----------------------------------------------------------------------
 *
 *  Header format (size in byte, 24 bytes in total):
 *  ---------------------------------------------------------------------
 * | PageType (4) | CurrentSize (4) | MaxSize (4) | ParentPageId (4) |
 *  ---------------------------------------------------------------------
 *  ------------------------------
 * | PageId (4) | NextPageId (4)
 *  ------------------------------
 */
#include <cstddef>
#include <cstring>
#include <iterator>
#include <utility>
#include <vector>

#include "index/index_key.h"
#include "page/b_plus_tree_page.h"
#include "record/row.h"

struct BLeafEntry {
  RowId value;
  IndexKey key;
  BLeafEntry(IndexKey *k, RowId v) {
    value = v;
    key.keysize = k->keysize;
    memcpy(key.value, k->value, k->keysize);
  }
  BLeafEntry(BLeafEntry &another) {
    value = another.value;
    key.keysize = another.key.keysize;
    memcpy(key.value, another.key.value, key.keysize);
  }
  BLeafEntry &operator=(BLeafEntry &another) {
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
};

class BPlusTreeLeafPage : public BPlusTreePage {
 public:
  // After creating a new leaf page from buffer pool, must call initialize
  // method to set default values
  void Init(page_id_t page_id, page_id_t parent_id, int keysize, size_t max_size);

  // helper methods
  page_id_t GetNextPageId() const;

  static constexpr size_t GetHeaderSize() { return sizeof(BPlusTreeLeafPage); }

  size_t GetEntrySize() const { return sizeof(BLeafEntry) + GetKeySize(); }

  void SetNextPageId(page_id_t next_page_id);

  RowId ValueAt(int index) const;

  IndexKey *KeyAt(int index) const;

  BLeafEntry *EntryAt(int index);

  char *GetData() { return data_; }

 private:
  page_id_t next_page_id_;
  char data_[0];
};

#endif  // MINISQL_B_PLUS_TREE_LEAF_PAGE_H
