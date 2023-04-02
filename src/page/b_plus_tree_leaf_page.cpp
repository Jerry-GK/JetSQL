#include "page/b_plus_tree_leaf_page.h"
#include <algorithm>
#include <cstddef>
#include <cstring>
#include "common/config.h"
#include "index/basic_comparator.h"
#include "index/index_iterator.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */

void BPlusTreeLeafPage::Init(page_id_t page_id, page_id_t parent_id, int keysize , size_t max_size) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetKeySize(keysize);
  this->SetMaxSize(max_size);
  this->SetPageType(IndexPageType::LEAF_PAGE);
  memset(data_, 0, PAGE_SIZE - GetHeaderSize());
}

/**
 * Helper methods to set/get next page id
 */

page_id_t BPlusTreeLeafPage::GetNextPageId() const { return next_page_id_; }

void BPlusTreeLeafPage::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

IndexKey *BPlusTreeLeafPage::KeyAt(int index) const {
  // replace with your own code
  int entry_offset = index * GetEntrySize();
  const BLeafEntry *entry = reinterpret_cast<const BLeafEntry *>(data_ + entry_offset);
  return const_cast<IndexKey *>(&entry->key);
}

RowId BPlusTreeLeafPage::ValueAt(int index) const {
  // replace with your own code
  int entry_offset = index * GetEntrySize();
  const BLeafEntry *entry = reinterpret_cast<const BLeafEntry *>(data_ + entry_offset);
  return entry->value;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */

BLeafEntry *BPlusTreeLeafPage::EntryAt(int index) {
  // replace with your own code
  int entry_offset = index * GetEntrySize();
  BLeafEntry *entry = reinterpret_cast<BLeafEntry *>(data_ + entry_offset);
  return entry;
}