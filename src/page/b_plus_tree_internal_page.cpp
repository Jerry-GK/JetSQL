#include "page/b_plus_tree_internal_page.h"
#include <cstddef>
#include "common/config.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */

void BPlusTreeInternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size ,size_t max_size) {
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetKeySize(key_size);
  this->SetMaxSize(max_size);
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  memset(data_, 0, PAGE_SIZE - GetHeaderSize());
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */

IndexKey *BPlusTreeInternalPage::KeyAt(int index) const {
  int entry_offset = index * GetEntrySize();
  const BInternalEntry *entry = reinterpret_cast<const BInternalEntry *>(data_ + entry_offset);
  return const_cast<IndexKey *>(&entry->key);
}

page_id_t BPlusTreeInternalPage::ValueAt(int index) const {
  int entry_offset = index * GetEntrySize();
  const BInternalEntry *entry = reinterpret_cast<const BInternalEntry *>(data_ + entry_offset);
  return entry->value;
}

BInternalEntry *BPlusTreeInternalPage::EntryAt(int index) {
  // replace with your own code
  int entry_offset = index * GetEntrySize();
  BInternalEntry *entry = reinterpret_cast<BInternalEntry *>(data_ + entry_offset);
  return entry;
}
