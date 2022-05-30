#include "page/b_plus_tree_page.h"
#include "common/config.h"

/*
 * Helper methods to get/set page type
 * Page type enum class is defined in b_plus_tree_page.h
 */
bool BPlusTreePage::IsLeafPage() const {
  return page_type_ == IndexPageType::LEAF_PAGE;
}

bool BPlusTreePage::IsRootPage() const {
  return page_type_ == IndexPageType::INTERNAL_PAGE && page_id_ == INVALID_PAGE_ID;
}

void BPlusTreePage::SetPageType(IndexPageType page_type) {
  page_type_ = page_type;
}

int BPlusTreePage::GetSize() const {
  return this->size_;
}

size_t BPlusTreePage::GetKeySize() const {
  return this->key_size_;
}

void BPlusTreePage::SetSize(int size) {
  this->size_ = size;
}

void BPlusTreePage::SetKeySize(size_t size){
  this->key_size_ = size;
}
/*
 * Helper methods to get/set max size (capacity) of the page
 */
int BPlusTreePage::GetMaxSize() const {
  return max_size_;
}

void BPlusTreePage::SetMaxSize(int size) {
  this->max_size_ = size;
}

/*
 * Helper method to get min page size
 * Generally, min page size == max page size / 2
 */
int BPlusTreePage::GetMinSize() const {
  return max_size_ / 2;
}

/*
 * Helper methods to get/set parent page id
 */
page_id_t BPlusTreePage::GetParentPageId() const {
  return parent_page_id_;
}

void BPlusTreePage::SetParentPageId(page_id_t parent_page_id) {
  parent_page_id_ = parent_page_id;
}

/*
 * Helper methods to get/set self page id
 */
page_id_t BPlusTreePage::GetPageId() const {
  return page_id_;
}

void BPlusTreePage::SetPageId(page_id_t page_id) {
  this->page_id_ = page_id;
}

/*
 * Helper methods to set lsn
 */
void BPlusTreePage::SetLSN(lsn_t lsn) {
  lsn_ = lsn;
}