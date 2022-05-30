#include "index/index_iterator.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_leaf_page.h"

BPlusTreeIndexIterator::BPlusTreeIndexIterator() : tree_(nullptr), node_(nullptr), index_offset_(-1) {
  // this is an invalid iterator
}

BPlusTreeIndexIterator::BPlusTreeIndexIterator(BPlusTree *tree, Schema *key_schema, BPlusTreeLeafPage *node, int offset)
    : tree_(tree), node_(node), index_offset_(offset), key_schema_(key_schema) {
  // this is a valid iterator
}

BPlusTreeIndexIterator::~BPlusTreeIndexIterator() {
  // just unpin the page
  if (tree_ && node_) tree_->buffer_pool_manager_->UnpinPage(node_->GetPageId(), false);
}
BLeafEntry *BPlusTreeIndexIterator::operator->() { return node_->EntryAt(index_offset_); }

BLeafEntry &BPlusTreeIndexIterator::operator*() { return *node_->EntryAt(index_offset_); }

bool BPlusTreeIndexIterator::IsNull() const {
  bool r = false;
  if (key_schema_ && node_ && tree_) {
    auto ent = node_->EntryAt(index_offset_);
    int nc = key_schema_->GetColumnCount();
    // int nb = (nc - 1) / 8 + 1;
    cout << "the value is "  << endl;
    for (int i = 0; i < nc; i++) {
      cout << hex << setw(2) << (unsigned int)ent->key.value[i];
      if (ent->key.value[i >> 3] & (1 << (i & 7))) {
        r = true;
      }
      break;
    }
  }else{
    cout << "invalid iterator !" << endl;
  }
  cout << "result is " << (r ? " true" : "false") << endl;
  return r;
}

BPlusTreeIndexIterator &BPlusTreeIndexIterator::operator++() {
  if (!node_ || !tree_ || index_offset_ < 0) return *this;
  if (index_offset_ < node_->GetSize() - 1) {
    this->index_offset_ += 1;
  } else {
    // need to fetch a new page
    this->index_offset_ = 0;
    page_id_t next = node_->GetNextPageId();
    if (next == INVALID_PAGE_ID) {
      tree_->buffer_pool_manager_->UnpinPage(node_->GetPageId(), false);
      this->node_ = nullptr;
      this->index_offset_ = -1;
      return *this;
    }
    // how to detect whether the pair is dirty
    tree_->buffer_pool_manager_->UnpinPage(node_->GetPageId(), false);
    Page *p = tree_->buffer_pool_manager_->FetchPage(next);
    this->node_ = reinterpret_cast<BPlusTreeLeafPage *>(p->GetData());
  }
  return *this;
}

bool BPlusTreeIndexIterator::operator==(const BPlusTreeIndexIterator &itr) const {
  if (tree_ && node_ && index_offset_ >= 0 && itr.node_ && itr.tree_ && itr.index_offset_ >= 0) {
    BLeafEntry *m1 = node_->EntryAt(index_offset_);
    BLeafEntry *m2 = itr.node_->EntryAt(itr.index_offset_);
    return tree_->comparator_(&m1->key, &m2->key) == 0;
  }
  return (tree_ == itr.tree_ && node_ == itr.node_ && (index_offset_ == itr.index_offset_) && (index_offset_ == -1));
}

bool BPlusTreeIndexIterator::operator!=(const BPlusTreeIndexIterator &itr) const {
  auto it = *this;
  return !(it == itr);
}