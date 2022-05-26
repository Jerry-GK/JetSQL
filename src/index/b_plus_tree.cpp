#include "index/b_plus_tree.h"
#include <cstddef>
#include <cstring>
#include <exception>
#include <iomanip>
#include <ostream>
#include <queue>
#include <sstream>
#include <string>
#include "common/config.h"
#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/index_iterator.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_page.h"
#include "page/index_roots_page.h"

IndexKey *IndexKey::SerializeFromKey(char * buf,const Row &row, Schema *schema, size_t keysize) {
  uint32_t size = row.GetSerializedSize(schema);
  ASSERT(row.GetFieldCount() == schema->GetColumnCount(), "field nums not match.");
  ASSERT(size <= keysize, "Index key size exceed max key size.");
  memset(buf, 0, keysize);
  IndexKey *key = reinterpret_cast<IndexKey *>(buf);
  key->keysize = keysize;
  row.SerializeTo(key->value, schema);
  return key;
}

BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, KeyComparator cmp,size_t key_size, size_t leaf_max_size,size_t internal_max_size)
    : index_id_(index_id),comparator_(cmp),buffer_pool_manager_(buffer_pool_manager) {
  Page *p = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *root_page = reinterpret_cast<IndexRootsPage *>(p->GetData());
  root_page_id_ = INVALID_PAGE_ID;
  root_page->GetRootId(index_id, &root_page_id_);
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  this->leaf_max_size_ = leaf_max_size;
  this->internal_max_size_ = internal_max_size;
  this->key_size_ = key_size;
}

void BPlusTree::Init(index_id_t index_id, BufferPoolManager *buffer_pool_manager, size_t keysize, size_t leaf_max_size,size_t internal_max_size ) {
  index_id_ = index_id;
  buffer_pool_manager_ = buffer_pool_manager;
  Page *p = buffer_pool_manager->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *root_page = reinterpret_cast<IndexRootsPage *>(p->GetData());
  root_page_id_ = INVALID_PAGE_ID;
  root_page->GetRootId(index_id, &root_page_id_);
  buffer_pool_manager->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
  this->leaf_max_size_ = leaf_max_size;
  this->internal_max_size_ = internal_max_size;
  this->key_size_ = keysize;
}

void BPlusTree::PrintTree(std::ostream &out) {
  queue<page_id_t> k;
  k.push(root_page_id_);
  k.push(INVALID_PAGE_ID);
  while (!k.empty()) {
    page_id_t c = k.front();
    k.pop();
    if (c == INVALID_PAGE_ID) {
      if (k.empty())
        break;
      else {
        out << "\n";
        k.push(INVALID_PAGE_ID);
        continue;
      }
    }
    Page *p = buffer_pool_manager_->FetchPage(c);
    BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
    if (bp->IsLeafPage()) {
      BPlusTreeLeafPage *lp = reinterpret_cast<BPlusTreeLeafPage *>(bp);
      for (int i = 0; i < lp->GetSize(); i++) out << &lp->EntryAt(i)->key << " ";
      out << " | ";
    } else {
      BPlusTreeInternalPage *ip = reinterpret_cast<BPlusTreeInternalPage *>(bp);
      for (int i = 0; i < ip->GetSize(); i++) {
        out << &ip->EntryAt(i)->key << " ";
        k.push(ip->EntryAt(i)->value);
      }
      out << " | ";
    }
    buffer_pool_manager_->UnpinPage(c, false);
  }
  out << endl;
}

bool BPlusTree::CheckIntergrity() {
  queue<page_id_t> k;
  k.push(root_page_id_);
  k.push(INVALID_PAGE_ID);
  while (!k.empty()) {
    page_id_t c = k.front();
    k.pop();
    if (c == INVALID_PAGE_ID) {
      if (k.empty())
        break;
      else {
        k.push(INVALID_PAGE_ID);
        continue;
      }
    }
    Page *p = buffer_pool_manager_->FetchPage(c);
    BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
    if (bp->IsLeafPage()) {
      BPlusTreeLeafPage *lp = reinterpret_cast<BPlusTreeLeafPage *>(bp);
      for (int i = 0; i < lp->GetSize(); i++) {
        if (i > 0) {
          if (comparator_(&lp->EntryAt(i)->key, &lp->EntryAt(i - 1)->key) <= 0) {
            return false;
          }
        }
      }
    } else {
      BPlusTreeInternalPage *ip = reinterpret_cast<BPlusTreeInternalPage *>(bp);
      for (int i = 0; i < ip->GetSize(); i++) {
        k.push(ip->EntryAt(i)->value);
        if (i > 0) {
          if (comparator_(&ip->EntryAt(i)->key, &ip->EntryAt(i - 1)->key) <= 0) {
            return false;
          }
        }
      }
    }
    buffer_pool_manager_->UnpinPage(c, false);
  }
  return true;
}

void BPlusTree::Destroy() {}

/*
 * Helper function to decide whether current b+tree is empty
 */

bool BPlusTree::IsEmpty() const { return false; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */

bool BPlusTree::GetValue(const IndexKey *key, std::vector<RowId> &result, Transaction *transaction) {
  Page *p = buffer_pool_manager_->FetchPage(root_page_id_);
  if (p == nullptr) return false;
  BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  while (!bp->IsLeafPage()) {
    auto ibp = reinterpret_cast<BPlusTreeInternalPage *>(bp);
    // auto data = ibp->GetData();
    int l = 0, r = ibp->GetSize() - 1;
    while (l < r) {
      int mid = (l + r + 1) / 2;
      if (comparator_(ibp->KeyAt(mid), key) > 0)
        r = mid - 1;
      else
        l = mid;
    }
    page_id_t next = ibp->ValueAt(r);
    buffer_pool_manager_->UnpinPage(bp->GetPageId(), false);
    p = buffer_pool_manager_->FetchPage(next);
    bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  auto *c_lp = reinterpret_cast<BPlusTreeLeafPage *>(bp);
  int l = 0, r = c_lp->GetSize() - 1;
  int mid = (l + r) / 2;
  while (l < r) {
    mid = (l + r) / 2;
    auto c = c_lp->KeyAt(mid);
    if (comparator_(c, key) > 0)
      r = mid;
    else if (comparator_(c, key) < 0)
      l = mid + 1;
    else
      break;
  }
  mid = (l + r) / 2;
  auto pair = c_lp->EntryAt(mid);
  buffer_pool_manager_->UnpinPage(bp->GetPageId(), false);
  if (comparator_(&pair->key, key) == 0) {
    result.push_back(pair->value);
    return true;
  }
  return false;
}

/**
 * @brief Recursively insert a key-value pair into destination.If destination is a internal node, keep searching.
 *
 * @param destination the node to insert in
 * @param key the key
 * @param value the value
 * @return BPlusTreePage* if a split happens, return the new created node. Otherwise , return nullptr.
 */

BPlusTreePage *BPlusTree::InternalInsert(BPlusTreePage *destination, const IndexKey *key, const RowId &value,
                                         IndexKey **newKey, bool *found, bool *modified) {
  // a leaf node is met
  BPlusTreePage *splitted_page = nullptr;
  *found = false;
  if (destination->IsLeafPage()) {
    auto *c_lp = reinterpret_cast<BPlusTreeLeafPage *>(destination);
    // perform a binary search ~
    // find the first index whose key value is greater than or equal to k , if no such value , index = 0
    // BLeafEntry *current_data = c_lp->GetData();
    // cout << "Leaf page reached." << endl;
    int l = 0, r = c_lp->GetSize();
    while (l < r) {
      int mid = (l + r) / 2;
      auto c = c_lp->KeyAt(mid);
      if (comparator_(c, key) > 0)
        r = mid;
      else if (comparator_(c, key) < 0)
        l = mid + 1;
      else {
        *found = true;
        // cout << "Duplicate key." << endl;
        return nullptr;
      }
    }
    int target_row_index = r;
    if (c_lp->GetSize() == c_lp->GetMaxSize()) {
      // the leaf page is full , split it
      page_id_t split_page_id = INVALID_PAGE_ID;
      Page *split_page = buffer_pool_manager_->NewPage(split_page_id);
      // cout << "Leaf page split." << endl;
      auto s_lp = reinterpret_cast<BPlusTreeLeafPage *>(split_page->GetData());
      s_lp->Init(split_page_id, c_lp->GetParentPageId(), key_size_ , leaf_max_size_);
      if (split_page == nullptr) {
        ASSERT(0, "Bufferpool new leaf page failed!");
        return nullptr;
      }
      s_lp->SetPageType(IndexPageType::LEAF_PAGE);
      s_lp->SetPageId(split_page_id);
      s_lp->SetSize(leaf_max_size_ + 1 - (leaf_max_size_ / 2));
      c_lp->SetSize(leaf_max_size_ / 2);
      s_lp->SetNextPageId(c_lp->GetNextPageId());
      c_lp->SetNextPageId(split_page_id);
      // cout << "size of r =" << s_lp->GetSize() <<",size of l = " << c_lp->GetSize() << endl;
      // auto split_data = s_lp->GetData();

      for (int i = leaf_max_size_; i >= 0; i--) {
        // BLeafEntry* current_pair;
        const IndexKey *current_key;
        RowId current_rid;
        // if(i < target_row_index)current_pair = current_data[i];
        if (i < target_row_index) {
          current_rid = c_lp->ValueAt(i);
          current_key = c_lp->KeyAt(i);
        } else if (i == target_row_index) {
          current_key = key;
          current_rid = value;
          // current_pair = {value,key};
        } else {
          current_rid = c_lp->ValueAt(i - 1);
          current_key = c_lp->KeyAt(i - 1);
          // current_pair = c_lp->EntryAt(i - 1);
        }

        if (i < leaf_max_size_ / 2) {
          // copy the leftmost floor(max/2) nodes to left node
          // c_lp->EntryAt(i) = current_pair;
          c_lp->EntryAt(i)->SetKey(current_key);
          c_lp->EntryAt(i)->SetValue(current_rid);
        } else {
          // copy the rightmost max - floor(max/2) nodes to right node
          s_lp->EntryAt(i - leaf_max_size_ / 2)->SetKey(current_key);
          s_lp->EntryAt(i - leaf_max_size_ / 2)->SetValue(current_rid);
          // split_data[i - internal_max_size_/2] = current_pair;
          // c_lp->EntryAt(i) = {RowId(),IndexKey()};

          // current_data[i] = {IndexKey(),RowId()};
        }
      }
      *modified = true;
      splitted_page = reinterpret_cast<BPlusTreePage *>(s_lp);
      *newKey = c_lp->KeyAt(0);
      // newKey = current_data[0].first;

    } else {
      // the leaf page is not null , simply insert
      c_lp->SetSize(c_lp->GetSize() + 1);
      for (int i = c_lp->GetSize() -1; i > target_row_index; i--) *c_lp->EntryAt(i) = *c_lp->EntryAt(i - 1);

      // c_lp->EntryAt(target_row_index) = {value,key};
      c_lp->EntryAt(target_row_index)->SetValue(value);
      c_lp->EntryAt(target_row_index)->SetKey(key);
      // current_data[target_row_index] = {key,value};
      *modified = true;
      *newKey = c_lp->KeyAt(0);
    }
  } else {  // still an internal node
    // cout << "Internal page reached." << endl;
    page_id_t target_page_id = INVALID_PAGE_ID;
    // perform a binary search ~
    auto *c_ip = reinterpret_cast<BPlusTreeInternalPage *>(destination);
    // target_page_id = p_internal->Lookup(key ,comparator_ );
    // we should not use the Lookup method since some useful information is hidden
    // perform the binary search here.

    // pair<IndexKey,page_id_t> *current_data = c_ip->GetData();
    int l = 0, r = c_ip->GetSize() - 1;
    // find the last index whose key is smaller than or equal to key
    while (l < r) {
      int mid = (l + r + 1) / 2;
      if (comparator_(c_ip->KeyAt(mid), key) > 0)
        r = mid - 1;
      else
        l = mid;
    }
    int target_page_index = r;
    target_page_id = c_ip->EntryAt(target_page_index)->value;
    if (target_page_id == INVALID_PAGE_ID) {
      *found = false;
      return nullptr;
    }
    Page *target_page = buffer_pool_manager_->FetchPage(target_page_id);
    if (target_page == nullptr) {
      *found = false;
      ASSERT(0, "Fetch BPlustree page failed!");
      return nullptr;
    }
    BPlusTreePage *target_bplus_page = reinterpret_cast<BPlusTreePage *>(target_page->GetData());
    bool child_found = false;
    bool child_modified = false;
    IndexKey *nk;

    BPlusTreePage *new_page = InternalInsert(target_bplus_page, key, value, &nk, &child_found, &child_modified);

    if (child_found) {
      *found = true;
      return nullptr;
    }
    if (new_page != nullptr) {  // a split happens !
      *modified = true;
      IndexKey *new_key;
      page_id_t new_page_id = new_page->GetPageId();
      // Page *new_page_page = buffer_pool_manager_->FetchPage(new_page_id);
      if (new_page->IsLeafPage()) {
        auto new_leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(new_page);
        new_key = &new_leaf_page->EntryAt(0)->key;
      } else {
        auto new_internal_page = reinterpret_cast<BPlusTreeInternalPage *>(new_page);
        new_key = &new_internal_page->EntryAt(0)->key;
      }
      if (c_ip->GetSize() == c_ip->GetMaxSize()) {
        // cout << "Internal page split." << endl;
        // continue splitting
        // create a new internal page
        page_id_t split_page_id = INVALID_PAGE_ID;
        Page *split_page = buffer_pool_manager_->NewPage(split_page_id);
        child_modified = true;
        if (split_page == nullptr) {
          ASSERT(0, "New BPlustree page failed!");
          return nullptr;
        }
        // auto split_general_page = reinterpret_cast<BPlusTreePage *>(split_page->GetData());
        auto s_ip = reinterpret_cast<BPlusTreeInternalPage *>(split_page->GetData());
        s_ip->Init(split_page_id,c_ip->GetParentPageId(),key_size_,internal_max_size_);
        // auto split_data = s_ip->GetData();
        s_ip->SetPageType(IndexPageType::INTERNAL_PAGE);
        s_ip->SetPageId(split_page_id);
        c_ip->SetSize(internal_max_size_ / 2);
        s_ip->SetSize(internal_max_size_ + 1 - (internal_max_size_ / 2));
        for (int i = internal_max_size_; i >= 0; i--) {
          IndexKey *current_key;
          page_id_t current_value;
          if (i <= target_page_index) {
            current_key = c_ip->KeyAt(i);
            current_value = c_ip->ValueAt(i);
          } else if (i == target_page_index + 1) {
            current_key = new_key;
            current_value = new_page_id;
            // current_pair = {new_page_id,new_key};
          } else{
            current_key = c_ip->KeyAt(i - 1);
            current_value = c_ip->ValueAt(i - 1);
          }

          if (i < internal_max_size_ / 2) {
            // copy the leftmost floor(max/2) nodes to left node

            c_ip->EntryAt(i)->SetKey(current_key);
            c_ip->EntryAt(i)->SetValue(current_value);
          } else {
            // copy the rightmost max - floor(max/2) nodes to right node
            s_ip->EntryAt(i - internal_max_size_ / 2)->SetKey(current_key);
            s_ip->EntryAt(i - internal_max_size_ / 2)->SetValue(current_value);
          }
        }
        // and return the splitted page~

        splitted_page = reinterpret_cast<BPlusTreePage *>(s_ip);

      } else {
        c_ip->SetSize(c_ip->GetSize() + 1);
        // simply insert the new node and update links
        for (int i = c_ip->GetSize() - 1; i > target_page_index; i--) {
          *c_ip->EntryAt(i) = *c_ip->EntryAt(i - 1);
        }
        c_ip->EntryAt(target_page_index + 1)->SetKey(new_key);  // = {new_page_id,new_key };
        c_ip->EntryAt(target_page_index + 1)->SetValue(new_page_id);
      }
      buffer_pool_manager_->UnpinPage(new_page_id, true);
    }
    buffer_pool_manager_->UnpinPage(target_page_id, modified);
    if (!(*nk == *c_ip->KeyAt(target_page_index))) {
      c_ip->EntryAt(target_page_index)->SetKey(nk);
      *modified = true;
    }
    *newKey = &c_ip->EntryAt(0)->key;
  }
  return splitted_page;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

bool BPlusTree::Insert(const IndexKey *key, const RowId &value, Transaction *transaction) {
  if (root_page_id_ == INVALID_PAGE_ID) {
    StartNewTree(key, value);
    UpdateRootPageId(true);
  } else {
    Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    page_id_t old_root_page_id = root_page_id_;
    if (root_page == nullptr) {
      ASSERT(0, "Bplustree fetch root page failed");
      return false;
    }
    BPlusTreePage *root_general_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
    bool found = false;
    bool modified = false;
    IndexKey *nk;
    BPlusTreePage *new_page = InternalInsert(root_general_page, key, value, &nk, &found, &modified);
    if (found) {
      return false;
    }
    if (new_page != nullptr) {
      // a split was performed at the 2th level and we need to build a new root page
      modified = true;
      // BInternalEntry* head_pair_left,head_pair_right;
      IndexKey *key_left, *key_right;
      page_id_t pid_left, pid_right;
      if (new_page->IsLeafPage()) {
        auto new_leaf_page_right = reinterpret_cast<BPlusTreeLeafPage *>(new_page);
        auto new_leaf_page_left = reinterpret_cast<BPlusTreeLeafPage *>(root_general_page);

        key_right = new_leaf_page_right->KeyAt(0);
        pid_right = new_page->GetPageId();
        // head_pair_right = {new_leaf_page_right->EntryAt(0).key,new_page->GetPageId()};

        key_left = new_leaf_page_left->KeyAt(0);
        pid_left = root_general_page->GetPageId();
        // head_pair_left = {new_leaf_page_left->EntryAt(0).key,root_general_page->GetPageId()};
      } else {
        auto new_leaf_page_right = reinterpret_cast<BPlusTreeInternalPage *>(new_page);
        auto new_leaf_page_left = reinterpret_cast<BPlusTreeInternalPage *>(root_general_page);
        // head_pair_right = {new_leaf_page_right->EntryAt(0).key,new_page->GetPageId()};
        // head_pair_left = {new_leaf_page_left->EntryAt(0).key,root_general_page->GetPageId()};
        key_right = new_leaf_page_right->KeyAt(0);
        pid_right = new_page->GetPageId();
        // head_pair_right = {new_leaf_page_right->EntryAt(0).key,new_page->GetPageId()};

        key_left = new_leaf_page_left->KeyAt(0);
        pid_left = root_general_page->GetPageId();
      }
      BPlusTreeInternalPage *new_root;
      page_id_t new_root_page_id;
      Page *new_root_page = buffer_pool_manager_->NewPage(new_root_page_id);
      if (new_root_page == nullptr) {
        ASSERT(0, "BPlustree new root page failed!");
        return false;
      }
      root_page_id_ = new_root_page_id;
      new_root = reinterpret_cast<BPlusTreeInternalPage *>(new_root_page->GetData());
      new_root->Init(new_root_page_id,new_root_page_id,key_size_ ,internal_max_size_);
      new_root->SetPageId(new_root_page_id);
      new_root->SetParentPageId(INVALID_PAGE_ID);
      new_root->SetPageType(IndexPageType::INTERNAL_PAGE);
      new_root->SetSize(2);
      new_root->SetMaxSize(internal_max_size_);
      // auto data = new_root->GetData();
      // new_root->EntryAt(0) = head_pair_left;
      new_root->EntryAt(0)->SetKey(key_left);
      new_root->EntryAt(0)->SetValue(pid_left);
      // data[0] = head_pair_left;
      // new_root->EntryAt(1) = head_pair_right;
      new_root->EntryAt(1)->SetKey(key_right);
      new_root->EntryAt(1)->SetValue(pid_right);
      // data[1] = head_pair_right;
      buffer_pool_manager_->UnpinPage(new_root_page_id, true);
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    }
    buffer_pool_manager_->UnpinPage(old_root_page_id, true);
  }
  return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */

void BPlusTree::StartNewTree(const IndexKey *key, const RowId &value) {
  page_id_t root_page_id = INVALID_PAGE_ID;
  Page *p = buffer_pool_manager_->NewPage(root_page_id);
  if (p == nullptr) {
    ASSERT(0, "Start new tree allocate page failed.");
    return;
  }
  BPlusTreeLeafPage *leafPage = reinterpret_cast<BPlusTreeLeafPage *>(p->GetData());
  leafPage->Init(root_page_id, INVALID_PAGE_ID, key_size_ , internal_max_size_);
  leafPage->SetNextPageId(INVALID_PAGE_ID);
  leafPage->SetPageType(IndexPageType::LEAF_PAGE);
  leafPage->SetSize(1);
  leafPage->SetMaxSize(leaf_max_size_);
  root_page_id_ = root_page_id;
  leafPage->EntryAt(0)->SetKey(key);
  leafPage->EntryAt(0)->SetValue(value);  // = {key,value};
  buffer_pool_manager_->UnpinPage(root_page_id, true);
}

void BPlusTree::Remove(const IndexKey *key, Transaction *transaction) {
  // stringstream ss;
  // PrintTree(ss);
  
  Page *root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *root_bplus_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  bool modified = false;
  bool shrink = false;
  IndexKey *nk;
  InternalRemove(root_bplus_page, key, &nk, &modified);
  if (!root_bplus_page->IsLeafPage()) {
    auto *ip = reinterpret_cast<BPlusTreeInternalPage *>(root_bplus_page);
    ip->EntryAt(0)->SetKey(nk);  // key = nk;
    if(ip->GetSize() == 1){// size == 1 , remove one level
      shrink = true;
      Page * new_root_page = buffer_pool_manager_->FetchPage(ip->ValueAt(0));
      BPlusTreePage * np = reinterpret_cast<BPlusTreePage *>(new_root_page->GetData());
      np->SetParentPageId(INVALID_PAGE_ID);
      buffer_pool_manager_->UnpinPage(root_page_id_ , false);
      buffer_pool_manager_->DeletePage(root_page_id_);
      root_page_id_ = new_root_page->GetPageId();
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    }
  }
  if(!shrink)buffer_pool_manager_->UnpinPage(root_page_id_, modified);

  // if(!CheckIntergrity()){
  //   cout << "Intergrity check failed !" << endl;
  //   cout <<"Before :" << endl;
  //   cout << ss.str() << endl;
  //   cout <<"After :" << endl;
  //   PrintTree(cout);
  // }
}

/**
 * @brief Recursively delete a key-value pair from destination node.If destination is an internal node, keep searching.
 *
 * @param destination
 * @param key the key to delete
 * @return int if the key is not found, return -1. Otherwise, return the new size of modified node.
 */

int BPlusTree::InternalRemove(BPlusTreePage *destination, const IndexKey *key, IndexKey **newKey, bool *modified) {
  if (destination->IsLeafPage()) {
    auto *c_lp = reinterpret_cast<BPlusTreeLeafPage *>(destination);
    // BLeafEntry *current_data = c_lp->GetData();
    int current_size = c_lp->GetSize();
    int l = 0, r = current_size - 1;
    // find the index whose key value is equal to key

    while (l < r) {
      int mid = (l + r + 1) / 2;
      if (comparator_(c_lp->KeyAt(mid), key) > 0)
        r = mid - 1;
      else
        l = mid;
    }
    if (comparator_(c_lp->KeyAt(r), key) != 0) {
      // cout << "Key " << key << " not found !" << endl;
      // cout << "r = " << r << " , data on current node :" << endl;
      // for(int i= 0;i< current_size;i++)cout << current_data[i].first << " ";
      // cout << endl;
      // cout << "Current tree :" << endl;
      // PrintTree(cout);
      return -1;
    }
    int target_row_index = r;
    for (int i = target_row_index; i < current_size - 1; i++) *c_lp->EntryAt(i) = *c_lp->EntryAt(i + 1);
    // memmove(current_data + target_row_index , current_data + target_row_index  + 1,  );
    c_lp->SetSize(current_size - 1);
    *modified = true;
    *newKey = c_lp->KeyAt(0);
    return current_size - 1;
  } else {
    // this is not a leaf page ,continue searching down
    page_id_t target_page_id = INVALID_PAGE_ID;
    // perform a binary search ~
    auto *c_ip = reinterpret_cast<BPlusTreeInternalPage *>(destination);
    // target_page_id = p_internal->Lookup(key ,comparator_ );
    // we should not use the Lookup method since some useful information is hidden
    // perform the binary search here.
    // pair<IndexKey,page_id_t> *current_data = c_ip->GetData();
    int l = 0, r = c_ip->GetSize() - 1;
    // find the last index whose key value is smaller than or equal key
    while (l < r) {
      int mid = (l + r + 1) / 2;
      if (comparator_(c_ip->KeyAt(mid), key) > 0)
        r = mid - 1;
      else
        l = mid;
    }
    int target_page_index = r;
    target_page_id = c_ip->ValueAt(target_page_index);
    if (target_page_id == INVALID_PAGE_ID) return -1;
    Page *target_page = buffer_pool_manager_->FetchPage(target_page_id);
    if (target_page == nullptr) {
      ASSERT(0, "Fetch BPlustree page failed!");
      return -1;
    }
    BPlusTreePage *target_bplus_page = reinterpret_cast<BPlusTreePage *>(target_page->GetData());
    bool child_modified = false;
    IndexKey *nk;
    int child_size = InternalRemove(target_bplus_page, key, &nk, &child_modified);
    if (child_size == -1) return -1;
    Page *p_left = nullptr;
    Page *p_right = nullptr;
    bool left_dirty = false;
    bool right_dirty = false;
    bool can_merge = false;
    bool merge_with_left = false;
    bool can_borrow = false;
    bool borrow_from_left = false;
    int target_size = target_bplus_page->GetSize();
    int target_max_size = target_bplus_page->GetMaxSize();
    int target_min_size = target_max_size / 2;
    if (child_size < target_min_size) {
      *modified = true;
      child_modified = true;
      // need to do some redistribution
      if (target_page_index > 0) {  // probably can be merged with left sib
        p_left = buffer_pool_manager_->FetchPage(c_ip->ValueAt(target_page_index - 1));
        ASSERT(p_left, "Fetch BPlustree page failed!");
        BPlusTreePage *left = reinterpret_cast<BPlusTreePage *>(p_left->GetData());
        if (left->GetSize() + child_size <= target_max_size) {
          // can be merged with left sib!!
          can_merge = true;
          merge_with_left = true;
        } else {
          // otherwise we can borrow from right sibling
          can_borrow = true;
          borrow_from_left = true;
        }
      } else if (target_page_index < target_size - 1) {  // probably can be merged with right sib
        p_right = buffer_pool_manager_->FetchPage(c_ip->ValueAt(target_page_index + 1));
        ASSERT(p_right, "Fetch BPlustree page failed!");
        BPlusTreePage *right = reinterpret_cast<BPlusTreePage *>(p_right->GetData());
        if (right->GetSize() + child_size <= target_max_size) {
          // can be merged with left sib!!
          can_merge = true;
          merge_with_left = false;
        } else {
          // otherwise we can borrow from left sibling
          can_borrow = true;
          borrow_from_left = false;
        }
      }

      if (can_merge) {
        // a merge operation is needed
        *modified = true;
        char *src, *dest;
        size_t size;
        int delete_index;
        if (target_bplus_page->IsLeafPage()) {
          // merge a leaf page with its sibling
          auto leaf_target = reinterpret_cast<BPlusTreeLeafPage *>(target_bplus_page);
          auto leaf_sibling =
              reinterpret_cast<BPlusTreeLeafPage *>(merge_with_left ? p_left->GetData() : p_right->GetData());
          if (merge_with_left) {
            left_dirty = true;
            src = reinterpret_cast<char *>(leaf_target->EntryAt(0));
            dest = reinterpret_cast<char *>(leaf_sibling->EntryAt(leaf_sibling->GetSize()));
            size = leaf_target->GetEntrySize() * leaf_target->GetSize();
            // merge current page into left page, delete current page
            delete_index = target_page_index;
            leaf_sibling->SetNextPageId(leaf_target->GetNextPageId());
            leaf_sibling->SetSize(leaf_target->GetSize() + leaf_sibling->GetSize());
          } else {
            right_dirty = true;
            src = reinterpret_cast<char *>(leaf_sibling->EntryAt(0));
            dest = reinterpret_cast<char *>(leaf_target->EntryAt(leaf_target->GetSize()));
            size = leaf_target->GetEntrySize() * leaf_sibling->GetSize();
            // merge right page into current page, delete right page
            delete_index = target_page_index + 1;
            leaf_target->SetNextPageId(leaf_sibling->GetNextPageId());
            leaf_target->SetSize(leaf_target->GetSize() + leaf_sibling->GetSize());
          }
        } else {
          // merge an internal page with its sibling
          auto internal_target = reinterpret_cast<BPlusTreeInternalPage *>(target_bplus_page);
          auto internal_sibling =
              reinterpret_cast<BPlusTreeInternalPage *>(merge_with_left ? p_left->GetData() : p_right->GetData());
          if (merge_with_left) {
            left_dirty = true;
            src = reinterpret_cast<char *>(internal_target->EntryAt(0));
            dest = reinterpret_cast<char *>(internal_sibling->EntryAt(internal_sibling->GetSize()));
            size = internal_sibling->GetEntrySize() * internal_target->GetSize();
            internal_sibling->SetSize(internal_sibling->GetSize() + internal_target->GetSize());
            delete_index = target_page_index;
            *nk = internal_sibling->EntryAt(0)->key;
          } else {
            right_dirty = true;
            src = reinterpret_cast<char *>(internal_sibling->EntryAt(0));
            dest = reinterpret_cast<char *>(internal_target->EntryAt(internal_target->GetSize()));
            size = internal_sibling->GetEntrySize() * internal_sibling->GetSize();
            internal_target->SetSize(internal_sibling->GetSize() + internal_target->GetSize());
            delete_index = target_page_index + 1;
          }
        }
        memcpy(dest, src, size);
        page_id_t page_to_delete = c_ip->ValueAt(delete_index);
        for (int i = delete_index; i < c_ip->GetSize() - 1; i++) *c_ip->EntryAt(i) = *c_ip->EntryAt(i + 1);
        // current_data[c_ip->GetSize() - 1] = pair<IndexKey,page_id_t>();
        c_ip->SetSize(c_ip->GetSize() - 1);
        // after merge , delete a page
        if (p_right && page_to_delete == p_right->GetPageId()) p_right = nullptr;
        if (target_page && page_to_delete == target_page->GetPageId()) target_page = nullptr;
        buffer_pool_manager_->UnpinPage(page_to_delete, false);
        buffer_pool_manager_->DeletePage(page_to_delete);
      } else if (can_borrow) {
        // simply borrow 1 from source to target , no further modification
        if (target_bplus_page->IsLeafPage()) {
          auto leaf_target = reinterpret_cast<BPlusTreeLeafPage *>(target_bplus_page);
          auto leaf_sibling =
              reinterpret_cast<BPlusTreeLeafPage *>(borrow_from_left ? p_left->GetData() : p_right->GetData());
          int sz_sib = leaf_sibling->GetSize() - 1;
          int sz_tg = leaf_target->GetSize() + 1;
          leaf_sibling->SetSize(sz_sib);
          leaf_target->SetSize(sz_tg);
          if (borrow_from_left) {
            left_dirty = true;
            for (int i = sz_tg - 1; i > 0; i--) *leaf_target->EntryAt(i) = *leaf_target->EntryAt(i - 1);
            *leaf_target->EntryAt(0) = *leaf_sibling->EntryAt(sz_sib);
            nk = &leaf_target->EntryAt(0)->key;
          } else {
            right_dirty = true;
            *leaf_target->EntryAt(sz_tg - 1) = *leaf_sibling->EntryAt(0);
            for (int i = 0; i < sz_sib; i++) *leaf_sibling->EntryAt(i) = *leaf_sibling->EntryAt(i + 1);
            nk = &leaf_sibling->EntryAt(0)->key;
          }
        } else {
          auto internal_target = reinterpret_cast<BPlusTreeInternalPage *>(target_bplus_page);
          auto internal_sibling =
              reinterpret_cast<BPlusTreeInternalPage *>(borrow_from_left ? p_left->GetData() : p_right->GetData());
          int sz_sib = internal_sibling->GetSize() - 1;
          int sz_tg = internal_target->GetSize() + 1;
          internal_sibling->SetSize(sz_sib);
          internal_target->SetSize(sz_tg);
          // auto dt_tg = internal_target->GetData();
          // auto dt_sib = internal_sibling->GetData();
          if (borrow_from_left) {
            left_dirty = true;
            for (int i = sz_tg - 1; i > 0; i--) *internal_target->EntryAt(i) = *internal_target->EntryAt(i - 1);
            *internal_target->EntryAt(0) = *internal_sibling->EntryAt(sz_sib);
            nk = &internal_target->EntryAt(0)->key;
          } else {
            right_dirty = true;
            *internal_target->EntryAt(sz_tg - 1) = *internal_sibling->EntryAt(0);
            for (int i = 0; i < sz_sib; i++) *internal_sibling->EntryAt(i) = *internal_sibling->EntryAt(i + 1);
            nk = &internal_sibling->EntryAt(0)->key;
          }
        }
        if (borrow_from_left) c_ip->EntryAt(target_page_index)->SetKey(nk);
        // current_data[target_page_index].first = nk;
        else
          c_ip->EntryAt(target_page_index + 1)->SetKey(nk);
      }
    } else {
      if (!(*nk == c_ip->EntryAt(target_page_index)->key)) {
        c_ip->EntryAt(target_page_index)->SetKey(nk);
        *modified = true;
      }
    }
    if (can_borrow) {
      if (borrow_from_left)
        c_ip->EntryAt(target_page_index)->SetKey(nk);
      else
        c_ip->EntryAt(target_page_index + 1)->SetKey(nk);
    }
    if (p_left) buffer_pool_manager_->UnpinPage(p_left->GetPageId(), left_dirty);
    if (p_right) buffer_pool_manager_->UnpinPage(p_right->GetPageId(), right_dirty);
    if (target_page) buffer_pool_manager_->UnpinPage(target_page->GetPageId(), child_modified);
    *newKey = &c_ip->EntryAt(0)->key;
    return c_ip->GetSize();
  }
}

BPlusTreeIndexIterator BPlusTree::Begin() {
  page_id_t next_page_id = root_page_id_;
  Page *p = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(p->GetData());
  while (!page->IsLeafPage()) {
    BPlusTreeInternalPage *internal_page = reinterpret_cast<BPlusTreeInternalPage *>(page);
    next_page_id = internal_page->EntryAt(0)->value;
    buffer_pool_manager_->UnpinPage(next_page_id, false);
    p = buffer_pool_manager_->FetchPage(next_page_id);
    page = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  BPlusTreeLeafPage *page_leaf = reinterpret_cast<BPlusTreeLeafPage *>(page);
  return BPlusTreeIndexIterator(this, page_leaf, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */

BPlusTreeIndexIterator BPlusTree::Begin(const IndexKey *key) {
  Page *p = buffer_pool_manager_->FetchPage(root_page_id_);
  if (p == nullptr) return End();
  BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  while (!bp->IsLeafPage()) {
    auto ibp = reinterpret_cast<BPlusTreeInternalPage *>(bp);
    // auto data = ibp->GetData();
    int l = 0, r = ibp->GetSize() - 1;
    while (l < r) {
      int mid = (l + r + 1) / 2;
      if (comparator_(ibp->KeyAt(mid), key) > 0)
        r = mid - 1;
      else
        l = mid;
    }
    page_id_t next = ibp->ValueAt(r);
    buffer_pool_manager_->UnpinPage(bp->GetPageId(), false);
    p = buffer_pool_manager_->FetchPage(next);
    bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  auto *c_lp = reinterpret_cast<BPlusTreeLeafPage *>(bp);
  // auto leaf_data = c_lp->GetData();
  int l = 0, r = c_lp->GetSize() - 1;
  int mid = (l + r) / 2;
  while (l < r) {
    mid = (l + r) / 2;
    // auto c = leaf_data[mid];
    if (comparator_(c_lp->KeyAt(mid), key) > 0)
      r = mid;
    else if (comparator_(c_lp->KeyAt(mid), key) < 0)
      l = mid + 1;
    else
      break;
  }
  mid = (l + r) / 2;
  if (*c_lp->KeyAt(mid) == *key) return BPlusTreeIndexIterator{this, c_lp, mid};
  return End();
}

BPlusTreeIndexIterator BPlusTree::FindLastSmaller(const IndexKey *key) {
  Page *p = buffer_pool_manager_->FetchPage(root_page_id_);
  if (p == nullptr) return End();
  BPlusTreePage *bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  while (!bp->IsLeafPage()) {
    auto ibp = reinterpret_cast<BPlusTreeInternalPage *>(bp);
    // auto data = ibp->GetData();
    int l = 0, r = ibp->GetSize() - 1;
    while (l < r) {
      int mid = (l + r + 1) / 2;
      if (comparator_(ibp->KeyAt(mid), key) > 0)
        r = mid - 1;
      else
        l = mid;
    }
    page_id_t next = ibp->ValueAt(r);
    buffer_pool_manager_->UnpinPage(bp->GetPageId(), false);
    p = buffer_pool_manager_->FetchPage(next);
    bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  auto *c_lp = reinterpret_cast<BPlusTreeLeafPage *>(bp);
  // auto leaf_data = c_lp->GetData();
  int l = -1, r = c_lp->GetSize() - 1;
  int mid = (l + r) / 2;
  while (l < r) {
    mid = (l + r + 1) / 2;
    // auto c = leaf_data[mid];
    if (comparator_(c_lp->KeyAt(mid), key) >= 0)
      r = mid - 1;
    else if (comparator_(c_lp->KeyAt(mid), key) < 0)
      l = mid;
    else
      break;
  }
  if (r < 0) return this->End();
  return BPlusTreeIndexIterator{this, c_lp, r};
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */

BPlusTreeIndexIterator BPlusTree::End() { return BPlusTreeIndexIterator{this, nullptr, -1}; }

Page *BPlusTree::FindLeafPage(const IndexKey &key, bool leftMost) { return nullptr; }

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */

void BPlusTree::UpdateRootPageId(int insert_record) {
  Page *p = buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID);
  IndexRootsPage *roots = reinterpret_cast<IndexRootsPage *>(p->GetData());
  if (insert_record) {
    roots->Insert(index_id_, root_page_id_);
  } else {
    roots->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */

void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
      }
    }
  }
}

/**
 * This function is for debug only, you don't need to modify
 */

void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}
