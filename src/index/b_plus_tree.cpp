#include <cstddef>
#include <cstring>
#include <exception>
#include <queue>
#include <string>
#include "common/config.h"
#include "glog/logging.h"
#include "index/b_plus_tree.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_page.h"
#include "page/index_roots_page.h"


INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
        : index_id_(index_id),
          buffer_pool_manager_(buffer_pool_manager),
          comparator_(comparator),
          leaf_max_size_(leaf_max_size),
          internal_max_size_(internal_max_size) {
  root_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Destroy() {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return false;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction) {
  Page *p = buffer_pool_manager_->FetchPage(root_page_id_);
  if(p == nullptr)return false;
  BPlusTreePage * bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  while(!bp->IsLeafPage()){
    auto ibp = reinterpret_cast<INTERNAL_PAGE_TYPE * >(bp);
    auto data = ibp->GetData();
    int l = 0,r = ibp->GetSize() - 1;
    while(l < r){
      int mid = (l + r + 1) /2 ;
      if(comparator_(data[mid].first,key) > 0)r = mid - 1;
      else l = mid;
    }
    page_id_t next = data[r].second;
    buffer_pool_manager_->UnpinPage(bp->GetPageId(), false);
    p = buffer_pool_manager_->FetchPage(next);
    bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  auto * current_leaf_page = reinterpret_cast<LEAF_PAGE_TYPE *>(bp);
  auto leaf_data = current_leaf_page->GetData();
  int l = 0,r = current_leaf_page->GetSize() - 1;
  int mid = (l + r) /2 ;
  while(l < r){
    mid = (l + r) /2 ;
    auto c = leaf_data[mid];
    if(comparator_(c.first,key) > 0)r = mid;
    else if(comparator_(c.first,key) < 0)l = mid + 1;
    else break;
  }
  mid = (l + r) /2 ;
  auto pair = leaf_data[mid];
  buffer_pool_manager_->UnpinPage(bp->GetPageId(), false);
  if(pair.first == key){
    result.push_back(pair.second);
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


INDEX_TEMPLATE_ARGUMENTS
BPlusTreePage * BPLUSTREE_TYPE::InternalInsert(BPlusTreePage * destination,const KeyType& key, const ValueType & value , KeyType & newKey,bool * found
,bool *modified){
  // a leaf node is met
  BPlusTreePage * splitted_page = nullptr;
  *found = false;
  if( destination->IsLeafPage()){
    auto * current_leaf_page = reinterpret_cast<LEAF_PAGE_TYPE *>(destination);
    // perform a binary search ~
    // find the first index whose key value is greater than or equal to k , if no such value , index = 0
    LEAF_MAPPING_TYPE *current_data = current_leaf_page->GetData();
    int l = 0,r = current_leaf_page->GetSize();
    while(l < r){
      int mid = (l + r) /2 ;
      auto c = current_data[mid];
      if(comparator_(c.first,key) > 0)r = mid;
      else if(comparator_(c.first,key) < 0)l = mid + 1;
      else{
        *found = true;
        return nullptr;
      }
    }
    int target_row_index = r;
    if(current_leaf_page->GetSize() == current_leaf_page->GetMaxSize()){
      // the leaf page is full , split it
      page_id_t split_page_id = INVALID_PAGE_ID;
      Page * split_page = buffer_pool_manager_->NewPage(split_page_id);

      auto split_leaf_page = reinterpret_cast<LEAF_PAGE_TYPE *>(split_page->GetData());
      split_leaf_page->Init(split_page_id);
      if(split_page == nullptr){
        ASSERT(0,"Bufferpool new leaf page failed!");
        return nullptr;
      }
      split_leaf_page->SetPageType(IndexPageType::LEAF_PAGE);
      split_leaf_page->SetPageId(split_page_id);
      split_leaf_page->SetSize(leaf_max_size_ + 1 - (leaf_max_size_ / 2));
      current_leaf_page->SetSize(leaf_max_size_ / 2);
      split_leaf_page->SetMaxSize(leaf_max_size_);
      split_leaf_page->SetNextPageId(current_leaf_page->GetNextPageId());
      current_leaf_page->SetNextPageId(split_page_id);
      split_leaf_page->SetParentPageId(current_leaf_page->GetParentPageId());
      auto split_data = split_leaf_page->GetData();

      for(int i = leaf_max_size_; i >= 0 ;i --){
        LEAF_MAPPING_TYPE current_pair;
        if(i < target_row_index)current_pair = current_data[i];
        else if(i == target_row_index)current_pair = {key,value};
        else current_pair = current_data[i - 1];

        if(i < internal_max_size_ / 2){
          // copy the leftmost floor(max/2) nodes to left node
          current_data[i] = current_pair;
        }else{
          // copy the rightmost max - floor(max/2) nodes to right node
          split_data[i - internal_max_size_/2] = current_pair;
          current_data[i] = {KeyType(),ValueType()};
        }
      }
      *modified = true;
      splitted_page = reinterpret_cast<BPlusTreePage *>(split_leaf_page);
      newKey = current_data[0].first;

    }else{
      // the leaf page is not null , simply insert
      for(int i = current_leaf_page->GetSize();i > target_row_index;i --)current_data[i] = current_data[i - 1];
      current_leaf_page->SetSize(current_leaf_page->GetSize() + 1);
      current_data[target_row_index] = {key,value};
      *modified = true;
      newKey = current_data[0].first;
    }
  }else{  // still an internal node
    page_id_t target_page_id = INVALID_PAGE_ID;
    //perform a binary search ~
    auto * current_internal_page = reinterpret_cast<INTERNAL_PAGE_TYPE * >(destination);
    // target_page_id = p_internal->Lookup(key ,comparator_ );
    // we should not use the Lookup method since some useful information is hidden
    // perform the binary search here.

    pair<KeyType,page_id_t> *current_data = current_internal_page->GetData();
    int l = 0,r = current_internal_page->GetSize() - 1;
    // find the last index whose key is smaller than or equal to key
    while(l < r){
      int mid = (l + r + 1) /2 ;
      if(comparator_(current_data[mid].first,key) > 0)r = mid - 1;
      else l = mid;
    }
    int target_page_index = r;
    target_page_id = current_internal_page->GetData()[target_page_index].second;
    if(target_page_id == INVALID_PAGE_ID){
      *found = false;
      return nullptr;
    }
    Page * target_page = buffer_pool_manager_->FetchPage(target_page_id);
    if(target_page == nullptr){
      *found = false;
      ASSERT(0,"Fetch BPlustree page failed!");
      return nullptr;
    }
    BPlusTreePage * target_bplus_page = reinterpret_cast<BPlusTreePage *>(target_page->GetData());
    bool child_found = false;
    bool child_modified = false;
    KeyType nk;

    BPlusTreePage * new_page = InternalInsert(target_bplus_page, key, value ,nk, &child_found, &child_modified);

    if(child_found){
      *found = true;
      return nullptr;
    }
    if(new_page != nullptr){ // a split happens !
      *modified = true;
      KeyType new_key;
      page_id_t new_page_id = new_page->GetPageId();
      // Page *new_page_page = buffer_pool_manager_->FetchPage(new_page_id);
      if( new_page->IsLeafPage()){
        auto new_leaf_page = reinterpret_cast<LEAF_PAGE_TYPE * >(new_page);
        new_key = new_leaf_page->GetData()[0].first;
      }
      else{
        auto new_internal_page = reinterpret_cast<INTERNAL_PAGE_TYPE *>(new_page);
        new_key = new_internal_page->GetData()[0].first;
      }
      if(current_internal_page->GetSize() == current_internal_page->GetMaxSize()){
        // continue splitting
        // create a new internal page
        page_id_t split_page_id = INVALID_PAGE_ID;
        Page *split_page = buffer_pool_manager_->NewPage(split_page_id);
        child_modified = true;
        if(split_page == nullptr){
          ASSERT(0,"New BPlustree page failed!");
          return nullptr;
        }
        // auto split_general_page = reinterpret_cast<BPlusTreePage *>(split_page->GetData());
        auto split_internal_page = reinterpret_cast<INTERNAL_PAGE_TYPE *>(split_page->GetData());
        split_internal_page->Init(split_page_id);
        auto split_data = split_internal_page->GetData();
        split_internal_page->SetPageType(IndexPageType::INTERNAL_PAGE);
        split_internal_page->SetPageId(split_page_id);
        current_internal_page->SetSize(internal_max_size_ / 2);
        split_internal_page->SetSize(internal_max_size_  + 1- (internal_max_size_ / 2));
        split_internal_page->SetMaxSize(internal_max_size_);
        split_internal_page->SetParentPageId(current_internal_page->GetParentPageId());
        for(int i = internal_max_size_; i >= 0 ;i --){
          INTERNAL_MAPPING_TYPE current_pair;
          if(i <= target_page_index)current_pair = current_data[i];
          else if(i == target_page_index + 1)current_pair = {new_key,new_page_id};
          else current_pair = current_data[i - 1];

          if(i < internal_max_size_ / 2){
            // copy the leftmost floor(max/2) nodes to left node
            current_data[i] = current_pair;
          }else{
            // copy the rightmost max - floor(max/2) nodes to right node
            split_data[i - internal_max_size_/2] = current_pair;
            current_data[i] = {KeyType(),INVALID_PAGE_ID};
          }
        }
        // and return the splitted page~

        splitted_page = reinterpret_cast<BPlusTreePage *>(split_internal_page);

      }else{
        current_internal_page->SetSize(current_internal_page->GetSize() + 1);
        // simply insert the new node and update links
        for(int i = current_internal_page->GetSize(); i > target_page_index ;i -- ){
          current_data[i] = current_data[i - 1];
        }
        current_data[target_page_index + 1] = {new_key , new_page_id};

      }
      buffer_pool_manager_->UnpinPage(new_page_id, true);
    }
    buffer_pool_manager_->UnpinPage(target_page_id, modified);
    current_data[target_page_index].first = nk;
    newKey = current_data[0].first;
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
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  if(root_page_id_ == INVALID_PAGE_ID){
    StartNewTree(key, value);
  }else{
    Page * root_page = buffer_pool_manager_->FetchPage(root_page_id_);
    page_id_t old_root_page_id = root_page_id_;
    if(root_page == nullptr){
      ASSERT(0,"Bplustree fetch root page failed");
      return false;
    }
    BPlusTreePage * root_general_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
    bool found = false;
    bool modified = false;
    KeyType nk;
    BPlusTreePage * new_page = InternalInsert(root_general_page,  key, value,nk, &found, &modified);
    if(found){
      return false;
    }
    if(new_page != nullptr){
      // a split was performed at the 2th level and we need to build a new root page
      modified = true;
      INTERNAL_MAPPING_TYPE head_pair_left,head_pair_right;
      if(new_page->IsLeafPage()){
        auto new_leaf_page_right = reinterpret_cast<LEAF_PAGE_TYPE *>(new_page);
        auto new_leaf_page_left = reinterpret_cast<LEAF_PAGE_TYPE *>(root_general_page);
        head_pair_right = {new_leaf_page_right->GetData()[0].first,new_page->GetPageId()};
        head_pair_left = {new_leaf_page_left->GetData()[0].first,root_general_page->GetPageId()};
      }else{
        auto new_leaf_page_right = reinterpret_cast<INTERNAL_PAGE_TYPE *>(new_page);
        auto new_leaf_page_left = reinterpret_cast<INTERNAL_PAGE_TYPE *>(root_general_page);
        head_pair_right = {new_leaf_page_right->GetData()[0].first,new_page->GetPageId()};
        head_pair_left = {new_leaf_page_left->GetData()[0].first,root_general_page->GetPageId()};
      }
      INTERNAL_PAGE_TYPE * new_root;
      page_id_t new_root_page_id;
      Page * new_root_page = buffer_pool_manager_->NewPage(new_root_page_id);
      if(new_root_page == nullptr){
        ASSERT(0,"BPlustree new root page failed!");
        return false;
      }
      root_page_id_ = new_root_page_id;
      new_root = reinterpret_cast<INTERNAL_PAGE_TYPE *>(new_root_page->GetData());
      new_root->Init(new_root_page_id);
      new_root->SetPageId(new_root_page_id);
      new_root->SetParentPageId(INVALID_PAGE_ID);
      new_root->SetPageType(IndexPageType::INTERNAL_PAGE);
      new_root->SetSize(2);
      new_root->SetMaxSize(internal_max_size_);
      root_general_page->SetParentPageId(new_root_page_id);
      auto data = new_root->GetData();
      data[0] = head_pair_left;
      data[1] = head_pair_right;
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  page_id_t root_page_id = INVALID_PAGE_ID;
  Page * p = buffer_pool_manager_->NewPage(root_page_id);
  if(p == nullptr){
    ASSERT(0,"Start new tree allocate page failed.");
    return;
  }
  LEAF_PAGE_TYPE * leafPage = reinterpret_cast<LEAF_PAGE_TYPE *>(p->GetData());
  leafPage->Init(root_page_id);
  leafPage->SetParentPageId(INVALID_PAGE_ID);
  leafPage->SetNextPageId(INVALID_PAGE_ID);
  leafPage->SetPageType(IndexPageType::LEAF_PAGE);
  leafPage->SetSize(1);
  leafPage->SetMaxSize(leaf_max_size_);
  root_page_id_ = root_page_id;
  leafPage->GetData()[0] = {key,value};
  buffer_pool_manager_->UnpinPage(root_page_id, true);

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  return false;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  return nullptr;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {

  Page * root_page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage * root_bplus_page = reinterpret_cast<BPlusTreePage *>(root_page->GetData());
  bool modified = false;
  KeyType nk{};
  int cnt = InternalRemove(root_bplus_page, key,nk, &modified);
  if(cnt == 0){
    buffer_pool_manager_->DeletePage(root_page_id_);
    root_page_id_ = INVALID_PAGE_ID;
    return;
  }
  if(!root_bplus_page->IsLeafPage()){
    auto * ip = reinterpret_cast<INTERNAL_PAGE_TYPE *>(root_bplus_page);
    ip->GetData()[0].first = nk;
  }

}


/**
 * @brief Recursively delete a key-value pair from destination node.If destination is an internal node, keep searching.
 * 
 * @param destination 
 * @param key the key to delete
 * @return int if the key is not found, return -1. Otherwise, return the new size of modified node. 
 */
INDEX_TEMPLATE_ARGUMENTS
int BPLUSTREE_TYPE::InternalRemove(BPlusTreePage * destination,const KeyType& key, KeyType &newKey,bool *modified){
  if(destination->IsLeafPage()){
    auto * current_leaf_page = reinterpret_cast<LEAF_PAGE_TYPE * >(destination);
    LEAF_MAPPING_TYPE *current_data = current_leaf_page->GetData();
    int current_size = current_leaf_page->GetSize();
    int l = 0,r = current_size - 1;
    // find the index whose key value is equal to key
    
    while(l < r){
      int mid = (l + r + 1) /2 ;
      if(comparator_(current_data[mid].first,key) > 0)r = mid - 1;
      else l = mid;
    }
    if(comparator_(current_data[r].first,key) != 0){
      cout << "Key " << key << " not found !" << endl; 
      return -1;
    }
    int target_row_index = r;
    for(int i = target_row_index;i < current_size - 1;i ++)
      current_data[i] = current_data[i + 1];
    // memmove(current_data + target_row_index , current_data + target_row_index  + 1,  );
    current_leaf_page->SetSize(current_size - 1);
    *modified = true;
    newKey = current_data[0].first;
    return current_size - 1;
  }else{
    // this is not a leaf page ,continue searching down
    page_id_t target_page_id = INVALID_PAGE_ID;
    //perform a binary search ~
    auto * current_internal_page = reinterpret_cast<INTERNAL_PAGE_TYPE * >(destination);
    // target_page_id = p_internal->Lookup(key ,comparator_ );
    // we should not use the Lookup method since some useful information is hidden
    // perform the binary search here.
    pair<KeyType,page_id_t> *current_data = current_internal_page->GetData();
    int l = 0,r = current_internal_page->GetSize() - 1;
    // find the last index whose key value is smaller than or equal key
    while(l < r){
      int mid = (l + r + 1) /2 ;
      if(comparator_(current_data[mid].first,key) > 0)r = mid - 1;
      else l = mid;
    }
    int target_page_index = r;
    target_page_id = current_internal_page->GetData()[target_page_index].second;
    if(target_page_id == INVALID_PAGE_ID)return -1;
    Page * target_page = buffer_pool_manager_->FetchPage(target_page_id);
    if(target_page == nullptr){
      ASSERT(0,"Fetch BPlustree page failed!");
      return -1;
    }
    BPlusTreePage * target_bplus_page = reinterpret_cast<BPlusTreePage *>(target_page->GetData());
    bool child_modified = false;
    KeyType nk{};
    int child_size = InternalRemove(target_bplus_page, key, nk,&child_modified);
    if(child_size == -1)return -1;
    Page *p_left = nullptr;
    Page *p_right = nullptr;
    bool can_merge = false;
    bool merge_with_left = false;
    bool can_borrow = false;
    bool borrow_from_left = false;
    int target_max_size = target_bplus_page->GetMaxSize();
    int target_min_size = target_max_size / 2;
    if(child_size < target_min_size){
      child_modified = true;
      // need to do some redistribution
      if(target_page_index > 0){// probably can be merged with left sib
        p_left = buffer_pool_manager_->FetchPage(current_internal_page->GetData()[target_page_index - 1].second);
        ASSERT(p_left ,"Fetch BPlustree page failed!");
        BPlusTreePage * left = reinterpret_cast<BPlusTreePage *>(p_left->GetData());
        if(left->GetSize() + child_size <= target_max_size){
          // can be merged with left sib!!
          can_merge = true;
          merge_with_left = true;
        }else {
          // otherwise we can borrow from right sibling
          can_borrow = true;
          borrow_from_left = true;
        }
      }
      else if(target_page_index < target_max_size - 1){// probably can be merged with right sib
        p_right = buffer_pool_manager_->FetchPage(current_internal_page->GetData()[target_page_index + 1].second);
        ASSERT(p_right ,"Fetch BPlustree page failed!");
        BPlusTreePage * right = reinterpret_cast<BPlusTreePage *>(p_right->GetData());
        if(right->GetSize() + child_size <= target_max_size){
          // can be merged with left sib!!
          can_merge = true;
          merge_with_left = false;
        }else{
          // otherwise we can borrow from left sibling
          can_borrow = true;
          borrow_from_left = false;
        }
      }

      if(can_merge){
        // a merge operation is needed
        *modified = true;
        char *src, * dest;
        size_t size ;
        int delete_index;
        if(target_bplus_page->IsLeafPage()){
          // merge a leaf page with its sibling
          auto leaf_target = reinterpret_cast<LEAF_PAGE_TYPE * >(target_bplus_page);
          auto leaf_sibling = reinterpret_cast<LEAF_PAGE_TYPE *>(merge_with_left ? p_left->GetData() : p_right->GetData());
          if(merge_with_left){
            src = reinterpret_cast<char *>(leaf_target->GetData() );
            dest = reinterpret_cast<char *>(leaf_sibling->GetData() + leaf_sibling->GetSize());
            size = sizeof(LEAF_MAPPING_TYPE) * leaf_target->GetSize();
            // merge current page into left page, delete current page
            delete_index = target_page_index;
            leaf_sibling->SetNextPageId(leaf_target->GetNextPageId());
            leaf_sibling->SetSize(leaf_target->GetSize() + leaf_sibling->GetSize());
            nk = leaf_sibling->GetData()[0].first;
          }else{
            src = reinterpret_cast<char *>(leaf_sibling->GetData());
            dest = reinterpret_cast<char *>(leaf_target->GetData() + leaf_target->GetSize());
            size = sizeof(LEAF_MAPPING_TYPE) * leaf_sibling->GetSize();
            // merge right page into current page, delete right page
            delete_index = target_page_index + 1;
            leaf_target->SetNextPageId(leaf_sibling->GetNextPageId());
            leaf_target->SetSize(leaf_target->GetSize() + leaf_sibling->GetSize());
            nk = leaf_target->GetData()[0].first;
          }
        }else{
          // merge an internal page with its sibling
          auto internal_target = reinterpret_cast<INTERNAL_PAGE_TYPE * >(target_bplus_page);
          auto internal_sibling = reinterpret_cast<INTERNAL_PAGE_TYPE *>(merge_with_left ? p_left->GetData() : p_right->GetData());
          if(merge_with_left){
            src = reinterpret_cast<char *>(internal_target->GetData());
            dest = reinterpret_cast<char *>(internal_sibling->GetData() + internal_sibling->GetSize());
            size = sizeof(INTERNAL_MAPPING_TYPE) * internal_target->GetSize();
            internal_sibling->SetSize(internal_sibling->GetSize() + internal_target->GetSize());
            delete_index = target_page_index;
            nk = internal_sibling->GetData()[0].first;
          }else{
            src = reinterpret_cast<char *>(internal_sibling->GetData());
            dest = reinterpret_cast<char *>(internal_target->GetData() + internal_target->GetSize());
            size = sizeof(INTERNAL_MAPPING_TYPE) * internal_sibling->GetSize();
            internal_target->SetSize(internal_sibling->GetSize() + internal_target->GetSize());
            delete_index = target_page_index + 1;
            nk = internal_target->GetData()[0].first;
          }
        }
        memcpy(dest,src,size);
        page_id_t page_to_delete = current_data[delete_index].second;
        for(int i = delete_index;i < current_internal_page->GetSize() - 1;i++) current_data[i] = current_data[i + 1];
        current_data[page_to_delete -1].first = nk;
        current_data[current_internal_page->GetSize() - 1] = INTERNAL_MAPPING_TYPE(); 
        current_internal_page->SetSize(current_internal_page->GetSize() - 1);
        // after merge , delete a page
        buffer_pool_manager_->DeletePage(page_to_delete);
      }
      else if(can_borrow){
        // simply borrow 1 from source to target , no further modification
        if(target_bplus_page->IsLeafPage()){
          auto leaf_target = reinterpret_cast<LEAF_PAGE_TYPE * >(target_bplus_page);
          auto leaf_sibling = reinterpret_cast<LEAF_PAGE_TYPE *>(borrow_from_left ? p_left->GetData() : p_right->GetData());
          int sz_sib = leaf_sibling->GetSize() - 1;
          int sz_tg = leaf_target->GetSize() + 1;
          leaf_sibling->SetSize(sz_sib);
          leaf_target->SetSize(sz_tg);
          auto dt_tg = leaf_target->GetData();
          auto dt_sib = leaf_sibling->GetData();
          if(borrow_from_left){
            for(int i = sz_tg - 1;i > 0;i--)dt_tg[i] = dt_tg[i - 1];
            dt_tg[0] = dt_sib[sz_sib];
            dt_sib[sz_sib] = LEAF_MAPPING_TYPE{};
            nk = dt_tg[0].first;
          }else{
            dt_tg[sz_tg - 1] = dt_sib[0];
            for(int i = 0;i<sz_sib;i++)dt_sib[i] = dt_sib[i + 1];
            nk = dt_sib[0].first;
          }
        }else{
          auto internal_target = reinterpret_cast<INTERNAL_PAGE_TYPE * >(target_bplus_page);
          auto internal_sibling = reinterpret_cast<INTERNAL_PAGE_TYPE *>(borrow_from_left ? p_left->GetData() : p_right->GetData());
          int sz_sib = internal_sibling->GetSize() - 1;
          int sz_tg = internal_target->GetSize() + 1;
          internal_sibling->SetSize(sz_sib);
          internal_target->SetSize(sz_tg);
          auto dt_tg = internal_target->GetData();
          auto dt_sib = internal_sibling->GetData();
          if(borrow_from_left){
            for(int i = sz_tg - 1;i > 0;i--)dt_tg[i] = dt_tg[i - 1];
            dt_tg[0] = dt_sib[sz_sib];
            dt_sib[sz_sib] = INTERNAL_MAPPING_TYPE{};
            nk = dt_tg[0].first;
          }else{
            dt_tg[sz_tg - 1] = dt_sib[0];
            for(int i = 0;i<sz_sib;i++)dt_sib[i] = dt_sib[i + 1];
            nk = dt_sib[0].first;
          }
        }
        if(borrow_from_left) current_data[target_page_index].first = nk;
        else current_data[target_page_index + 1].first = nk;
      }
    }else{
      current_data[target_page_index].first = nk;
    }
    newKey = current_data[0].first;
    return current_internal_page->GetSize();
  }
  return 0;
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  page_id_t next_page_id = root_page_id_;
  Page *p = buffer_pool_manager_->FetchPage(root_page_id_);

  BPlusTreePage * page = reinterpret_cast<BPlusTreePage *>(p->GetData());
  while(!page->IsLeafPage()){
    INTERNAL_PAGE_TYPE * internal_page = reinterpret_cast<INTERNAL_PAGE_TYPE *>(page);
    next_page_id = internal_page->GetData()[0].second;

    p = buffer_pool_manager_->FetchPage(next_page_id);
    page = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  LEAF_PAGE_TYPE *page_leaf = reinterpret_cast<LEAF_PAGE_TYPE *>(page);
  return INDEXITERATOR_TYPE(this,page_leaf,0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  Page *p = buffer_pool_manager_->FetchPage(root_page_id_);
  if(p == nullptr)return End();
  BPlusTreePage * bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  while(!bp->IsLeafPage()){
    auto ibp = reinterpret_cast<INTERNAL_PAGE_TYPE * >(bp);
    auto data = ibp->GetData();
    int l = 0,r = ibp->GetSize() - 1;
    while(l < r){
      int mid = (l + r + 1) /2 ;
      if(comparator_(data[mid].first,key) > 0)r = mid - 1;
      else l = mid;
    }
    page_id_t next = data[r].second;
    buffer_pool_manager_->UnpinPage(bp->GetPageId(), false);
    p = buffer_pool_manager_->FetchPage(next);
    bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
  }
  auto * current_leaf_page = reinterpret_cast<LEAF_PAGE_TYPE *>(bp);
  auto leaf_data = current_leaf_page->GetData();
  int l = 0,r = current_leaf_page->GetSize() - 1;
  int mid = (l + r) /2 ;
  while(l < r){
    mid = (l + r) /2 ;
    auto c = leaf_data[mid];
    if(comparator_(c.first,key) > 0)r = mid;
    else if(comparator_(c.first,key) < 0)l = mid + 1;
    else break;
  }
  mid = (l + r) /2 ;
  auto pair = leaf_data[mid];
  if(pair.first == key)return INDEXITERATOR_TYPE{this,current_leaf_page,mid};
  return End();

}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::End() {
  return INDEXITERATOR_TYPE{this,nullptr,-1};
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {

}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
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
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId()
          << "};\n";
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
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> "
          << internal_prefix
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
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
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
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << std::endl;
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

INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

template
class BPlusTree<int, int, BasicComparator<int>>;

template
class BPlusTree<GenericKey<4>, RowId, GenericComparator<4>>;

template
class BPlusTree<GenericKey<8>, RowId, GenericComparator<8>>;

template
class BPlusTree<GenericKey<16>, RowId, GenericComparator<16>>;

template
class BPlusTree<GenericKey<32>, RowId, GenericComparator<32>>;

template
class BPlusTree<GenericKey<64>, RowId, GenericComparator<64>>;
