#ifndef MINISQL_B_PLUS_TREE_H
#define MINISQL_B_PLUS_TREE_H

#include <queue>
#include <string>
#include <vector>

#include "common/config.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_page.h"
#include "transaction/transaction.h"
#include "index/index_iterator.h"

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

#define LEAF_PAGE_TYPE BPlusTreeLeafPage<KeyType,ValueType,KeyComparator>
#define INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>
#define INTERNAL_MAPPING_TYPE pair<KeyType,page_id_t>
#define LEAF_MAPPING_TYPE pair<KeyType,ValueType>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;
  friend class INDEXITERATOR_TYPE;

public:
  explicit BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  bool IsEmpty() const;

  // Insert a key-value pair into this B+ tree.
  bool Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);

  // return the value associated with a given key
  bool GetValue(const KeyType &key, std::vector<ValueType> &result, Transaction *transaction = nullptr);

  INDEXITERATOR_TYPE Begin();

  INDEXITERATOR_TYPE Begin(const KeyType &key);

  INDEXITERATOR_TYPE End();

  // expose for test purpose
  Page *FindLeafPage(const KeyType &key, bool leftMost = false);

  // used to check whether all pages are unpinned
  bool Check();

  // destroy the b plus tree
  void Destroy();

  bool CheckIntergrity(){
    queue<page_id_t> k;
    k.push(root_page_id_);
    k.push(INVALID_PAGE_ID);
    while(!k.empty()){
      page_id_t c = k.front();
      k.pop();
      if(c == INVALID_PAGE_ID){
        if(k.empty())break;
        else {
          k.push(INVALID_PAGE_ID);
          continue;
        }
      }
      Page *p = buffer_pool_manager_->FetchPage(c);
      BPlusTreePage * bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
      if(bp->IsLeafPage()){
        LEAF_PAGE_TYPE * lp = reinterpret_cast<LEAF_PAGE_TYPE *>(bp);
        for(int i =0;i< lp->GetSize();i++){
          if(i > 0){
            if(comparator_(lp->GetData()[i].first , lp->GetData()[i - 1].first) <= 0){
              return false;
            }
          }
        }
      }else{
        INTERNAL_PAGE_TYPE * ip = reinterpret_cast<INTERNAL_PAGE_TYPE *>(bp);
        for(int i =0;i< ip->GetSize();i++) {
          k.push(ip->GetData()[i].second);
          if(i > 0){
            if(comparator_(ip->GetData()[i].first , ip->GetData()[i - 1].first) <= 0){
              return false;
            }
          }
        }
      }
      buffer_pool_manager_->UnpinPage(c, false);
    }
    return true;
  }

  void PrintTree(std::ostream &out) {
    queue<page_id_t> k;
    k.push(root_page_id_);
    k.push(INVALID_PAGE_ID);
    while(!k.empty()){
      page_id_t c = k.front();
      k.pop();
      if(c == INVALID_PAGE_ID){
        if(k.empty())break;
        else {
          out << "\n";
          k.push(INVALID_PAGE_ID);
          continue;
        }
      }
      Page *p = buffer_pool_manager_->FetchPage(c);
      BPlusTreePage * bp = reinterpret_cast<BPlusTreePage *>(p->GetData());
      if(bp->IsLeafPage()){
        LEAF_PAGE_TYPE * lp = reinterpret_cast<LEAF_PAGE_TYPE *>(bp);
        for(int i =0;i< lp->GetSize();i++) out << lp->GetData()[i].first << " ";
        out << " | ";
      }else{
        INTERNAL_PAGE_TYPE * ip = reinterpret_cast<INTERNAL_PAGE_TYPE *>(bp);
        for(int i =0;i< ip->GetSize();i++) {
          out << ip->GetData()[i].first << " " ;
          k.push(ip->GetData()[i].second);
        }
        out << " | ";
      }
      buffer_pool_manager_->UnpinPage(c, false);
    }
    out << endl;
  }

private:

  
  BPlusTreePage * InternalInsert(BPlusTreePage * destination,const KeyType& key, const ValueType & value , KeyType & newkey, bool * found ,bool *modified);

  int  InternalRemove(BPlusTreePage * destination,const KeyType& key,KeyType &newKey, bool * modified);


  // useless function
  void StartNewTree(const KeyType &key, const ValueType &value);

  // useless function
  bool InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr);

  // useless function
  void InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                        Transaction *transaction = nullptr);

  // useless function
  template<typename N>
  N *Split(N *node);

  // useless function
  template<typename N>
  bool CoalesceOrRedistribute(N *node, Transaction *transaction = nullptr);

  // useless function
  template<typename N>
  bool Coalesce(N **neighbor_node, N **node, BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent,
                int index, Transaction *transaction = nullptr);

  // useless function
  template<typename N>
  void Redistribute(N *neighbor_node, N *node, int index);

  bool AdjustRoot(BPlusTreePage *node);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  index_id_t index_id_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;
};

#endif  // MINISQL_B_PLUS_TREE_H
