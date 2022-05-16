#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "index/b_plus_tree.h"
#include "index/index_iterator.h"

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator()
 : tree_(nullptr),node_(nullptr),index_offset_(-1) {
   // this is an invalid iterator
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::IndexIterator(BPlusTree<KeyType, ValueType, KeyComparator> * tree, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * node, int offset)
 : tree_(tree),node_(node),index_offset_(offset) {
   // this is a valid iterator
}


INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE::~IndexIterator() {
  // just unpin the page
  if(tree_ && node_) tree_->buffer_pool_manager_->UnpinPage(node_->GetPageId(), false);
}
INDEX_TEMPLATE_ARGUMENTS MappingType *INDEXITERATOR_TYPE::operator->(){
  return node_->GetData() + index_offset_;
}

INDEX_TEMPLATE_ARGUMENTS MappingType &INDEXITERATOR_TYPE::operator*() {
  auto data = node_->GetData();
  return data[index_offset_];
}

INDEX_TEMPLATE_ARGUMENTS INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if(index_offset_ < node_->GetSize() - 1){
    this->index_offset_ += 1;
  }else{
    // need to fetch a new page
    this->index_offset_ = 0;
    Page *p = tree_->buffer_pool_manager_->FetchPage(node_->GetNextPageId());
    this->node_ = reinterpret_cast<BPlusTreeLeafPage<KeyType,ValueType,KeyComparator> * >(p->GetData());
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const {
  MappingType m1 = node_->GetData()[index_offset_];
  MappingType m2 = itr.node_->GetData()[itr.index_offset_];
  return tree_->comparator_(m1.first,m2.first) == 0;
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const {
  auto it = *this;
  return ! ( it == itr);
}

template
class IndexIterator<int, int, BasicComparator<int>>;

template
class IndexIterator<GenericKey<4>, RowId, GenericComparator<4>>;

template
class IndexIterator<GenericKey<8>, RowId, GenericComparator<8>>;

template
class IndexIterator<GenericKey<16>, RowId, GenericComparator<16>>;

template
class IndexIterator<GenericKey<32>, RowId, GenericComparator<32>>;

template
class IndexIterator<GenericKey<64>, RowId, GenericComparator<64>>;
