#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "buffer/buffer_pool_manager.h"
#include "page/b_plus_tree_leaf_page.h"

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class BPlusTree ;

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  explicit IndexIterator();
  IndexIterator(BPlusTree<KeyType, ValueType, KeyComparator> * tree, BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * node, int offset);

  ~IndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  MappingType &operator*();

  MappingType *operator->();

  /** Move to the next key/value pair.*/
  IndexIterator &operator++();

  /** Return whether two iterators are equal */
  bool operator==(const IndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const IndexIterator &itr) const;

private:
  // add your own private member variables here
  BPlusTree<KeyType, ValueType, KeyComparator> * tree_;
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> * node_;
  int index_offset_;
};


#endif //MINISQL_INDEX_ITERATOR_H
