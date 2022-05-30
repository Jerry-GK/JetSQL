#ifndef MINISQL_INDEX_ITERATOR_H
#define MINISQL_INDEX_ITERATOR_H

#include "buffer/buffer_pool_manager.h"
#include "record/row.h"

struct BLeafEntry;
class BPlusTree;
class BPlusTreeLeafPage;

class BPlusTreeIndexIterator {
 public:
  // you may define your own constructor based on your member variables
  explicit BPlusTreeIndexIterator();
  BPlusTreeIndexIterator(BPlusTree *tree, BPlusTreeLeafPage *node, int offset);

  ~BPlusTreeIndexIterator();

  /** Return the key/value pair this iterator is currently pointing at. */
  BLeafEntry &operator*();

  BLeafEntry *operator->();

  /** Move to the next key/value pair.*/
  BPlusTreeIndexIterator &operator++();

  bool IsNull() const;

  /** Return whether two iterators are equal */
  bool operator==(const BPlusTreeIndexIterator &itr) const;

  /** Return whether two iterators are not equal. */
  bool operator!=(const BPlusTreeIndexIterator &itr) const;

 private:
  // add your own private member variables here
  BPlusTree *tree_;
  Schema * key_schema_;
  BPlusTreeLeafPage *node_;
  int index_offset_;
};

#endif  // MINISQL_INDEX_ITERATOR_H
