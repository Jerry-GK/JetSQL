#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "common/rowid.h"
#include "record/row.h"
#include "utils/mem_heap.h"


class TableHeap;

class TableIterator {

public:
  // you ma y  define your own constructor based on your member variables
  explicit TableIterator();

  explicit TableIterator(TableHeap* tbp,const RowId& rid);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const;

  bool operator!=(const TableIterator &itr) const;

  const Row &operator*();

  Row *operator->();

  TableIterator &operator++();

  TableIterator operator++(int);

private:
  // add your own private member variables here
  TableHeap *tbp;
  RowId rid;
  MemHeap *heap_;
  Row *row;//allocate space for row while do * and ->, based on RowId. (temporary pointer)
};

#endif //MINISQL_TABLE_ITERATOR_H
