#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {

}

TableIterator::TableIterator(TableHeap* tbp, RowId& rid) { 
  this->tbp = tbp;
  this->rid = rid;
}

TableIterator::TableIterator(const TableIterator &other) {
  if(this==&other)
    return;
  this->tbp=other.tbp;
  this->rid = other.rid;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const { 
  return tbp == itr.tbp && rid == itr.rid; 
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  Row *r = NULL;
  tbp->GetTuple(r, NULL);
  ASSERT(r != NULL, "Row * is NULL!");
  return *r;
}

Row *TableIterator::operator->() {
  Row *r = NULL;
  tbp->GetTuple(r, NULL);
  ASSERT(r != NULL, "Row * is NULL!");
  return r;
}

TableIterator &TableIterator::operator++() {
  auto page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->GetNextTupleRid(rid, &rid);
  return *this; 
}

TableIterator TableIterator::operator++(int) {
  TableIterator it_temp(*this);
  auto page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->GetNextTupleRid(rid, &rid);
  return it_temp;
}
