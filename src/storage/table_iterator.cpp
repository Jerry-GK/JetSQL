#include "common/macros.h"
#include "storage/table_iterator.h"
#include "storage/table_heap.h"

TableIterator::TableIterator() {
  tbp=nullptr;
  rid.Set(INVALID_PAGE_ID, 0);
  row = nullptr;
}

TableIterator::TableIterator(TableHeap* tbp, RowId& rid) { 
  this->tbp = tbp;
  this->rid = rid;
  this->row = nullptr;
}

TableIterator::TableIterator(const TableIterator &other) {
  if(this==&other)
    return;
  this->tbp=other.tbp;
  this->rid = other.rid;
  this->row = nullptr;
  // not copy row
}

TableIterator::~TableIterator() {
  delete row;
}

bool TableIterator::operator==(const TableIterator &itr) const {
  //shouldn't we compare the values in each field ? 
  return tbp == itr.tbp && rid == itr.rid;
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  // if(row!=nullptr)
  //   delete row;//careful
  // row = nullptr;
  // row = new Row(rid);         // delete while deconstruction
  // tbp->GetTuple(row, nullptr);  // regardless of txn (controled by upper level?)
  // std::cout << row->GetRowId().GetPageId() << std::endl;
  return *row;
}

Row *TableIterator::operator->() {
  // if(row!=nullptr)
  //   delete row;
  // row = nullptr;
  // row = new Row(rid);         // delete while deconstruction
  // tbp->GetTuple(row, nullptr);//regardless of txn (controled by upper level?)
  return row;
}

TableIterator &TableIterator::operator++() {
  ASSERT(rid.GetPageId() != INVALID_PAGE_ID, "++ for invalid rowid");
  auto page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page->GetNextTupleRid(rid, &rid)){
    // do not forget to unpin the page
    tbp->buffer_pool_manager_->UnpinPage(rid.GetPageId(), false);
    return *this;
  }
  else//no next tuple in this page
  {
    if(page->GetNextPageId()==INVALID_PAGE_ID)//no more page too
    {
      RowId new_rid(INVALID_PAGE_ID, 0);
      this->rid = new_rid;
      return *this;
    }
    else//return the first iterator to the first tuple in the next page
    {
      auto next_page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      RowId new_rid;
      next_page->GetFirstTupleRid(&new_rid);//if no first tuple, new_rid will be set to (INVALID_PAGE_ID, 0) in this function
      //get the first iterator to the first tuple in the next page
      this->rid = new_rid;
      return *this;
    }
  }
}

TableIterator TableIterator::operator++(int) {
  ASSERT(rid.GetPageId() != INVALID_PAGE_ID, "++ for invalid rowid");
  TableIterator it_temp(*this);
  auto page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page->GetNextTupleRid(rid, &rid))
    return *this;
  else//no next tuple in this page
  {
    if(page->GetNextPageId()==INVALID_PAGE_ID)//no more page too
    {
      RowId new_rid(INVALID_PAGE_ID, 0);
      this->rid = new_rid;
      return *this;
    }
    else//return the first iterator to the first tuple in the next page
    {
      auto next_page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      RowId new_rid;
      next_page->GetFirstTupleRid(&new_rid);//if no first tuple, new_rid will be set to (INVALID_PAGE_ID, 0) in this function
      //get the first iterator to the first tuple in the next page
      this->rid = new_rid;
      return *this;
    }
  }
  return it_temp;
}
