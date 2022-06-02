#include "storage/table_iterator.h"
#include "common/config.h"
#include "common/macros.h"
#include "storage/table_heap.h"
#include "utils/mem_heap.h"

TableIterator::TableIterator() {
  this->heap_ = nullptr;
  tbp = nullptr;
  rid.Set(INVALID_PAGE_ID, 0);
  row = nullptr;
}

TableIterator::TableIterator(TableHeap *tbp, const RowId &rid) {
  this->tbp = tbp;
  this->rid = rid;
  if (rid.GetPageId() == INVALID_PAGE_ID || tbp == nullptr) {
    this->row = nullptr;
    this->heap_ = nullptr;
    return;
  }
  this->heap_ = tbp->heap_;
  this->row = ALLOC_P(heap_, Row)(rid, heap_);
  tbp->GetTuple(this->row, nullptr);
}

TableIterator::TableIterator(const TableIterator &other) : TableIterator(other.tbp, other.rid) {}

TableIterator::~TableIterator() {
  if(this->row){
    this->row->~Row();
    heap_->Free(this->row);
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {
  // shouldn't we compare the values in each field ?
  return tbp == itr.tbp && rid == itr.rid;
}

bool TableIterator::operator!=(const TableIterator &itr) const { return !(*this == itr); }

const Row &TableIterator::operator*() {
  // if(row!=nullptr)
  //   delete row;//careful
  // row = nullptr;
  // row = new Row(rid);         // delete while deconstruction
  // tbp->GetTuple(row, nullptr);  // regardless of txn (controled by upper level?)
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
  if (this->row) this->row->~Row();
  auto page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page->GetNextTupleRid(rid, &rid)) {
    // do not forget to unpin the page
    tbp->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  } else  // no next tuple in this page
  {
    tbp->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    if (page->GetNextPageId() == INVALID_PAGE_ID)  // no more page too
    {
      RowId new_rid(INVALID_PAGE_ID, 0);
      this->rid = new_rid;

    } else  // return the first iterator to the first tuple in the next page
    {
      auto next_page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      RowId new_rid;
      next_page->GetFirstTupleRid(
          &new_rid);  // if no first tuple, new_rid will be set to (INVALID_PAGE_ID, 0) in this function
      // get the first iterator to the first tuple in the next page
      this->rid = new_rid;
      tbp->buffer_pool_manager_->UnpinPage(next_page->GetPageId(), false);
    }
  }
  *(this->row) = Row(rid, heap_);
  tbp->GetTuple(this->row, nullptr);
  return *this;
}

TableIterator TableIterator::operator++(int) {
  ASSERT(rid.GetPageId() != INVALID_PAGE_ID, "++ for invalid rowid");
  TableIterator it_temp(*this);
  if (this->row) this->row->~Row();
  auto page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if (page->GetNextTupleRid(rid, &rid)) {
    // do not forget to unpin the page
    tbp->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  } else  // no next tuple in this page
  {
    tbp->buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    if (page->GetNextPageId() == INVALID_PAGE_ID)  // no more page too
    {
      RowId new_rid(INVALID_PAGE_ID, 0);
      this->rid = new_rid;
    } else  // return the first iterator to the first tuple in the next page
    {
      auto next_page = reinterpret_cast<TablePage *>(tbp->buffer_pool_manager_->FetchPage(page->GetNextPageId()));
      RowId new_rid;
      next_page->GetFirstTupleRid(
          &new_rid);  // if no first tuple, new_rid will be set to (INVALID_PAGE_ID, 0) in this function
      // get the first iterator to the first tuple in the next page
      this->rid = new_rid;
      tbp->buffer_pool_manager_->UnpinPage(next_page->GetPageId(), false);
    }
  }
  *(this->row) = Row(rid, heap_);
  tbp->GetTuple(this->row, nullptr);
  return it_temp;
}
