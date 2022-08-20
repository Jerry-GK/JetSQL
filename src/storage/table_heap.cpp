#include "storage/table_heap.h"
#include <iostream>
#include "common/config.h"
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  page_id_t pid=INVALID_PAGE_ID;
  //find the page to insert
  if(first_page_id_==INVALID_PAGE_ID)
  {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(pid));  // first tuple, get a new page from buffer pool
    if(pid==INVALID_PAGE_ID)//can't even create a new page
      return false;
    first_page_id_ = pid;
    last_page_id_ = pid;
    page->Init(pid,INVALID_PAGE_ID,log_manager_,txn);//initialize the new page
    page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);//single tuple must be able to insert (assumption)
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    page_heap_.push(make_pair(this,page->GetPageId()));
    return true;
  }
  else// not first tuple
  {
    uint32_t max_remain_space=0;
    if(!page_heap_.empty())
    {
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_heap_.top().second, false));
      max_remain_space=page->GetFreeSpaceRemaining();
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    }
    if(max_remain_space >= row.GetSerializedSize(schema_)+TablePage::SIZE_TUPLE)//enough space for insertion
    {
      auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_heap_.top().second, true));
      page_heap_.pop();
      page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      // ASSERT(, "logic error: enough space but insert failed!");
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
      page_heap_.push(make_pair(this,page->GetPageId()));
      return true;
    }
    else//create a new page to insert
    {
      auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(last_page_id_, true));
      page_id_t next_pid = INVALID_PAGE_ID;
      auto page_next = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_pid));
      if(next_pid==INVALID_PAGE_ID)
        return false;
      last_page->SetNextPageId(next_pid);
      page_next->Init(next_pid,last_page_id_,log_manager_,txn);//initialize the new page
      page_next->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);//single tuple must be able to insert (assumption)
      buffer_pool_manager_->UnpinPage(last_page_id_, true);
      last_page_id_ = next_pid;
      //page_heap_.push(make_pair(this,page_next->GetPageId()));
      //cout<<"page id = "<<page_next->GetPageId()<<"  table page id = "<<page_next->GetTablePageId()<<endl;
      buffer_pool_manager_->UnpinPage(page_next->GetPageId(), true);
      page_heap_.push(make_pair(this,page_next->GetPageId()));
      return true;
    }
  }
}

//implemented already
bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId(), true));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  //difficult--------------------(not well yet!)
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId(), true));//find the original page
  if(page==nullptr)
    return false;
  Row old_row(row);
  old_row.SetRowId(rid);
  if(!page->GetTuple(&old_row, schema_, txn, lock_manager_))
  {
    return false;
  }
  UPDATE_RESULT res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  if(res==SLOT_INVALID||res==TUPLE_DELETED)
    return false;
  else if(res==UPDATE_SUCCESS)
  {
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
  }
  else if(res==SPACE_NOT_ENOUGH)//space not enough for new tuple. new method to update
  {
    //not sure!
    if(!this->MarkDelete(rid, txn))
      return false;
    //copy row to new row
    Row new_row(row);
    RowId null_rid(INVALID_PAGE_ID, 0);
    new_row.SetRowId(null_rid);  // position is not determined
    if(!this->InsertTuple(new_row, txn))
      return false;
    buffer_pool_manager_->UnpinPage(new_row.GetRowId().GetPageId(), true);
    return true;
  }
  return false;//won't come here
}

void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId(), true));
  if(page==NULL)
    return;
  // Step2: Delete the tuple from the page.
  page->ApplyDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

//implemented already
void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId(), true));
  assert(page != nullptr);
  // Rollback the delete.
  page->RollbackDelete(rid, txn, log_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  page_id_t pid= this->GetFirstPageId();
  while(pid!=INVALID_PAGE_ID)
  {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(pid, false));
    page_id_t next_pid = page->GetNextPageId();
    buffer_pool_manager_->UnpinPage(pid, false);
    buffer_pool_manager_->DeletePage(pid);
    pid = next_pid;
  }
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  if(row->GetRowId().GetPageId() == INVALID_PAGE_ID)return false;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId(), false));
  if(page==nullptr)
  {
    return false;
  }
  bool ret = page->GetTuple(row, schema_, txn, lock_manager_);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return ret;
}

TableIterator TableHeap::Begin() {
  page_id_t fpid = GetFirstNotEmptyPageId();
  if(fpid==INVALID_PAGE_ID)
  {
    RowId rid(INVALID_PAGE_ID, 0);
    TableIterator ret(this, rid);
    return ret;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(fpid, false));
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  RowId rid;
  page->GetFirstTupleRid(&rid);
  TableIterator ret(this, rid);
  return ret;
}

TableIterator TableHeap::End() {
  RowId rid(INVALID_PAGE_ID, 0);//the rid for the end
  TableIterator ret(this, rid);
  return ret;
}

//added
TableIterator TableHeap::Find(RowId rid)
{
  TableIterator ret(this, rid);
  return ret;
}