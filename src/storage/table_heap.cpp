#include "storage/table_heap.h"
#include <iostream>
bool TableHeap::InsertTuple(Row &row, Transaction *txn) {
  page_id_t pid=INVALID_PAGE_ID;
  //find the page to insert
  if(first_page_id_==INVALID_PAGE_ID)
  {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(pid));  // first tuple, get a new page from buffer pool
    if(pid==INVALID_PAGE_ID)//can't even create a new page
      return false;
    first_page_id_ = pid;
    page->WLatch();
    page->Init(pid,INVALID_PAGE_ID,log_manager_,txn);//initialize the new page
    page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);//single tuple must be able to insert (assumption)
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    //std::cout<<"page id = "<<row.GetRowId().GetPageId()<<" slot num = "<<row.GetRowId().GetSlotNum()<<std::endl;
    return true;
  }
  else 
  {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));//get the fitst page
    while(1)
    {
      page->WLatch();
      bool suc = page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
      page->WUnlatch();
      if(suc==true)
      {
        buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
        //std::cout<<"page id = "<<row.GetRowId().GetPageId()<<"slot num = "<<row.GetRowId().GetSlotNum()<<std::endl;
        return true; 
      }
      //unable to insert the current page
      buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);//unpin the current page first
      page_id_t next_pid = page->GetNextPageId();
      if(next_pid==INVALID_PAGE_ID)//the last page is still full, create a new page.
      {
        auto page_next = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(next_pid));
        if(next_pid==INVALID_PAGE_ID)//can't even create a new page
          return false;
        page->SetNextPageId(next_pid);
        page_next->WLatch();
        page_next->Init(next_pid,page->GetPageId(),log_manager_,txn);//initialize the new page
        page_next->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);//single tuple must be able to insert (assumption)
        page_next->WUnlatch();
        //std::cout<<"page id = "<<row.GetRowId().GetPageId()<<"slot num = "<<row.GetRowId().GetSlotNum()<<std::endl;
        buffer_pool_manager_->UnpinPage(page_next->GetTablePageId(), true);
        return true; 
      }
      page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(next_pid));  // fetch next page if inserted failed(full page)
      ASSERT(page != nullptr, "logic error!");
    }
  }
}

//implemented already
bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

bool TableHeap::UpdateTuple(const Row &row, const RowId &rid, Transaction *txn) {
  //difficult--------------------(not well yet!)
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));//find the original page
  if(page==nullptr)
    return false;
  Row old_row(rid);
  page->WLatch();
  if(!page->GetTuple(&old_row, schema_, txn, lock_manager_))
  {
    page->WUnlatch();
    return false;
  }
  UPDATE_RESULT res = page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_);
  page->WUnlatch();
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
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  if(page==NULL)
    return;
  // Step2: Delete the tuple from the page.
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

//implemented already
void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback the delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::FreeHeap() {
  SimpleMemHeap heap;
  heap.Free(this);//like this?
}

bool TableHeap::GetTuple(Row *row, Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if(page==nullptr)
    return false;
  page->WLatch();
  bool ret = page->GetTuple(row, schema_, txn, lock_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return ret;
}

TableIterator TableHeap::Begin() {
  page_id_t fpid = GetFirstPageId();
  if(fpid==INVALID_PAGE_ID)
  {
    RowId rid(INVALID_PAGE_ID, 0);
    TableIterator ret(this, rid);
    return ret;
  }
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(fpid));
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
