#ifndef MINISQL_TABLE_HEAP_H
#define MINISQL_TABLE_HEAP_H

#include "buffer/buffer_pool_manager.h"
#include "page/table_page.h"
#include "storage/table_iterator.h" 
#include "transaction/log_manager.h"
#include "transaction/lock_manager.h"
#include <queue>

class TableHeap {
  friend class TableIterator;

public:
  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, Schema *schema, Transaction *txn,
                           LogManager *log_manager, LockManager *lock_manager, MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableHeap));
    return new(buf) TableHeap(buffer_pool_manager, schema, txn, log_manager, lock_manager);
  }

  static TableHeap *Create(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                           LogManager *log_manager, LockManager *lock_manager, MemHeap *heap) {
    void *buf = heap->Allocate(sizeof(TableHeap));
    return new(buf) TableHeap(buffer_pool_manager, first_page_id, schema, log_manager, lock_manager);
  }

  ~TableHeap() {
    schema_->~Schema();
    delete heap_;
  }

  /**
   * Insert a tuple into the table. If the tuple is too large (>= page_size), return false.
   * @param[in/out] row Tuple Row to insert, the rid of the inserted tuple is wrapped in object row
   * @param[in] txn The transaction performing the insert
   * @return true iff the insert is successful
   */
  bool InsertTuple(Row &row, Transaction *txn);

  /**
   * Mark the tuple as deleted. The actual delete will occur when ApplyDelete is called.
   * @param[in] rid Resource id of the tuple of delete
   * @param[in] txn Transaction performing the delete
   * @return true iff the delete is successful (i.e the tuple exists)
   */
  bool MarkDelete(const RowId &rid, Transaction *txn);

  /**
   * if the new tuple is too large to fit in the old page, return false (will delete and insert)
   * @param[in] row Tuple of new row
   * @param[in] rid Rid of the old tuple
   * @param[in] txn Transaction performing the update
   * @return true is update is successful.
   */
  bool UpdateTuple(const Row &row, const RowId &rid, Transaction *txn);

  /**
   * Called on Commit/Abort to actually delete a tuple or rollback an insert.
   * @param rid Rid of the tuple to delete
   * @param txn Transaction performing the delete.
   */
  void ApplyDelete(const RowId &rid, Transaction *txn);

  /**
   * Called on abort to rollback a delete.
   * @param[in] rid Rid of the deleted tuple.
   * @param[in] txn Transaction performing the rollback
   */
  void RollbackDelete(const RowId &rid, Transaction *txn);

  /**
   * Read a tuple from the table.
   * @param[in/out] row Output variable for the tuple, row id of the tuple is wrapped in row
   * @param[in] txn transaction performing the read
   * @return true if the read was successful (i.e. the tuple exists)
   */
  bool GetTuple(Row *row, Transaction *txn);

  /**
   * Free table heap and release storage in disk file
   */
  void FreeHeap();

  /**
   * @return the begin iterator of this table
   */
  TableIterator Begin();

  /**
   * @return the end iterator of this table
   */
  TableIterator End();

  /**
   * @return the id of the first page of this table
   */
  inline page_id_t GetFirstPageId() const { return first_page_id_; }

  MemHeap * GetMemHeap()const {return heap_;}

  //my added function
  TableIterator Find(RowId rid);

 private:
  /**
   * create table heap and initialize first page
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, Schema *schema, Transaction *txn,
                     LogManager *log_manager, LockManager *lock_manager) :
          buffer_pool_manager_(buffer_pool_manager),
          schema_(schema),
          log_manager_(log_manager),
          lock_manager_(lock_manager) {
    //add my code
    first_page_id_ = INVALID_PAGE_ID;
    heap_ = new UsedHeap;
  };

  /**
   * load existing table heap by first_page_id
   */
  explicit TableHeap(BufferPoolManager *buffer_pool_manager, page_id_t first_page_id, Schema *schema,
                     LogManager *log_manager, LockManager *lock_manager)
          : buffer_pool_manager_(buffer_pool_manager),
            first_page_id_(first_page_id),
            schema_(schema),
            log_manager_(log_manager),
            lock_manager_(lock_manager) {
    heap_ = new UsedHeap;
    if(first_page_id != INVALID_PAGE_ID){
      Page * p = buffer_pool_manager_->FetchPage(first_page_id);
      buffer_pool_manager_->UnpinPage(p->GetPageId(), false);
      page_heap_.push(make_pair(this,first_page_id));
      last_page_id_ = first_page_id;
    }
  }

class cmp
{
public:
  bool operator()(pair<TableHeap*,page_id_t> p1, pair<TableHeap*,page_id_t> p2)
  {
    auto page1 = reinterpret_cast<TablePage *>(p1.first->buffer_pool_manager_->FetchPage(p1.second));
    auto page2 = reinterpret_cast<TablePage *>(p2.first->buffer_pool_manager_->FetchPage(p2.second));
    bool ret = page1->GetFreeSpaceRemaining() < page2->GetFreeSpaceRemaining();
    p1.first->buffer_pool_manager_->UnpinPage(page1->GetTablePageId(), false);
    p2.first->buffer_pool_manager_->UnpinPage(page2->GetTablePageId(), false);
    return ret;
  }
};

private:
  BufferPoolManager *buffer_pool_manager_;
  page_id_t first_page_id_;
  Schema *schema_;
  LogManager *log_manager_;
  LockManager *lock_manager_;

  //add max heap for the remaining size of pages
  priority_queue<pair<TableHeap*,page_id_t>, vector<pair<TableHeap* ,page_id_t>>, cmp> page_heap_;
  page_id_t last_page_id_;
  MemHeap * heap_;
};

#endif  // MINISQL_TABLE_HEAP_H
