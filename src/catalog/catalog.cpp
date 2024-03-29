#include "catalog/catalog.h"
#include <cstddef>
#include <cstdint>
#include "common/config.h"
#include "common/dberr.h"
#include "storage/table_iterator.h"

void CatalogMeta::SerializeTo(char *buf) const {
  uint32_t *buf_int = reinterpret_cast<uint32_t *>(buf);
  *(buf_int++) = CATALOG_METADATA_MAGIC_NUM;
  *(buf_int++) = table_meta_pages_.size();

  *(buf_int++) = index_meta_pages_.size();

  for (auto it = table_meta_pages_.begin(); it != table_meta_pages_.end(); it++) {
    *(buf_int++) = it->first;
    *(buf_int++) = it->second;
  }
  for (auto it = index_meta_pages_.begin(); it != index_meta_pages_.end(); it++) {
    *(buf_int++) = it->first;
    *(buf_int++) = it->second;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf, MemHeap *heap) {
  uint32_t *buf_int = reinterpret_cast<uint32_t *>(buf);
  if (*(buf_int++) != CATALOG_METADATA_MAGIC_NUM) {return nullptr;}
  CatalogMeta *res = new (heap->Allocate(sizeof(CatalogMeta)))(CatalogMeta);
  uint32_t num_table = *(buf_int++);
  uint32_t num_index = *(buf_int++);
  for (size_t i = 0; i < num_table; i++) {
    uint32_t table_id = *(buf_int++);
    uint32_t pid = *(buf_int++);
    res->table_meta_pages_[table_id] = pid;
  }
  for (size_t i = 0; i < num_index; i++) {
    uint32_t index_id = *(buf_int++);
    uint32_t pid = *(buf_int++);
    res->index_meta_pages_[index_id] = pid;
  }
  return res;
}

uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t sz_magic = sizeof(CATALOG_METADATA_MAGIC_NUM);
  uint32_t sz_tablemap = table_meta_pages_.size() * (sizeof(page_id_t) + sizeof(table_id_t));
  uint32_t sz_indexmap = index_meta_pages_.size() * (sizeof(index_id_t) + sizeof(page_id_t));
  return sz_magic + sz_indexmap + sz_tablemap + 2 * sizeof(uint32_t);
}

CatalogMeta::CatalogMeta() {}
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,

                               LogManager *log_manager, bool init) 
    : buffer_pool_manager_(buffer_pool_manager),
      lock_manager_(lock_manager),
      log_manager_(log_manager),
      heap_(new UsedHeap()) {
  latch_.lock();
  // simply load catalog meta and load pages & indexes ?
  Page *p;
  if (init) {
    catalog_meta_ = CatalogMeta::NewInstance(heap_);
  } else {
    p = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID, false);
    if (p == nullptr) 
    {
      throw -1;
    }
    // ASSERT(p != nullptr, "No catalog metapage for the existed database!");
    catalog_meta_ = CatalogMeta::DeserializeFrom(p->GetData(), heap_);
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
  }

  ASSERT(catalog_meta_, "Catalog meta deserialize failed!");
  next_table_id_ = 0;
  for (auto it = catalog_meta_->table_meta_pages_.begin(); it != catalog_meta_->table_meta_pages_.end(); it++) {
    LoadTable(it->first, it->second);
    if (next_table_id_ <= it->first) next_table_id_ = it->first + 1;
  }

  next_index_id_ = 0;
  for (auto it = catalog_meta_->index_meta_pages_.begin(); it != catalog_meta_->index_meta_pages_.end(); it++) {
    LoadIndex(it->first, it->second);
    if (next_index_id_ <= it->first) next_index_id_ = it->first + 1;
  }

  FlushCatalogMetaPage();
  latch_.unlock();
  return;
}

CatalogManager::~CatalogManager() {
  latch_.lock();
  FlushCatalogMetaPage();
  for (auto &it : tables_) {
    if (it.second) {
      // serialize back before deconstruct
      Page *meta_page;
      meta_page = buffer_pool_manager_->FetchPage(catalog_meta_->table_meta_pages_[it.first], true);
      it.second->table_meta_->SerializeTo(meta_page->GetData());
      buffer_pool_manager_->UnpinPage(catalog_meta_->table_meta_pages_[it.first], true);

      it.second->~TableInfo();
    }
  }
  for (auto &it : indexes_) {
    if (it.second) {
      // serialize back before deconstruct
      Page *meta_page;
      meta_page = buffer_pool_manager_->FetchPage(catalog_meta_->index_meta_pages_[it.first], true);
      it.second->index_meta_->SerializeTo(meta_page->GetData());
      buffer_pool_manager_->UnpinPage(catalog_meta_->index_meta_pages_[it.first], true);

      it.second->~IndexInfo();
    }
  }
  catalog_meta_->~CatalogMeta();
  delete heap_;
  latch_.unlock();
  return;
}

dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Transaction *txn,
                                    TableInfo *&table_info) {
  latch_.lock();
  // 1. Check if the table already exists.
  auto it = table_names_.find(table_name);
  if (it != table_names_.end()) {latch_.unlock(); return DB_TABLE_ALREADY_EXIST;}
  // 2. Allocate table meta page.
  page_id_t meta_page_id;
  page_id_t first_page_id;
  Page *meta_page;
  Page *table_first_page;

  if (!(meta_page = buffer_pool_manager_->NewPage(meta_page_id))) {latch_.unlock(); return DB_FAILED;}
  buffer_pool_manager_->UnpinPage(meta_page_id, true);
  if (!(table_first_page = buffer_pool_manager_->NewPage(first_page_id))) {latch_.unlock(); return DB_FAILED;}
  TablePage *tbp = reinterpret_cast<TablePage *>(table_first_page->GetData());
  tbp->Init(first_page_id, INVALID_PAGE_ID, log_manager_, nullptr);
  // 3. create table heap
  buffer_pool_manager_->UnpinPage(first_page_id, true);
  Schema *table_schema = Schema::DeepCopySchema(schema, heap_);
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, first_page_id, table_schema, log_manager_, lock_manager_, heap_);

  if (table_heap == nullptr) {
    buffer_pool_manager_->DeletePage(meta_page_id);
    latch_.unlock();
    return DB_FAILED;
  }
  // 4. create table meta data.
  table_id_t tid = next_table_id_;

  TableMetadata *table_meta = TableMetadata::Create(tid, table_name, table_heap->GetFirstPageId(), 0,
                                                    Schema::DeepCopySchema(schema, heap_), heap_);
                                                    
  if (!(meta_page = buffer_pool_manager_->FetchPage(meta_page_id, true))) {
    //buffer_pool_manager_->UnpinPage(meta_page_id, true);
    latch_.unlock(); 
    return DB_FAILED;
  }
  table_meta->SerializeTo(meta_page->GetData());
  buffer_pool_manager_->UnpinPage(meta_page_id, true);
  if (table_meta == nullptr) {
    table_heap->FreeHeap();
    buffer_pool_manager_->DeletePage(meta_page_id);
    latch_.unlock();
    return DB_FAILED;
  }
  TableInfo *tinfo = TableInfo::Create(heap_);
  tinfo->Init(table_meta, table_heap);
  // 5 .update catalog meta data
  catalog_meta_->table_meta_pages_[tid] = meta_page_id;
  tables_[tid] = tinfo;
  index_names_[table_name] = {};
  table_names_[table_name] = tid;
  next_table_id_ += 1;

  // 6 .store result
  table_info = tinfo;

  FlushCatalogMetaPage();//flush to buffer
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  latch_.lock();
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) {latch_.unlock(); return DB_TABLE_NOT_EXIST;}
  auto it2 = tables_.find(it->second);
  if (it2 == tables_.end()) {latch_.unlock(); return DB_FAILED;}
  table_info = it2->second;
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) {
  latch_.lock();
  for (auto it1 = table_names_.begin(); it1 != table_names_.end(); it1++) {
    auto it2 = tables_.find(it1->second);
    if (it2 == tables_.end()) {
      ASSERT(0, "Table name map inconsistent with table id map.");
      latch_.unlock();
      return DB_FAILED;
    }
    tables.push_back(it2->second);
  }
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info) {
  latch_.lock();
  // 0. check if index already exist
  auto imap = index_names_.find(table_name);
  if (imap == index_names_.end()) {latch_.unlock(); return DB_TABLE_NOT_EXIST;}
  if (imap->second.find(index_name) != imap->second.end()) {latch_.unlock(); return DB_INDEX_ALREADY_EXIST;}

  // 1. Allocate index meta page
  page_id_t index_meta_pageid;
  Page *p;
  if (!(p = buffer_pool_manager_->NewPage(index_meta_pageid))) {latch_.unlock(); return DB_FAILED;}

  // 2. Create index meta info
  index_id_t iid = next_index_id_;
  table_id_t tid;
  auto it = table_names_.find(table_name);
  if (it == table_names_.end()) {latch_.unlock(); return DB_FAILED;}
  tid = it->second;
  auto it2 = tables_.find(tid);
  if (it2 == tables_.end()) {latch_.unlock(); return DB_FAILED;}
  TableInfo *tinfo = it2->second;
  Schema *tschema = tinfo->GetSchema();

  vector<uint32_t> keymap;
  // 2.1 calculate index key map
  for (auto it2 = index_keys.begin(); it2 != index_keys.end(); it2++) {
    uint32_t idx;
    dberr_t err = tschema->GetColumnIndex(*it2, idx);
    if (err != DB_SUCCESS) {latch_.unlock(); return err;}
    keymap.push_back(idx);
  }
  IndexMetadata *meta = IndexMetadata::Create(iid, index_name, it->second, keymap, heap_);
  meta->SerializeTo(p->GetData());
  buffer_pool_manager_->UnpinPage(index_meta_pageid, true);
  IndexInfo *iinfo = IndexInfo::Create(heap_);
  iinfo->Init(meta, tinfo, buffer_pool_manager_);
  index_info = iinfo;

  // 2.2 update catalog meta info
  catalog_meta_->index_meta_pages_[iid] = index_meta_pageid;
  index_names_[table_name][index_name] = iid;
  indexes_[iid] = iinfo;
  next_index_id_ += 1;

  FlushCatalogMetaPage();//flush to buffer
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) {
  latch_.lock();
  auto it1 = index_names_.find(table_name);
  if (it1 == index_names_.end()) {latch_.unlock(); return DB_TABLE_NOT_EXIST;}
  auto it2 = it1->second.find(index_name);
  if (it2 == it1->second.end()) {latch_.unlock(); return DB_INDEX_NOT_FOUND;}
  auto it3 = indexes_.find(it2->second);
  if (it3 == indexes_.end()) {latch_.unlock(); return DB_FAILED;}  // index name map inconsistent with index id map
  index_info = it3->second;
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) {
  latch_.lock();
  indexes.clear();
  auto it1 = index_names_.find(table_name);
  if (it1 == index_names_.end()) {latch_.unlock(); return DB_TABLE_NOT_EXIST;}
  for (auto it2 = it1->second.begin(); it2 != it1->second.end(); it2++) {
    auto it3 = indexes_.find(it2->second);
    if (it3 == indexes_.end()) {latch_.unlock(); return DB_FAILED;}  // index name map inconsistent with index id map
    indexes.push_back(it3->second);
  }
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropTable(const string &table_name) {
  latch_.lock();
  // 1. check if the table exists.
  auto it1 = table_names_.find(table_name);
  if (it1 == table_names_.end()) {latch_.unlock(); return DB_TABLE_NOT_EXIST;}
  auto it2 = tables_.find(it1->second);
  if (it2 == tables_.end()) {latch_.unlock(); return DB_FAILED;} // name map inconsistent with id map
  table_id_t tid = it1->second;

  // 2. drop all indexes on this table
  auto it3 = index_names_.find(table_name);
  if (it3 != index_names_.end()) {
    // drop indexes on this table (pay attention to the problem caused by deletion while traverse!)
    vector<string> drop_index_names;  // for deletion
    for (auto it4 = it3->second.begin(); it4 != it3->second.end(); it4++) {
      drop_index_names.push_back(it4->first);
    }
    for (string drop_index_name : drop_index_names) {
      dberr_t err = DropIndex(table_name, drop_index_name);
      if (err != DB_SUCCESS) {latch_.unlock(); return err;}
    }
  }

  // 2.1 and then drop the entry on index name map
  index_names_.erase(it3);

  // 3.drop this table
  // 3.1 drop all table pages on the table heap
  TableInfo *tinfo = it2->second;
  tinfo->~TableInfo();
  ASSERT(tinfo, "Invaid table info ");
  tinfo->GetTableHeap()->FreeHeap();
  heap_->Free(tinfo);
  auto &tmap = catalog_meta_->table_meta_pages_;
  auto it5 = tmap.find(tid);
  if (it5 == tmap.end()) {latch_.unlock(); return DB_FAILED;}
  page_id_t tmeta_pid = it5->second;
  if (!buffer_pool_manager_->DeletePage(tmeta_pid)) {latch_.unlock(); return DB_FAILED;}
  // 4. update catalog meta
  tmap.erase(it5);
  table_names_.erase(it1);
  tables_.erase(it2);

  FlushCatalogMetaPage();//flush to buffer
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  latch_.lock();
  // 1. check if index exist
  auto it1 = index_names_.find(table_name);
  if (it1 == index_names_.end()) {latch_.unlock(); return DB_TABLE_NOT_EXIST;}
  auto it2 = it1->second.find(index_name);
  if (it2 == it1->second.end()) {latch_.unlock(); return DB_INDEX_NOT_FOUND;}
  index_id_t iid = it2->second;
  auto it3 = indexes_.find(iid);
  if (it3 == indexes_.end()) {latch_.unlock(); return DB_FAILED;}
  IndexInfo *info = it3->second;
  ASSERT(info, "Invalid index info");

  // 2. drop the whole index
  info->GetIndex()->Destroy();
  info->~IndexInfo();
  heap_->Free(info);
  // 3. drop index meta data
  auto &imap = catalog_meta_->index_meta_pages_;
  auto it4 = imap.find(iid);
  if (it4 == imap.end()) {latch_.unlock(); return DB_FAILED;}
  page_id_t imeta_pid = it4->second;
  //buffer_pool_manager_->UnpinPage(imeta_pid, false);
  if (!buffer_pool_manager_->DeletePage(imeta_pid)) {latch_.unlock(); return DB_FAILED;}
  // 4. update catalog meta data
  imap.erase(it4);
  it1->second.erase(it2);
  indexes_.erase(it3);

  FlushCatalogMetaPage();//flush to buffer
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::FlushCatalogMetaPage() {
  latch_.lock();
  Page *p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID, true);
  catalog_meta_->SerializeTo(p->GetData());
  if (!buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true)) {latch_.unlock(); return DB_FAILED;}
  latch_.unlock();
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  latch_.lock();
  // loading a table is nothing more than loading the table info and adding it to the maps
  Page *p_tmeta = buffer_pool_manager_->FetchPage(page_id, false);
  if (p_tmeta == nullptr) {latch_.unlock(); return DB_FAILED;}
  TableMetadata *tmeta;
  TableInfo *tinfo = TableInfo::Create(heap_);
  TableMetadata::DeserializeFrom(p_tmeta->GetData(), tmeta, tinfo->GetMemHeap());
  buffer_pool_manager_->UnpinPage(page_id, false);
  if (tmeta == nullptr) {latch_.unlock(); return DB_FAILED;}
  if (tinfo == nullptr) {latch_.unlock(); return DB_FAILED;}
  Schema *scm = Schema::DeepCopySchema(tmeta->GetSchema(), tinfo->GetMemHeap());
  TableHeap *theap =
      TableHeap::Create(buffer_pool_manager_, tmeta->GetFirstPageId(), scm, log_manager_, lock_manager_, tinfo->GetMemHeap());
  if (theap == nullptr) {latch_.unlock(); return DB_FAILED;}
  tinfo->Init(tmeta, theap);
  table_names_[tinfo->GetTableName()] = tinfo->GetTableId();
  tables_[tinfo->GetTableId()] = tinfo;

  auto it = index_names_.find(tinfo->GetTableName());
  if (it == index_names_.end()) index_names_[tinfo->GetTableName()] = {};
  latch_.unlock(); 
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  latch_.lock();
  // much the same as loadTable
  Page *p_meta = buffer_pool_manager_->FetchPage(page_id, false);
  if (p_meta == nullptr) {latch_.unlock(); return DB_FAILED;}
  IndexMetadata *meta;
  IndexInfo *info = IndexInfo::Create(heap_);
  IndexMetadata::DeserializeFrom(p_meta->GetData(), meta, info->GetMemHeap());
  buffer_pool_manager_->UnpinPage(page_id, false);
  if (meta == nullptr) {latch_.unlock(); return DB_FAILED;}
  if (info == nullptr) {latch_.unlock(); return DB_FAILED;}
  table_id_t tid = meta->GetTableId();
  if (tables_.find(tid) == tables_.end()) return DB_FAILED;
  TableInfo *tinfo = tables_[tid];
  if (!tinfo) {latch_.unlock(); return DB_FAILED;}
  info->Init(meta, tinfo, buffer_pool_manager_);
  string tname = tinfo->GetTableName();
  string iname = info->GetIndexName();
  if (index_names_.find(tname) == index_names_.end()) index_names_[tname] = {};
  index_names_[tname][iname] = index_id;
  indexes_[index_id] = info;
  latch_.unlock(); 
  return DB_SUCCESS;
}

dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  latch_.lock();
  auto it2 = tables_.find(table_id);
  if (it2 == tables_.end()) {latch_.unlock(); return DB_FAILED;}  // name map inconsistent with id map
  table_info = it2->second;
  latch_.unlock(); 
  return DB_SUCCESS;
}

dberr_t CatalogManager::SetRowNum(table_id_t tid, uint32_t row_num)
{
  latch_.lock();
  //set row number
  if(tables_.find(tid)==tables_.end())
  {
    latch_.unlock(); 
    return DB_FAILED;
  }
  tables_[tid]->SetRowNum(row_num);

  //write table meta page
  Page* table_meta_page;
  page_id_t table_meta_page_id = catalog_meta_->table_meta_pages_[tid];
  if (!(table_meta_page = buffer_pool_manager_->FetchPage(table_meta_page_id, true))) {latch_.unlock(); return DB_FAILED;}
  tables_[tid]->table_meta_->SerializeTo(table_meta_page->GetData());
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);
  if (tables_[tid]->table_meta_ == nullptr) {
    tables_[tid]->table_heap_->FreeHeap();
    buffer_pool_manager_->DeletePage(table_meta_page_id);
    latch_.unlock(); 
    return DB_FAILED;
  }
  latch_.unlock(); 
  return DB_SUCCESS;
}

dberr_t CatalogManager::LoadFromBuffer()
{
  latch_.lock();
  // simply load catalog meta and load pages & indexes ?
  Page *p;
  p = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID, false);
  if (p == nullptr) throw -1;
  ASSERT(p != nullptr, "No catalog metapage for the existed database!");

  catalog_meta_->table_meta_pages_.clear();
  catalog_meta_->index_meta_pages_.clear();

  catalog_meta_ = CatalogMeta::DeserializeFrom(p->GetData(), heap_);
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);

  //clear
  table_names_.clear();
  tables_.clear();
  index_names_.clear();
  indexes_.clear();

  // throw -1;
  ASSERT(catalog_meta_!=nullptr, "Catalog meta deserialize failed!");
  next_table_id_ = 0;
  for (auto it = catalog_meta_->table_meta_pages_.begin(); it != catalog_meta_->table_meta_pages_.end(); it++) {
    LoadTable(it->first, it->second);
    if (next_table_id_ <= it->first) next_table_id_ = it->first + 1;
  }
  next_index_id_ = 0;
  for (auto it = catalog_meta_->index_meta_pages_.begin(); it != catalog_meta_->index_meta_pages_.end(); it++) {
    LoadIndex(it->first, it->second);
    if (next_index_id_ <= it->first) next_index_id_ = it->first + 1;
  }
  latch_.unlock();  
  return DB_SUCCESS;
}