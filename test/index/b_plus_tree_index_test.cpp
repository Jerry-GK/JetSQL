#include <string>

// #define HEAP_LOGGING

#include "common/dberr.h"
#include "common/instance.h"
#include "gtest/gtest.h"
#include "index/b_plus_tree_index.h"
#include "index/generic_key.h"

static const std::string db_name = "bp_tree_index_test.db";

TEST(BPlusTreeTests, BPlusTreeIndexGenericKeyTest) {
  using INDEX_KEY_TYPE = GenericKey<128>;
  using INDEX_COMPARATOR_TYPE = GenericComparator<128>;
  DBStorageEngine engine(db_name);
  UsedHeap heap;
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  std::vector<uint32_t> index_key_map{0, 1};
  TableSchema table_schema(columns);
  TableInfo * tinfo;
  IndexInfo * iinfo;
  engine.catalog_mgr_->CreateTable("test_table", &table_schema, nullptr, tinfo);
  engine.catalog_mgr_->CreateIndex("test_table", "index1", {"id","name"}, nullptr,iinfo);
  // auto *key_schema = Schema::ShallowCopySchema(&table_schema, index_key_map, &heap);
  std::vector<Field> fields{
          Field(TypeId::kTypeInt, 27,&heap),
          Field(TypeId::kTypeChar, const_cast<char *>("minisql"),&heap, 7, true)
  };

  Row key(fields, new SimpleMemHeap);
  INDEX_KEY_TYPE k1;
  k1.SerializeFromKey(key, iinfo->GetIndexKeySchema());
  INDEX_KEY_TYPE k2;
  Row copy_key(fields,new SimpleMemHeap);
  k2.SerializeFromKey(copy_key, iinfo->GetIndexKeySchema());
  INDEX_COMPARATOR_TYPE comparator(iinfo->GetIndexKeySchema());
  ASSERT_EQ(0, comparator(k1, k2));
}

TEST(BPlusTreeTests, BPlusTreeIndexSimpleTest) {
  DBStorageEngine engine(db_name);
  UsedHeap heap;
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  std::vector<uint32_t> index_key_map{0, 1};
  TableSchema table_schema(columns);
  // auto *index_schema = Schema::ShallowCopySchema(&table_schema, index_key_map, &heap);
  TableInfo *tinfo;
  IndexInfo *iinfo;
  engine.catalog_mgr_->CreateTable("testtable", &table_schema, nullptr, tinfo);
  engine.catalog_mgr_->CreateIndex("testtable", "index_schema",{"id","name"},nullptr,iinfo);

  auto index = iinfo->GetIndex(); //No need to specify the type of index
  for (int i = 0; i < 10; i++) {
    std::vector<Field> fields{
            Field(TypeId::kTypeInt, i,&heap),
            Field(TypeId::kTypeChar, const_cast<char *>("minisql"),&heap, 7, true)
    };
    Row row(fields,&heap);
    RowId rid(1000, i);
    ASSERT_EQ(DB_SUCCESS, index->InsertEntry(row, rid, nullptr));
  }
  // Test Scan
  std::vector<RowId> ret;
  for (int i = 0; i < 10; i++) {
    std::vector<Field> fields{
            Field(TypeId::kTypeInt, i,&heap),
            Field(TypeId::kTypeChar, const_cast<char *>("minisql"),&heap, 7, true)
    };
    Row row(fields ,&heap);
    RowId rid(1000, i);
    dberr_t err = index->ScanKey(row, ret, nullptr);
    auto bindex = dynamic_cast<BPlusTreeIndex * >(index);
    auto iter = bindex->GetBeginIterator(row);
    ASSERT_EQ(DB_SUCCESS, err);
    ASSERT_EQ(rid.Get(), ret[i].Get());
    ASSERT_EQ(rid, iter->value);
  }
  
  // Iterator Scan
//   auto iter = index->GetBeginIterator();
//   uint32_t i = 0;
//   for (; iter != index->GetEndIterator(); ++iter) {
//     ASSERT_EQ(1000, (*iter).second.GetPageId());
//     ASSERT_EQ(i, (*iter).second.GetSlotNum());
//     i++;
//   }
}