#define HEAP_LOGGING

#include <vector>
#include <unordered_map>
#include <iostream>
#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "storage/table_heap.h"
#include "utils/utils.h"
#include "utils/mem_heap.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  ManagedHeap heap;
  const int row_nums = 1;
  // create schema
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableInfo * tinfo;
  engine.catalog_mgr_->CreateTable("tabl1", schema.get(), nullptr, tinfo);
  
  tinfo=nullptr;
  engine.catalog_mgr_->GetTable("tabl1", tinfo);

  cout << "Create table finish" << endl;
  TableHeap *table_heap = tinfo->GetTableHeap();
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields = new Fields{
            Field(TypeId::kTypeInt, i,&heap),
            Field(TypeId::kTypeChar, const_cast<char *>(characters),&heap, len, true),
            Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f),&heap)
    };
    Row row(*fields,new SimpleMemHeap);
    table_heap->InsertTuple(row, nullptr);
    row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first),new SimpleMemHeap);
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFieldCount());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  cout << "official test finish" << endl;
  engine.bpm_->get_hit_rate();//show hit rate

  //-----------my rough test--------------------
  bool do_my_test = true;//set true if want to do my test

  if(!do_my_test)
    return;

  //my test for iterator----------------------------------
  int i = 0;
  const Field test_field(kTypeFloat, (float)3.77,&heap);
  cout << "0" <<endl;
  for (auto it = table_heap->Begin(); it != table_heap->End(); ++it) {
    Row row = *it;
    std::cout << "i = " << i<<"  page id = "<<row.GetRowId().GetPageId()
              <<"  slot num = "<<row.GetRowId().GetSlotNum()<<" float equal 3.77= "<<row.GetField(2)->CompareEquals(test_field)<<std::endl;
    i++;
  }
  cout << "1" <<endl;
  //end my test for iterator-----------------------------

  //my test for update, delete---------
  i = 0;
  RowId del_rid(3, 0);//delete the first row in the seconde data page, nake sure row number bigger than 90
  table_heap->MarkDelete(del_rid, nullptr);
  table_heap->ApplyDelete(del_rid, nullptr);

  RowId upd_rid(3, 1);//update the tuple in the page
  char test_str[2];
  test_str[0]='t';
  test_str[1]='\0';
  
  Fields *fields = new Fields{
        Field(TypeId::kTypeInt, 1,&heap),
        Field(TypeId::kTypeChar, test_str, &heap,strlen(test_str), true),
        Field(TypeId::kTypeFloat, (float)3.77,&heap)//the same as the tested field
  };
  Row new_row(*fields, new SimpleMemHeap);
  table_heap->UpdateTuple(new_row, upd_rid, nullptr);//test update
  //table_heap->InsertTuple(new_row, nullptr);
  std::cout << "------------------------after deletion and update-----------------------" << endl;
  for (auto it = table_heap->Begin(); it != table_heap->End(); it++) {
    Row row = *it;
    std::cout << "(after) i = " << i<<"  page id = "<<row.GetRowId().GetPageId()
              <<"  slot num = "<<row.GetRowId().GetSlotNum()<<" float equal 3.77 = "<<row.GetField(2)->CompareEquals(test_field)<<std::endl;
    i++;
  }
  table_heap->FreeHeap();
  // end my test for update, delete---------------
  //------------------end my test-----------------
}

