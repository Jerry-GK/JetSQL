#include <vector>
#include <unordered_map>
#include <iostream>
#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "storage/table_heap.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  DBStorageEngine engine(db_file_name);
  SimpleMemHeap heap;
  const int row_nums = 200;
  // create schema
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr, &heap);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields = new Fields{
            Field(TypeId::kTypeInt, i),
            Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
            Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))
    };
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);//seg fault?
    row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }

  //-----------my test--------------------
  bool do_my_test=false;//set true if want to do my test

  if(!do_my_test)
    return;

  //my test for iterator----------------------------------
  int i = 0;
  for (auto it = table_heap->Begin(); it != table_heap->End(); it++) {
    Row row = *it;
    std::cout << "i = " << i<<"  page id = "<<row.GetRowId().GetPageId()
              <<"  slot num = "<<row.GetRowId().GetSlotNum()<<std::endl;
    i++;
  }
  //end my test for iterator-----------------------------

  //my test for update, delete---------
  i = 0;
  RowId del_rid(2, 0);
  table_heap->MarkDelete(del_rid, nullptr);
  table_heap->ApplyDelete(del_rid, nullptr);

  RowId upd_rid(2, 1);
  char test_str[2];
  test_str[0]='t';
  test_str[1]='\0';
  Fields *fields = new Fields{
        Field(TypeId::kTypeInt, 1),
        Field(TypeId::kTypeChar, test_str, strlen(test_str), true),
        Field(TypeId::kTypeFloat, (float)3.77)
  };
  Row new_row(*fields);
  table_heap->UpdateTuple(new_row, upd_rid, nullptr);//this will cause error!
  //table_heap->InsertTuple(new_row, nullptr);
  std::cout << "------------------------after deletion and update-----------------------" << endl;
  for (auto it = table_heap->Begin(); it != table_heap->End(); it++) {
    Row row = *it;
    std::cout << "i = " << i<<"  page id = "<<row.GetRowId().GetPageId()
              <<"  slot num = "<<row.GetRowId().GetSlotNum()<<std::endl;
    i++;
  }
  // end my test for update, delete---------------
  //------------------end my test-----------------
}

