#include <cstring>
#include<iostream>
#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"
#include "record/types.h"
MemHeap *heap = new SimpleMemHeap();
char *chars[] = {
        const_cast<char *>(""),
        const_cast<char *>("hello"),
        const_cast<char *>("world!"),
        const_cast<char *>("\0")
};

Field int_fields[] = {
        Field(TypeId::kTypeInt, 188,heap),
        Field(TypeId::kTypeInt, -65537,heap),
        Field(TypeId::kTypeInt, 33389,heap),
        Field(TypeId::kTypeInt, 0,heap),
        Field(TypeId::kTypeInt, 999,heap),
};
Field float_fields[] = {
        Field(TypeId::kTypeFloat, -2.33f,heap),
        Field(TypeId::kTypeFloat, 19.99f,heap),
        Field(TypeId::kTypeFloat, 999999.9995f,heap),
        Field(TypeId::kTypeFloat, -77.7f,heap),
};
Field char_fields[] = {
        Field(TypeId::kTypeChar, chars[0],heap, strlen(chars[0]), false),
        Field(TypeId::kTypeChar, chars[1],heap, strlen(chars[1]), false),
        Field(TypeId::kTypeChar, chars[2],heap, strlen(chars[2]), false),
        Field(TypeId::kTypeChar, chars[3],heap, 1, false)
};
Field null_fields[] = {
        Field(TypeId::kTypeInt,heap), Field(TypeId::kTypeFloat,heap), Field(TypeId::kTypeChar,heap)
};

TEST(TupleTest, FieldSerializeDeserializeTest) {
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  MemHeap *heap = new UsedHeap();
  for (int i = 0; i < 4; i++) {
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 3; i++) {
    p += float_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 4; i++) {
    p += char_fields[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = ALLOC_P(heap,Field)(TypeId::kTypeInt,heap);
  for (int i = 0; i < 4; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, df,heap,TypeId::kTypeInt, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, df,heap,TypeId::kTypeFloat, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
  }
  for (int i = 0; i < 3; i++) {
    ofs += Field::DeserializeFrom(buffer + ofs, df,heap,TypeId::kTypeChar, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
  }
}

TEST(TupleTest, RowTest) {
  UsedHeap heap;
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {
          ALLOC_COLUMN(heap)("id", TypeId::kTypeInt, 0, false, false),
          ALLOC_COLUMN(heap)("name", TypeId::kTypeChar, 64, 1, true, false),
          ALLOC_COLUMN(heap)("account", TypeId::kTypeFloat, 2, true, false)
  };
  std::vector<Field> fields = {
          Field(TypeId::kTypeInt, 188,&heap),
          Field(TypeId::kTypeChar, const_cast<char *>("minisql"),&heap, strlen("minisql"), false),
          Field(TypeId::kTypeFloat, 19.99f,&heap)
  };

  auto schema = std::make_shared<Schema>(columns);

  //mytest-----------------------------------------------------s
  //mytest for column

  MemHeap *mem_heap1 = new UsedHeap();
  char buffer1[PAGE_SIZE];
  Column* col = columns[1];
  col->SerializeTo(buffer1);
  Column* dcol=NULL;
  Column::DeserializeFrom(buffer1, dcol, mem_heap1);
  ASSERT(dcol->GetName()==col->GetName(), "Column deserialize error!");//test condition

  //mytest for schema (also including column naturally)
  MemHeap *mem_heap2 = new UsedHeap();
  char buffer2[PAGE_SIZE];
  schema->SerializeTo(buffer2);
  Schema *dsch = NULL;
  Schema::DeserializeFrom(buffer2, dsch, mem_heap2);
  ASSERT(dsch->GetColumnCount()==schema->GetColumnCount(), "Schema deserialize error");//test condition
  
  //-------------------end my test--------------------------------

  Row row(fields, new SimpleMemHeap);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  Row row2(row.GetRowId(),new SimpleMemHeap);
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  Field * row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2.GetFieldCount());
  for (size_t i = 0; i < row2.GetFieldCount(); i++) {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i].CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}