#include <cstring>
#include <iostream>
#include <vector>
#include "gtest/gtest.h"
#include "utils/mem_heap.h"
#include "utils/utils.h"

using namespace std;

template <int size>
struct Block {
  char data_[size];
  Block() {
    for (int i = 0; i < size; i++) data_[i] = RandomUtils::RandomInt(0, 65536);
  }
  bool operator==(const Block &another) const {
    for (int i = 0; i < size; i++)
      if (data_[i] != another.data_[i]) return false;
    return true;
  }
};
template <int size>
struct Fragment{
  char data_[size];
  Fragment(){}
};

using SmallBlock = Block<8>;
using MidBlock = Block<64>;
using LargeBlock = Block<512>;

using SmallFrag = Fragment<8>;
using MidFrag = Fragment<64>;
using LargeFrag = Fragment<512>;
int test_num = 1000;
int repeat_num = 100000;

TEST(HeapTest, SimpleHeapCorrectNessTest) {
  MemHeap *heap = new SimpleMemHeap;
  // test for small block
  {
    vector<SmallBlock *> addrs;
    vector<SmallBlock *> addrs2;
    map<SmallBlock *, SmallBlock> kv_map;
    for (int i = 0; i < test_num; i++) {
      addrs.push_back(ALLOC_P(heap, SmallBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < test_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < test_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, SmallBlock)());
    }
    for (int i = test_num / 2; i < test_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<MidBlock *> addrs;
    vector<MidBlock *> addrs2;
    map<MidBlock *, MidBlock> kv_map;
    for (int i = 0; i < test_num; i++) {
      addrs.push_back(ALLOC_P(heap, MidBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < test_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < test_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, MidBlock)());
    }
    for (int i = test_num / 2; i < test_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<LargeBlock *> addrs;
    vector<LargeBlock *> addrs2;
    map<LargeBlock *, LargeBlock> kv_map;
    for (int i = 0; i < test_num; i++) {
      addrs.push_back(ALLOC_P(heap, LargeBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < test_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < test_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, LargeBlock)());
    }
    for (int i = test_num / 2; i < test_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
}

TEST(HeapTest, ListHeapTest) {

}

TEST(HeapTest, ManagedHeapCorrectNessTest) {
  MemHeap *heap = new ManagedHeap;
  // test for small block
  {
    vector<SmallBlock *> addrs;
    vector<SmallBlock *> addrs2;
    map<SmallBlock *, SmallBlock> kv_map;
    for (int i = 0; i < test_num; i++) {
      addrs.push_back(ALLOC_P(heap, SmallBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < test_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < test_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, SmallBlock)());
    }
    for (int i = test_num / 2; i < test_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<MidBlock *> addrs;
    vector<MidBlock *> addrs2;
    map<MidBlock *, MidBlock> kv_map;
    for (int i = 0; i < test_num; i++) {
      addrs.push_back(ALLOC_P(heap, MidBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < test_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < test_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, MidBlock)());
    }
    for (int i = test_num / 2; i < test_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<LargeBlock *> addrs;
    vector<LargeBlock *> addrs2;
    map<LargeBlock *, LargeBlock> kv_map;
    for (int i = 0; i < test_num; i++) {
      addrs.push_back(ALLOC_P(heap, LargeBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < test_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < test_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, LargeBlock)());
    }
    for (int i = test_num / 2; i < test_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
}


TEST(HeapTest, SimpleHeapFragmentalTest){
  MemHeap * heap = new SimpleMemHeap;
  for(int i =0;i<repeat_num;i++){
    auto p = ALLOC_P(heap,SmallBlock)();
    heap->Free(p);
  }
  for(int i =0;i<repeat_num;i++){
    auto p = ALLOC_P(heap,MidBlock)();
    heap->Free(p);
  }
  for(int i =0;i<repeat_num;i++){
    auto p = ALLOC_P(heap,LargeBlock)();
    heap->Free(p);
  }
}

TEST(HeapTest, ManagedHeapFragmentalTest){
  MemHeap * heap = new ManagedHeap;
  for(int i =0;i<repeat_num;i++){
    auto p = ALLOC_P(heap,SmallFrag)();
    heap->Free(p);
  }
  for(int i =0;i<repeat_num;i++){
    auto p = ALLOC_P(heap,MidFrag)();
    heap->Free(p);
  }
  for(int i =0;i<repeat_num;i++){
    auto p = ALLOC_P(heap,LargeFrag)();
    heap->Free(p);
  }
}