// #define HEAP_LOGGING

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
int corr_num = 1000;
int frag_num = 100000;
int cont_num = 100000;

TEST(HeapTest, DISABLED_SimpleHeapCorrectNessTest) {
  MemHeap *heap = new SimpleMemHeap;
  // test for small block
  {
    vector<SmallBlock *> addrs;
    vector<SmallBlock *> addrs2;
    map<SmallBlock *, SmallBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, SmallBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, SmallBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<MidBlock *> addrs;
    vector<MidBlock *> addrs2;
    map<MidBlock *, MidBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, MidBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, MidBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<LargeBlock *> addrs;
    vector<LargeBlock *> addrs2;
    map<LargeBlock *, LargeBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, LargeBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, LargeBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
}

TEST(HeapTest, DISABLED_ListHeapCorrectNessTest) {
  MemHeap *heap = new ListHeap;
  // test for small block
  {
    vector<SmallBlock *> addrs;
    vector<SmallBlock *> addrs2;
    map<SmallBlock *, SmallBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, SmallBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, SmallBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<MidBlock *> addrs;
    vector<MidBlock *> addrs2;
    map<MidBlock *, MidBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, MidBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, MidBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<LargeBlock *> addrs;
    vector<LargeBlock *> addrs2;
    map<LargeBlock *, LargeBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, LargeBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, LargeBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
}


TEST(HeapTest, DISABLED_ManagedHeapCorrectNessTest) {
  MemHeap *heap = new UsedHeap;
  // test for small block
  {
    vector<SmallBlock *> addrs;
    vector<SmallBlock *> addrs2;
    map<SmallBlock *, SmallBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, SmallBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, SmallBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<MidBlock *> addrs;
    vector<MidBlock *> addrs2;
    map<MidBlock *, MidBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, MidBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, MidBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
  {
    // test for small block
    vector<LargeBlock *> addrs;
    vector<LargeBlock *> addrs2;
    map<LargeBlock *, LargeBlock> kv_map;
    for (int i = 0; i < corr_num; i++) {
      addrs.push_back(ALLOC_P(heap, LargeBlock)());
      kv_map[addrs[i]] = *addrs[i];
    }
    ShuffleArray(addrs);
    for (int i = 0; i < corr_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < corr_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, LargeBlock)());
    }
    for (int i = corr_num / 2; i < corr_num; i++) {
      EXPECT_EQ(kv_map[addrs[i]], *addrs[i]);
	  // heap->Free(addrs2[i]); dont free now for more stressed debug
    }
  }
}


TEST(HeapTest, DISABLED_SimpleHeapFragmentalTest){
  MemHeap * heap = new SimpleMemHeap;
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,SmallFrag)();
    heap->Free(p);
  }
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,MidFrag)();
    heap->Free(p);
  }
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,LargeFrag)();
    heap->Free(p);
  }
}


TEST(HeapTest, DISABLED_ListHeapFragmentalTest) {
  MemHeap * heap = new ListHeap;
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,SmallFrag)();
    heap->Free(p);
  }
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,MidFrag)();
    heap->Free(p);
  }
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,LargeFrag)();
    heap->Free(p);
  }
}

TEST(HeapTest, DISABLED_ManagedHeapFragmentalTest){
  MemHeap * heap = new UsedHeap;
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,SmallFrag)();
    heap->Free(p);
  }
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,MidFrag)();
    heap->Free(p);
  }
  for(int i =0;i<frag_num;i++){
    auto p = ALLOC_P(heap,LargeFrag)();
    heap->Free(p);
  }
  delete heap;
}

TEST(HeapTest, DISABLED_SimpleHeapContinualTest){
  MemHeap *heap = new SimpleMemHeap;
  // test for small block
  {
    vector<SmallFrag *> addrs;
    vector<SmallFrag *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(ALLOC_P(heap, SmallFrag)());
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, SmallFrag)());
    }
  }
  {
    vector<MidFrag *> addrs;
    vector<MidFrag *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(ALLOC_P(heap, MidFrag)());
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, MidFrag)());
    }
  }
  {
    // test for small block
    vector<LargeFrag *> addrs;
    vector<LargeFrag *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(ALLOC_P(heap, LargeFrag)());
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, LargeFrag)());
    }
  }
}



TEST(HeapTest, DISABLED_ListHeapContinualTest){
  MemHeap *heap = new ListHeap;
  // test for small block
  {
    vector<SmallFrag *> addrs;
    vector<SmallFrag *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(ALLOC_P(heap, SmallFrag)());
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, SmallFrag)());
    }
  }
  {
    vector<MidFrag *> addrs;
    vector<MidFrag *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(ALLOC_P(heap, MidFrag)());
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, MidFrag)());
    }
  }
  {
    // test for small block
    vector<LargeFrag *> addrs;
    vector<LargeFrag *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(ALLOC_P(heap, LargeFrag)());
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(ALLOC_P(heap, LargeFrag)());
    }
  }
}


TEST(HeapTest, ManagedHeapContinualTest){
  MemHeap *heap = new UsedHeap;
  // test for small block
  {
    vector<void *> addrs;
    vector<void *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(heap->Allocate(RandomUtils::RandomInt(12, 200)));
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(heap->Allocate(RandomUtils::RandomInt(12, 200)));
    }
    for(int i = cont_num / 2;i < cont_num; i++){
      heap->Free(addrs[i]);
    }
    ShuffleArray(addrs2);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs2[i]);
    }
  }
  {
    vector<void *> addrs;
    vector<void *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(heap->Allocate(RandomUtils::RandomInt(12, 200)));
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(heap->Allocate(RandomUtils::RandomInt(12, 200)));
    }
    for(int i = cont_num / 2;i < cont_num; i++){
      heap->Free(addrs[i]);
    }
    ShuffleArray(addrs2);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs2[i]);
    }
  }
  {
    // test for small block
    vector<void *> addrs;
    vector<void *> addrs2;
    for (int i = 0; i < cont_num; i++) {
      addrs.push_back(heap->Allocate(RandomUtils::RandomInt(12, 200)));
    }
    ShuffleArray(addrs);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs[i]);
    }
    for (int i = 0; i < cont_num / 2; i++) {
      addrs2.push_back(heap->Allocate(RandomUtils::RandomInt(12, 200)));
    }
    for(int i = cont_num / 2;i < cont_num; i++){
      heap->Free(addrs[i]);
    }
    ShuffleArray(addrs2);
    for (int i = 0; i < cont_num / 2; i++) {
      heap->Free(addrs2[i]);
    }
  }
  delete heap;
}
