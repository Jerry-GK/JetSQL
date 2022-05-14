#include "buffer/lru_replacer.h"
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <ostream>
LRUReplacer::LRUReplacer(size_t num_pages) {
  this->num_frames_ = num_pages;
  this->lru_list_ = new int[num_pages];
  this->present_ = new bool[num_pages]{false};
  this->num_present_ = 0;
}

LRUReplacer::~LRUReplacer(){
  delete[] lru_list_;
  delete[] present_;
}

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  frame_id_t max_frame_id = -1;
  if(!num_present_)return false;
  int max_time = min_;
  int *lru_list_last = lru_list_ + num_frames_;
  int *it = lru_list_;
  bool *b = present_;
  for (;it < lru_list_last;it++,b++) {
    int k = *it;
    if(k>=max_time && *b)
    {
      max_time = k;
      max_frame_id = it - lru_list_;
    }
  }
  *frame_id = max_frame_id;
  present_[max_frame_id] = false;
  num_present_--;
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  if(present_[frame_id]) {
    present_[frame_id] = false;
    num_present_ --;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(present_[frame_id])return;
  if(min_ == INT32_MIN){ // in case of overflow
    // find the largest
    int max = min_;
    int *i = lru_list_;
    bool *b = present_;
    for(; i < lru_list_ + num_frames_;i++,b++) if(*b && *i > max) max = *i;
    int diff = INT32_MAX - max;
    for(i = lru_list_,b = present_; i < lru_list_ + num_frames_;i++,b++) if(*b)*i += diff;
    min_ = INT32_MIN + diff - 1;
    lru_list_[frame_id] = INT32_MIN + diff - 1;
  }else{  // do not overflow ;)
    lru_list_[frame_id] = min_ - 1;
    min_ -= 1;
  }
  this->num_present_++;
  present_[frame_id] = true;
}

size_t LRUReplacer::Size() {
  return num_present_;
}