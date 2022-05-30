#include "buffer/lru_replacer_old.h"

LRUReplacerOld::LRUReplacerOld(size_t num_frames_) {
  this->num_frames_ = num_frames_;
  // set the max size of lru_list_ to num_frames_
}
LRUReplacerOld::~LRUReplacerOld() = default;

bool LRUReplacerOld::Victim(frame_id_t *frame_id) {
  frame_id_t max_frame_id = -1;
  int max_time = -1;
  for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
    if (it->second > max_time) {
      max_time = it->second;
      max_frame_id = it->first;
    }
  }
  if (max_frame_id == -1)  // empty lru_list_, can not replace
  {
    frame_id = nullptr;
    return false;
  }
  *frame_id = max_frame_id;
  lru_list_.erase(max_frame_id);  // delete the victimed frame
  return true;
}
void LRUReplacerOld::Pin(frame_id_t frame_id) {
  if (lru_list_.find(frame_id) != lru_list_.end())  // exist
  {
    lru_list_.erase(frame_id);
    for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
      it->second++;  // increase old factor of frames in list
    }
  }
}
void LRUReplacerOld::Unpin(frame_id_t frame_id) {
  if (lru_list_.find(frame_id) == lru_list_.end())  // not exist
  {
    lru_list_.insert(make_pair(frame_id, 0));
    for (auto it = lru_list_.begin(); it != lru_list_.end(); it++) {
      it->second++;  // increase old factor of frames in list
    }
  }
}
size_t LRUReplacerOld::Size() { return lru_list_.size(); }