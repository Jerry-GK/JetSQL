#include "page/index_roots_page.h"
#include "common/config.h"

bool IndexRootsPage::Insert(const index_id_t index_id, const page_id_t root_id) {
  auto index = FindIndex(index_id);
  // check for duplicate index id
  if (index != -1) {
    return false;
  }
  int pos = FindPosition(index_id);
  count_ ++;
  for(auto i = count_ - 1 ;i > pos;i --)roots_[i] = roots_[i - 1];
  roots_[pos].first = index_id;
  roots_[pos].second = root_id;
  return true;
}

bool IndexRootsPage::Delete(const index_id_t index_id) {
  auto index = FindIndex(index_id);
  if (index == -1) return false;
  for (auto i = index; i < count_ - 1; i++) roots_[i] = roots_[i + 1];
  count_--;
  return true;
}


bool IndexRootsPage::Update(const index_id_t index_id, const page_id_t root_id) {
  auto index = FindIndex(index_id);
  if (index == -1) {
    return false;
  }
  roots_[index].second = root_id;
  return true;
}

bool IndexRootsPage::GetRootId(const index_id_t index_id, page_id_t *root_id) {
  auto index = FindIndex(index_id);
  if (index == -1) {
    return false;
  }
  *root_id = roots_[index].second;
  return true;
}


int IndexRootsPage::FindPosition(const index_id_t index_id){
  int l = 0,r = count_;
  while(l < r){
    int mid = (l + r) / 2;
    index_id_t c = roots_[mid].first;
    if(c >= index_id)r = mid;
    else l = mid + 1;
  }
  return r;
}

int IndexRootsPage::FindIndex(const index_id_t index_id) {
  int l = 0,r = count_ - 1;
  if(count_ == 0)return -1;
  while(l < r){
    int mid = (l + r) / 2;
    index_id_t c = roots_[mid].first;
    if(c > index_id)r = mid - 1;
    else if(c < index_id)l = mid + 1;
    else return mid;
  }
  if(roots_[l].first == index_id)return l;
  return -1;
}
