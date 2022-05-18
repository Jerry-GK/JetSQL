#include "page/bitmap_page.h"
#include <cstdint>
#include <iostream>
using namespace std;

template<size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {

  uint32_t offset = next_free_page_;
  if(page_allocated_ >= MAX_CHARS * 8)return false;
  if(offset >= MAX_CHARS * 8) return false;
  uint8_t &current_byte = bytes[offset >> 3];
  if(current_byte & (1 << ( offset & 7)))return false;
  current_byte |= (1 << (offset & 7));
  page_offset = offset;
  //update next free page
  page_allocated_ += 1;
  for(uint32_t i = offset;i < MAX_CHARS * 8;i++){
    if((bytes[i >> 3] & (1 << (i & 7))) == 0){
      next_free_page_ = i;
      break;
    }
  }
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  uint8_t &current_byte = bytes[page_offset >> 3];
  if(page_allocated_ == 0)return false;
  if((current_byte & (1 << ( page_offset & 7))) == 0)return false;
  else current_byte &= ~(1 << (page_offset & 7));
  page_allocated_ -= 1;
  if(next_free_page_ > page_offset)next_free_page_ = page_offset;
  return true;
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  if(page_allocated_ == MAX_CHARS * 8)return false;
  return IsPageFreeLow(page_offset >> 3, page_offset & 7);
}

template<size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  if(bytes[byte_index] & ( 1 << bit_index))return false;
  return true;
}

template
class BitmapPage<64>;

template
class BitmapPage<128>;

template
class BitmapPage<256>;

template
class BitmapPage<512>;

template
class BitmapPage<1024>;

template
class BitmapPage<2048>;

template
class BitmapPage<4096>;