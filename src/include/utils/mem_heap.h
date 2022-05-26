#ifndef MINISQL_MEM_HEAP_H
#define MINISQL_MEM_HEAP_H

#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include <vector>
#include "common/macros.h"

class MemHeap {
public:
  virtual ~MemHeap() = default;

  /**
   * @brief Allocate a contiguous block of memory of the given size
   * @param size The size (in bytes) of memory to allocate
   * @return A non-null pointer if allocation is successful. A null pointer if
   * allocation fails.
   */
  virtual void *Allocate(size_t size) = 0;

  /**
   * @brief Returns the provided chunk of memory back into the pool
   */
  virtual void Free(void *ptr) = 0;

};

class SimpleMemHeap : public MemHeap {
public:
  ~SimpleMemHeap() {
    for (auto it: allocated_) {
      free(it);
    }
  }

  void *Allocate(size_t size) {
    void *buf = malloc(size);
    ASSERT(buf != nullptr, "Out of memory exception");
    allocated_.push_back(buf);
    return buf;
  }

  void Free(void *ptr) {
    if (ptr == nullptr) {
      return;
    }
    std::vector<void *>::iterator iter;
    for(iter = allocated_.begin();iter!=allocated_.end();iter++)//linear scan to find
    {
      if((*iter)==ptr)
        break;
    }
    if (iter != allocated_.end()) {
      allocated_.erase(iter);
    }
  }

private:
  std::vector<void *> allocated_;
};

#endif //MINISQL_MEM_HEAP_H
