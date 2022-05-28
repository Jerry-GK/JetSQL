#ifndef MINISQL_MEM_HEAP_H
#define MINISQL_MEM_HEAP_H

#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include <vector>
#include "common/macros.h"

class MemHeap;
class SimpleMemHeap;
class VecMemHeap;
class ListHeap;

using UsedHeap = ListHeap;//chosen heap (the best)

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
    allocated_.insert(buf);
    return buf;
  }

  void Free(void *ptr) {
    if (ptr == nullptr) {
      return;
    }
    auto iter = allocated_.find(ptr);
    if (iter != allocated_.end()) {
      free(*iter);
      allocated_.erase(iter);
    }
  }

private:
  std::unordered_set<void *> allocated_;
};

class VecMemHeap : public MemHeap {
public:
  ~VecMemHeap() {
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
      free(*iter);
      allocated_.erase(iter);
    }
  }

private:
  std::vector<void *> allocated_;
};


class ListHeap : public MemHeap {
struct Node_
{
  void* val;
  struct Node_ *next;
};
typedef struct Node_ Node;

public:
 ListHeap() { 
   head_allocated_ = new Node; 
   head_allocated_->val=nullptr;
   head_allocated_->next = nullptr;
 }
 ~ListHeap() {
   Node *p = head_allocated_;
   while (p != nullptr) {
     Node *next = p->next;
     if(p->val!=nullptr)
      free(p->val);
     delete p;
     p = next;
   }
  }

  void *Allocate(size_t size) {
    void *buf = malloc(size);
    ASSERT(buf != nullptr, "Out of memory exception");
    Node* new_node = new Node;
    new_node->val = buf;
    new_node->next = head_allocated_->next;
    head_allocated_->next = new_node;
    return buf;
  }

  void Free(void *ptr) {
    if (ptr == nullptr) {
      return;
    }
    Node *p = head_allocated_;
    while (p->next != nullptr) {
      if(p->next->val == ptr)
      {
        free(p->next->val);
        Node *temp = p->next;
        p->next = p->next->next;
        delete temp;
      }
      else
        p=p->next;
    }
  }

private:
  Node* head_allocated_;
};


#endif //MINISQL_MEM_HEAP_H
