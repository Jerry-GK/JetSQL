#ifndef MINISQL_MEM_HEAP_H
#define MINISQL_MEM_HEAP_H

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <unordered_set>
#include <vector>

// #define HEAP_LOGGING

#ifdef HEAP_LOGGING
#define BOOST_STACKTRACE_USE_ADDR2LINE
//#include <boost/stacktrace.hpp>
#endif

#include "common/macros.h"

class MemHeap;
class SimpleMemHeap;
class VecMemHeap;
class ListHeap;
template <int size = 512>
class ManagedHeap;


using UsedHeap = ManagedHeap<>;  // chosen heap (the best)

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

  void Stat() { std::cout << "Allocated = " << allocated_count_ << ";Freed = " << freed_count_ << std::endl; }
  size_t allocated_count_ = 0;
  size_t freed_count_ = 0;
};

class SimpleMemHeap : public MemHeap {
 public:
  ~SimpleMemHeap() {
    for (auto it : allocated_) {
      free(it);
    }
  }

  void *Allocate(size_t size) {
    void *buf = malloc(size);
    ASSERT(buf != nullptr, "Out of memory exception");
    allocated_.insert(buf);
    allocated_count_ += 1;
    return buf;
  }

  void Free(void *ptr) {
    if (ptr == nullptr) {
      return;
    }
    auto iter = allocated_.find(ptr);
    if (iter != allocated_.end()) {
      freed_count_ += 1;
      free(*iter);
      allocated_.erase(iter);
    }
  }

 private:
  std::unordered_set<void *> allocated_;
};

static const uint32_t BLOCK_MAGIC = 0x08AFDBC4;

struct BlockHeader {
  uint32_t magic_;
  uint32_t block_size_;
  uint32_t prev_free_;
  uint32_t next_free_;

  inline bool IsBlockFree() { return !(block_size_ & (1 << 31)); }
  inline uint32_t GetBlockSize() { return block_size_ & (~(1 << 31)); }
  inline void SetBlockSize(size_t size) {
    block_size_ &= 1 << 31;
    block_size_ |= (size & (~(1 << 31)));
  }
  inline void SetBlockFree(bool f) {
    if (f)
      block_size_ &= ~(1 << 31);
    else
      block_size_ |= 1 << 31;
  }
};

struct ChunkHeader {
  int chunk_size_;
  int chunk_pointer_;
  char *chunk_addr_;
};

template <int minsize>
class ManagedHeap : public MemHeap {
 public:
  ~ManagedHeap() {
    for (size_t i = 0; i < num_chunks_; i++) {
      free(chunks_[i].chunk_addr_);
    }
    free(chunks_);
  }

  ManagedHeap() {
    num_chunks_ = 0;
    max_chunks_ = 0;
    allocated_count_ = 0;
    freed_count_ = 0;
    chunks_ = nullptr;
  }

  void *Allocate(size_t size) {
    allocated_count_ += 1;
    size_t chunk_idx = GetFirstChunkFor(size);
    char *ptr = nullptr;
    while (ptr == nullptr) {
      while (num_chunks_ <= chunk_idx) ExpandChunk();
      ptr = AllocateInChunk(chunk_idx, size);
      chunk_idx++;
    }
    return ptr;
  }

  // in address sequence :
  //    a - q - b
  //
  // in free chain sequence :
  //    p - q - r
  //
  // 'f' stands for "footer" , 'h' stands for "header"
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wsequence-point"
#define B(x) reinterpret_cast<BlockHeader *>(reinterpret_cast<void *>(x))
#define C(x) reinterpret_cast<char *>(reinterpret_cast<void *>(x))
#define OK(x) (C(x) < top && C(x) >= base)
#define V(x) reinterpret_cast<void *>(x)
#define Z sizeof(BlockHeader)

  void Free(void *ptr) {
    freed_count_ += 1;
    char *p = reinterpret_cast<char *>(ptr);
    char *qh = p - Z;
    ASSERT(B(qh)->magic_ == BLOCK_MAGIC, "Address does not belong to this heap!");
    ASSERT(!B(qh)->IsBlockFree(),"Block is already free!Double free!");

    // find the chunk for this ptr
    for (size_t i = 0; i < num_chunks_; i++) {
      auto &chk = chunks_[i];
      size_t chunksz = chk.chunk_size_;
      char *top = chk.chunk_addr_ + chk.chunk_size_;
      char *base = chk.chunk_addr_;
      if (p >= chk.chunk_addr_ + Z && p < chk.chunk_addr_ + chunksz - Z) {
        char *qf = qh + B(qh)->GetBlockSize() + Z;
        char *af = qh - Z;
        char *bh = qf + Z;
        char *ah = nullptr, *bf = nullptr;
#ifdef HEAP_LOGGING
        std::cout << "Free at chunk " << i << " offset " << (C(ptr) - base - Z) << " addr " << V(ptr) << std::endl;
        //std::cout << boost::stacktrace::stacktrace() << std::endl;
#endif

        // the new free block
        char *nh = qh;
        char *nf = qf;
        // add current block into free list
        B(qh)->next_free_ = B(qf)->next_free_ = chk.chunk_pointer_;
        B(qh)->prev_free_ = B(qf)->prev_free_ = chunksz;
        char *th = base + chk.chunk_pointer_;  // the old free head
        B(nh)->SetBlockFree(true);
        B(nf)->SetBlockFree(true);
        if (OK(th)) {
          char *tf = th + B(th)->GetBlockSize() + Z;
          B(th)->prev_free_ = B(tf)->prev_free_ = nh - base;
        }
        // merge with next free block if possible
        if (OK(bh) && B(bh)->IsBlockFree()) {
          bf = bh + B(bh)->GetBlockSize() + Z;
          char *pbh = base + B(bh)->prev_free_;
          char *rbh = base + B(bh)->next_free_;
          if (OK(pbh)) {
            char *pbf = pbh + B(pbh)->GetBlockSize() + Z;
            B(pbh)->next_free_ = B(pbf)->next_free_ = rbh - base;
          }
          if (OK(rbh)) {
            char *rbf = rbh + B(rbh)->GetBlockSize() + Z;
            B(rbh)->prev_free_ = B(rbf)->prev_free_ = pbh - base;
          }
          size_t newsz = B(nh)->GetBlockSize() + B(bh)->GetBlockSize() + 2 * Z;
          // if(bh - base == chk.chunk_pointer_ && OK(th))B(qh)->next_free_ = B(qf)->next_free_ = B(th)->next_free_;
          B(nh)->SetBlockSize(newsz);
          B(bf)->SetBlockSize(newsz);
          B(bf)->SetBlockFree(true);
          B(bf)->next_free_ = B(nh)->next_free_;
          B(bf)->prev_free_ = B(nh)->prev_free_;
          nf = bf;
        }
        th = B(nh)->next_free_ + base;
        // merge with previous block if possible
        if (OK(af) && B(af)->IsBlockFree()) {
          ah = af - B(af)->GetBlockSize() - Z;
          char *pah = base + B(af)->prev_free_;
          char *rah = base + B(af)->next_free_;
          if (OK(pah)) {
            char *paf = pah + B(pah)->GetBlockSize() + Z;
            B(pah)->next_free_ = B(paf)->next_free_ = rah - base;
          }
          if (OK(rah)) {
            char *raf = rah + B(rah)->GetBlockSize() + Z;
            B(rah)->prev_free_ = B(raf)->prev_free_ = pah - base;
          }
          size_t newsz = B(nh)->GetBlockSize() + B(ah)->GetBlockSize() + 2 * Z;
          // if(ah - base == chk.chunk_pointer_ && OK(th))B(ah)->next_free_ = B(af)->next_free_  = B(th)->next_free_;
          B(nf)->SetBlockSize(newsz);
          B(ah)->SetBlockSize(newsz);
          B(ah)->SetBlockFree(true);
          B(ah)->next_free_ = B(nf)->next_free_;
          B(ah)->prev_free_ = B(nf)->prev_free_;
          nh = ah;
        }
        th = B(nh)->next_free_ + base;
        chk.chunk_pointer_ = nh - base;
        if (OK(th)) {
          char *tf = th + B(th)->GetBlockSize() + Z;
          B(th)->prev_free_ = B(tf)->prev_free_ = nh - base;
        }
#ifdef HEAP_LOGGING
        //ShowHeap();
#endif
        return;
      }
    }
    ASSERT(0, "Address does not belong to managed heap!");
  }

 private:
  static constexpr size_t GetSmallestChunkSize() { return minsize; }
  inline size_t GetFirstChunkFor(size_t size) {
    size_t k = GetSmallestChunkSize(), i = 0;
    while (size > k) {
      k = k << 1;
      i = i + 1;
    }
    return i;
  }

  void ShowHeap() {
    for (size_t i = 0; i < num_chunks_; i++) {
      auto &chk = chunks_[i];
      std::cout << "Chunk " << i << " : h = " << chk.chunk_pointer_ << std::endl;
      char *top = chk.chunk_addr_ + chk.chunk_size_;
      char *base = chk.chunk_addr_;
      char *p = base;
      std::cout << "|";
      while (p < top) {
        if (!B(p)->IsBlockFree()) std::cout << "*";
        std::cout << C(p) - base << "," << B(p)->GetBlockSize() << "(" << B(p)->prev_free_ << "," << B(p)->next_free_
                  << ")|";
        p += 2 * Z + B(p)->GetBlockSize();
      }
      std::cout << std::endl;
    }
  }

  inline char *AllocateInChunk(size_t chunk_idx, size_t size) {
    auto &chk = chunks_[chunk_idx];
    auto chunk_size = chk.chunk_size_;
    char *base = chk.chunk_addr_;
    char *top = base + chunk_size;
    char *qh = base + chk.chunk_pointer_;
    while (qh < top) {
      size_t sz_current = B(qh)->GetBlockSize();
      if (sz_current >= size && B(qh)->IsBlockFree()) {
        // the free block is found
        char *ph = base + B(qh)->prev_free_;
        char *qf = qh + B(qh)->GetBlockSize() + Z;
        char *rh = base + B(qh)->next_free_;
        char *pf = nullptr, *rf = nullptr;
        if (B(qh)->GetBlockSize() - size > 8 * Z) {
          // do splitting
          char *nh = nullptr, *nf = nullptr;
          nh = qh + size + 2 * Z;
          nf = qf;
          B(nh)->next_free_ = B(nf)->next_free_ = rh - base;
          B(nh)->prev_free_ = B(nf)->prev_free_ = ph - base;
          if (OK(ph)) {
            pf = ph + B(ph)->GetBlockSize() + Z;
            B(ph)->next_free_ = B(pf)->next_free_ = nh - base;
            B(nh)->prev_free_ = B(nf)->prev_free_ = ph - base;
          } else {  // this is the first chunk
            chk.chunk_pointer_ = nh - base;
          }
          if (OK(rh)) {
            rf = rh + B(rh)->GetBlockSize() + Z;
            B(rh)->prev_free_ = B(rf)->prev_free_ = nh - base;
            B(nh)->next_free_ = B(nh)->next_free_ = rh - base;
          }
          auto old_size = B(qh)->GetBlockSize();
          auto new_size = old_size - size - 2 * Z;
          B(nh)->SetBlockFree(true);
          B(nf)->SetBlockFree(true);
          B(nh)->magic_ = BLOCK_MAGIC;
          B(nh)->SetBlockSize(new_size);
          B(nf)->SetBlockSize(new_size);
          B(qh)->SetBlockSize(size);
          B(nh - Z)->magic_ = BLOCK_MAGIC;
          B(nh - Z)->prev_free_ = B(nh - Z)->next_free_ = chunk_size;
          B(nh - Z)->SetBlockFree(false);
          B(nh - Z)->SetBlockSize(size);
        } else {
          // do not split
          if (OK(ph)) {
            pf = ph + B(ph)->GetBlockSize() + Z;
            B(ph)->next_free_ = B(pf)->next_free_ = B(qh)->next_free_;
          } else {
            chk.chunk_pointer_ = rh - base;
          }
          if (OK(rh)) {
            rf = rh + B(rh)->GetBlockSize() + Z;
            B(rh)->prev_free_ = B(rf)->prev_free_ = B(qh)->prev_free_;
          }
          B(qf)->SetBlockFree(false);
        }
        B(qh)->SetBlockFree(false);
#ifdef HEAP_LOGGING
        std::cout << "Allocate size " << size << " at chunk " << chunk_idx << " offset " << (qh - base) << " addr " << V(qh + Z) << std::endl;
        //std::cout << boost::stacktrace::stacktrace() << std::endl;
        //ShowHeap();
#endif
        return qh + Z;
      }
      qh = B(qh)->next_free_ + base;
    }
    return nullptr;
  }

  inline void ExpandChunk() {
#ifdef HEAP_LOGGING
    std::cout << "Expanding Chunk.Current Chunk size :" << num_chunks_ << std::endl;
#endif
    size_t n_chks = num_chunks_;
    size_t chk_size = GetSmallestChunkSize() << n_chks;
    ChunkHeader h_chk;
    h_chk.chunk_addr_ = reinterpret_cast<char *>(malloc(chk_size));
    h_chk.chunk_size_ = chk_size;
    h_chk.chunk_pointer_ = 0;
    BlockHeader *bh = reinterpret_cast<BlockHeader *>(h_chk.chunk_addr_);
    BlockHeader *bf = reinterpret_cast<BlockHeader *>(h_chk.chunk_addr_ + h_chk.chunk_size_ - sizeof(BlockHeader));
    bh->SetBlockFree(true);
    bh->magic_ = bf->magic_ = BLOCK_MAGIC;
    bh->SetBlockSize(chk_size - 2 * sizeof(BlockHeader));
    bh->next_free_ = chk_size;
    bh->prev_free_ = chk_size;

    bf->SetBlockFree(true);
    bf->SetBlockSize(chk_size - 2 * sizeof(BlockHeader));
    bf->next_free_ = chk_size;
    bf->prev_free_ = chk_size;

    num_chunks_ += 1;
    chunks_ = reinterpret_cast<ChunkHeader *>(realloc(chunks_, num_chunks_ * sizeof(ChunkHeader)));
    chunks_[num_chunks_ - 1] = h_chk;
    if (num_chunks_ > max_chunks_) max_chunks_ = num_chunks_;
  }
  size_t num_chunks_;
  ChunkHeader *chunks_;
  size_t max_chunks_;

#pragma GCC diagnostic pop
};

class ListHeap : public MemHeap {
  struct Node_ {
    void *val;
    struct Node_ *next;
  };
  typedef struct Node_ Node;

 public:
  ListHeap() {
    head_allocated_ = new Node;
    head_allocated_->val = nullptr;
    head_allocated_->next = nullptr;
  }
  ~ListHeap() {
    std::cout << "Destructing listHeap. allocated = " << allocated_count_ << ", released = " << freed_count_
              << std::endl;

    Node *p = head_allocated_;
    while (p != nullptr) {
      Node *next = p->next;
      if (p->val != nullptr) free(p->val);
      delete p;
      p = next;
    }
  }

  void *Allocate(size_t size) {
    this->allocated_count_ += 1;
    void *buf = malloc(size);
    ASSERT(buf != nullptr, "Out of memory exception");
    Node *new_node = new Node;
    new_node->val = buf;
    new_node->next = head_allocated_->next;
    head_allocated_->next = new_node;
    return buf;
  }

  void Free(void *ptr) {
    this->freed_count_ += 1;
    if (ptr == nullptr) {
      return;
    }
    Node *p = head_allocated_;
    while (p->next != nullptr) {
      if (p->next->val == ptr) {
        free(p->next->val);
        Node *temp = p->next;
        p->next = p->next->next;
        delete temp;
      } else
        p = p->next;
    }
  }

 private:
  Node *head_allocated_;
};

#endif  // MINISQL_MEM_HEAP_H
