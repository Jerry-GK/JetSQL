#ifndef MINISQL_MEM_HEAP_H
#define MINISQL_MEM_HEAP_H

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <unordered_set>
#include <vector>
#include <iostream>
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

  void Stat(){
    std::cout << "Allocated = " << allocated_count_ << ";Freed = " << freed_count_ << std::endl;
  }
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
    // std::cout << "Allocate" << size << std::endl;
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
  uint32_t block_pointer_;

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

template <int minsize = 512>
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
    #ifdef HEAP_LOGGING
    std::cout << "Allocate " << reinterpret_cast<void *>(ptr) << std::endl;
    #endif
    return ptr;
  }

  void Free(void *ptr) {
    freed_count_ += 1;
    #ifdef HEAP_LOGGING
    std::cout << "Free " << reinterpret_cast<void *>(ptr) << std::endl;
    #endif
    char *p = reinterpret_cast<char *>(ptr);
    char *block_header = p - sizeof(BlockHeader);
    BlockHeader *bh = reinterpret_cast<BlockHeader *>(block_header);
    ASSERT(bh->magic_ == BLOCK_MAGIC,"Address does not belong to this heap!");;
    // find the chunk for this ptr
    for (size_t i = 0; i < num_chunks_; i++) {
      auto &it = chunks_[i];
      size_t chunksz = it.chunk_size_;
      char *chunk_top = it.chunk_addr_ + it.chunk_size_;
      char *chunk_base = it.chunk_addr_;
      if (p >= it.chunk_addr_ + sizeof(BlockHeader) && p < it.chunk_addr_ + chunksz - sizeof(BlockHeader)) {
        if (bh->IsBlockFree()) ASSERT(0, "Double free in managed heap!");
        size_t sz = bh->GetBlockSize();
        char *bh_next_addr = sz + block_header + 2 * sizeof(BlockHeader);
        char *bf_prev_addr = block_header - sizeof(BlockHeader);
        BlockHeader *bf = reinterpret_cast<BlockHeader *>(block_header + bh->GetBlockSize() + sizeof(BlockHeader));
        size_t blksz = bh->GetBlockSize();
        bool merge = false;
        if (bh_next_addr < chunk_top) {
          BlockHeader *bh_next = reinterpret_cast<BlockHeader *>(bh_next_addr);
          if (bh_next->IsBlockFree()) {
            // merge with next block
            merge = true;
            char *bf_next_addr = bh_next_addr + sizeof(BlockHeader) + bh_next->GetBlockSize();
            blksz += bh_next->GetBlockSize() + 2 * sizeof(BlockHeader);
            bf = reinterpret_cast<BlockHeader *>(bf_next_addr);
          }
        }
        if (bf_prev_addr > chunk_base) {
          BlockHeader *bf_prev = reinterpret_cast<BlockHeader *>(bf_prev_addr);
          if (!bf_prev->IsBlockFree()) return;
          char *bh_prev_addr = bf_prev_addr - sizeof(BlockHeader) - bf_prev->GetBlockSize();
          if (bh_prev_addr > chunk_base) {
            // merge with prev block
            merge = true;
            bh = reinterpret_cast<BlockHeader *>(bh_prev_addr);
            blksz += bf_prev->GetBlockSize() + 2 * sizeof(BlockHeader);
          }
        }
        bh->SetBlockFree(true);
        bf->SetBlockFree(true);
        if (merge) {
          bf->SetBlockSize(blksz);
          bf->SetBlockSize(blksz);
        }
        bh->block_pointer_ = it.chunk_pointer_;
        it.chunk_pointer_ = reinterpret_cast<char *>(bh) - chunk_base;
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

  inline char *AllocateInChunk(size_t chunk_idx, size_t size) {
    auto &chk = chunks_[chunk_idx];
    char *chunk_base = chk.chunk_addr_;
    char *chunk_top = chunk_base + chk.chunk_size_;
    char *block_header_addr = chunk_base + chk.chunk_pointer_;
    BlockHeader *bh_prev = nullptr;
    while (block_header_addr < chunk_top) {
      BlockHeader *bh = reinterpret_cast<BlockHeader *>(block_header_addr);
      size_t sz_current = bh->GetBlockSize();
      if (sz_current >= size && bh->IsBlockFree()) {
        bh->SetBlockFree(false);
        if (sz_current - size > 8 * sizeof(BlockHeader)) {
          // split the block
          char *bh_new_addr = block_header_addr + size + sizeof(BlockHeader) * 2;
          char *bf_new_addr = block_header_addr + sz_current - sizeof(BlockHeader);
          char *bf_addr = block_header_addr + size + sizeof(BlockHeader);
          BlockHeader *bh_new = reinterpret_cast<BlockHeader *>(bh_new_addr);
          BlockHeader *bf_new = reinterpret_cast<BlockHeader *>(bf_new_addr);
          BlockHeader *bf = reinterpret_cast<BlockHeader *>(bf_addr);
          bh_new->SetBlockFree(true);
          bh_new->magic_ = BLOCK_MAGIC;
          bh_new->SetBlockSize(sz_current - size - 2 * sizeof(BlockHeader));
          bf_new->SetBlockFree(true);
          bf_new->magic_ = BLOCK_MAGIC;
          bf_new->SetBlockSize(sz_current - size - 2 * sizeof(BlockHeader));
          bf->SetBlockFree(false);
          bf->magic_ = BLOCK_MAGIC;
          bf->SetBlockSize(size);
          bh->SetBlockSize(size);

          if (bh_prev == nullptr)
            chk.chunk_pointer_ = reinterpret_cast<char *>(bh_new) - chunk_base;
          else
            bh_prev->block_pointer_ = reinterpret_cast<char *>(bh_new) - chunk_base;
          bh_new->block_pointer_ = bh->block_pointer_;
        } else {
          char *bf_addr = block_header_addr + sz_current + sizeof(BlockHeader);
          BlockHeader *bf = reinterpret_cast<BlockHeader *>(bf_addr);
          bf->SetBlockFree(false);
          if (bh_prev == nullptr)
            chk.chunk_pointer_ = bh->block_pointer_;
          else
            bh_prev->block_pointer_ = bh->block_pointer_;
        }
        chk.chunk_pointer_ = block_header_addr + bh->GetBlockSize() + 2 * sizeof(BlockHeader) - chunk_base;
        bh->magic_ = BLOCK_MAGIC;
        return block_header_addr + sizeof(BlockHeader);
      }
      bh_prev = bh;
      block_header_addr = chunk_base + bh->block_pointer_;
    }
    return nullptr;
  }

  inline void ExpandChunk() {
    size_t n_chks = num_chunks_;
    size_t chk_size = GetSmallestChunkSize() << n_chks;
    ChunkHeader h_chk;
    h_chk.chunk_addr_ = reinterpret_cast<char *>(malloc(chk_size));
    h_chk.chunk_size_ = chk_size;
    h_chk.chunk_pointer_ = 0;
    BlockHeader *bh = reinterpret_cast<BlockHeader *>(h_chk.chunk_addr_);
    bh->SetBlockFree(true);
    bh->SetBlockSize(chk_size - 2 * sizeof(BlockHeader));
    bh->block_pointer_ = chk_size;
    num_chunks_ += 1;
    chunks_ = reinterpret_cast<ChunkHeader *>(realloc(chunks_, num_chunks_ * sizeof(ChunkHeader)));
    chunks_[num_chunks_ - 1] = h_chk;
  }
  size_t num_chunks_;
  ChunkHeader *chunks_;
};

#endif  // MINISQL_MEM_HEAP_H
