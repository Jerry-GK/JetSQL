#ifndef MINISQL_LRU_REPLACER_OLD_H
#define MINISQL_LRU_REPLACER_OLD_H

#include <list>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacerOld : public Replacer {
public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacerOld(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacerOld() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

private:
  // add your own private member variables here

  size_t num_frames_;
  unordered_map<frame_id_t, int> lru_list_;
};

#endif  // MINISQL_LRU_REPLACER_H
