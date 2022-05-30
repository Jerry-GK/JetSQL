#ifndef INDEX_KEY_H
#define INDEX_KEY_H

#include <cstdint>
#include <iomanip>
#include <iostream>
#include <memory>
#include "record/row.h"

using namespace std;
struct IndexKey {
  uint8_t keysize;
  char value[0];

  friend std::ostream &operator<<(std::ostream &os, const IndexKey *key) {
    for (int i = 0; i < key->keysize; i++)
      os << std::hex << std::setw(2) << std::setfill('0') << (unsigned int)(key->value[i] & 0xff);
    return os;
  }
  static IndexKey *Create(int sz) {
    char *r = (char *)calloc(sizeof(IndexKey) + sz, 1);
    IndexKey *k = reinterpret_cast<IndexKey *>(r);
    k->keysize = sz;
    return k;
  }
  template <typename T>
  static IndexKey *Create(int sz, T &val) {
    char *r = new char[sizeof(IndexKey) + sz];
    IndexKey *k = reinterpret_cast<IndexKey *>(r);
    k->keysize = sz;
    k->SetValue(val);
    return k;
  }
  bool operator==(const IndexKey &another) {
    if (keysize != another.keysize) return false;
    return memcmp(value, another.value, keysize) == 0;
  }
  static void operator delete(void *p) { return delete[] static_cast<char *>(p); }
  static IndexKey *SerializeFromKey(char *buf, const Row &row, Schema *schema, size_t keysize);

  inline void DeserializeToKey(Row &key, Schema *schema) const {
    // uint32_t ofs =
    key.DeserializeFrom(const_cast<char *>(value), schema);

    // ASSERT(ofs <= keysize, "Index key size exceed max key size.");
    return;
  }

  template <typename T>
  void SetValue(T &val) {
    memcpy(value, &val, sizeof(T));
  }

  template <typename T>
  T &GetValue() {
    return *reinterpret_cast<T *>(value);
  }
};
class IndexKeyComparator {
 public:
  inline int operator()(const IndexKey *lhs, const IndexKey *rhs) const {
    if (!key_schema_) {
      for (int i = lhs->keysize - 1; i >= 0; i--) {
        if ((unsigned char)lhs->value[i] > (unsigned char)rhs->value[i]) return 1;
        if ((unsigned char)lhs->value[i] < (unsigned char)rhs->value[i]) return -1;
      }

      return 0;
    }
    int column_count = key_schema_->GetColumnCount();
    Row lhs_key(INVALID_ROWID, heap_);
    Row rhs_key(INVALID_ROWID, heap_);

    lhs->DeserializeToKey(lhs_key, key_schema_);
    rhs->DeserializeToKey(rhs_key, key_schema_);

    for (int i = 0; i < column_count; i++) {
      Field *lhs_value = lhs_key.GetField(i);
      Field *rhs_value = rhs_key.GetField(i);
      // std::cout << "Comparing " << lhs_value->GetDataStr() << " with " << rhs_value->GetDataStr() << std::endl;

      if (lhs_value->CompareLessThan(*rhs_value) == CmpBool::kTrue) {
        return -1;
      }
      if (lhs_value->CompareGreaterThan(*rhs_value) == CmpBool::kTrue) {
        return 1;
      }
    }
    return 0;
  }

  IndexKeyComparator(const IndexKeyComparator &other) {
    this->key_schema_ = other.key_schema_;
    heap_ = new ManagedHeap;
  }

  // constructor
  IndexKeyComparator(Schema *key_schema) : key_schema_(key_schema) { heap_ = new ManagedHeap; }
  ~IndexKeyComparator() { delete heap_; }

 private:
  Schema *key_schema_;
  MemHeap *heap_;
};

#endif