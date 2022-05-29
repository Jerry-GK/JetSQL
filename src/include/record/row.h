#ifndef MINISQL_ROW_H
#define MINISQL_ROW_H

#include <cstddef>
#include <cstring>
#include <memory>
#include <ostream>
#include <vector>
#include "common/macros.h"
#include "common/rowid.h"
#include "record/field.h"
#include "record/schema.h"
#include "record/types.h"
#include "utils/mem_heap.h"

/**
 *  Row format:
 * -------------------------------------------
 * | Header | Field-1 | ... | Field-N |
 * -------------------------------------------
 *  Header format:
 * --------------------------------------------
 * | Field Nums | Null bitmap |
 * -------------------------------------------
 *
 *
 */
class Row {
 public:
  /**
   * Row used for insert
   * Field integrity should check by upper level
   */
  explicit Row(std::vector<Field> &fields, MemHeap *heap) {
    // deep copy
    size_t cnt_other = fields.size();
    field_count_ = fields.size();
    heap_ = heap;
    fields_ = reinterpret_cast<Field *>(heap->Allocate(cnt_other * sizeof(Field)));
    for (size_t i = 0; i < cnt_other; i++) new (fields_ + i) Field(fields[i], heap_);
  }

  /**
   * Row used for deserialize
   */
  explicit Row() = delete;

  /**
   * Row used for deserialize and update
   */
  explicit Row(RowId rid, MemHeap *heap) : rid_(rid) {
    fields_ = nullptr;
    heap_ = heap;
    field_count_ = 0;
  }

  /**
   * Row copy function
   */
  Row(const Row &other) {
    heap_ = other.heap_;
    field_count_ = other.field_count_;
    this->rid_ = other.rid_;
    if (other.heap_ && other.field_count_) {
      fields_ = reinterpret_cast<Field *>(heap_->Allocate(field_count_ * sizeof(Field)));
      for (size_t i = 0; i < field_count_; i++) new (fields_ + i) Field(other.fields_[i], heap_);
    } else {
      fields_ = nullptr;
    }
  }

  Row &operator=(const Row &other) {
    heap_ = other.heap_;
    field_count_ = other.field_count_;
    this->rid_ = other.rid_;
    if (other.heap_ && other.field_count_) {
      fields_ = reinterpret_cast<Field *>(heap_->Allocate(field_count_ * sizeof(Field)));
      for (size_t i = 0; i < field_count_; i++) new (fields_ + i) Field(other.fields_[i], heap_);
    } else {
      fields_ = nullptr;
    }
    return *this;
  }

  ~Row() {
    if (fields_ && heap_) {
      for (size_t i = 0; i < field_count_; i++) fields_[i].~Field();
      heap_->Free(this->fields_);
    }
  };

  /**
   * Note: Make sure that bytes write to buf is equal to GetSerializedSize()
   */
  uint32_t SerializeTo(char *buf, Schema *schema) const;

  uint32_t DeserializeFrom(char *buf, Schema *schema);

  /**
   * For empty row, return 0
   * For non-empty row with null fields, eg: |null|null|null|, return header size only
   * @return
   */
  uint32_t GetSerializedSize(Schema *schema) const;

  inline const RowId GetRowId() const { return rid_; }

  inline void SetRowId(RowId rid) { rid_ = rid; }

  void Delete(MemHeap &heap) { heap.Free(fields_); }

  inline Field *GetFields() { return fields_; }

  inline Field *GetField(uint32_t idx) const {
    ASSERT(idx < field_count_, "Failed to access field");
    return fields_ + idx;
  }

  inline size_t GetFieldCount() const { return field_count_; }

 private:
  RowId rid_{};
  MemHeap *heap_;
  size_t field_count_;
  Field *fields_;
};

#endif  // MINISQL_TUPLE_H
