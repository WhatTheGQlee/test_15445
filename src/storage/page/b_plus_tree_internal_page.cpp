//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size > static_cast<int>(INTERNAL_PAGE_SIZE) ? INTERNAL_PAGE_SIZE : max_size);
  SetSize(0);
  SetParentPageId(parent_id);
  SetPageId(page_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  // replace with your own code
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::FindIndex(const ValueType &value) const -> int {
  int i;
  for (i = 0; i < GetSize(); ++i) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftLeft(int index) {
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftRight() {
  std::copy_backward(array_, array_ + GetSize(), array_ + 1 + GetSize());
  IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LowerBound(const KeyType &key, const KeyComparator &comparator) const -> int {
  int left = 1;
  int right = GetSize();
  while (left < right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) <= 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::LowerBound(const std::vector<MappingType> &vec, const KeyType &key,
                                                const KeyComparator &comparator) const -> int {
  int left = 1;
  int right = vec.size();
  while (left < right) {
    int mid = (left + right) / 2;
    if (comparator(key, KeyAt(mid)) <= 0) {
      right = mid;
    } else {
      left = mid + 1;
    }
  }
  return left;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> bool {
  int index = LowerBound(key, comparator);
  if (index < GetSize() && comparator(key, KeyAt(index)) == 0) {
    return false;
  }
  array_[GetSize()] = MappingType{key, value};
  for (int j = GetSize(); j > index; --j) {
    std::swap(array_[j], array_[j - 1]);
  }
  IncreaseSize(1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(B_PLUS_TREE_INTERNAL_PAGE_TYPE *new_page, const KeyType &key,
                                           const ValueType &value, const KeyComparator &comparator) {
  std::vector<MappingType> vec(array_, array_ + GetMaxSize());
  vec.reserve(GetMaxSize() + 1);
  int index = LowerBound(vec, key, comparator);
  vec.insert(vec.begin() + index, {key, value});
  // for (MappingType num : vec) {
  //   LOG_DEBUG("%ld ", num.first.ToString());
  // }
  int x = GetMinSize();
  SetSize(x);
  // for (int i = 0;i < x;i++) {
  //   LOG_DEBUG("%ld %d", array_[x].first.ToString(), x);
  // }
  std::copy(vec.data(), vec.data() + x, array_);
  std::copy(vec.data() + x, vec.data() + vec.size(), new_page->array_);
  new_page->SetSize(vec.size() - x);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
