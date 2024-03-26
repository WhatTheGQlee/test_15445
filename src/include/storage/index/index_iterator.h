//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/index_iterator.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "buffer/buffer_pool_manager.h"
#include "common/config.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define INDEXITERATOR_TYPE IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

 public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(page_id_t page_id, int position, BufferPoolManager *buffer_pool_manager, MappingType value);

  ~IndexIterator();  // NOLINT

  auto IsEnd() -> bool;

  auto operator*() -> const MappingType &;

  auto operator++() -> IndexIterator &;

  auto operator==(const IndexIterator &itr) const -> bool {
    return position_ == itr.position_ && page_id_ == itr.page_id_;
  }

  auto operator!=(const IndexIterator &itr) const -> bool {
    return position_ != itr.position_ || page_id_ != itr.page_id_;
  }

 private:
  // add your own private member variables here
  page_id_t page_id_{INVALID_PAGE_ID};
  int position_{0};
  BufferPoolManager *buffer_pool_manager_{};
  MappingType value_;
};

}  // namespace bustub
