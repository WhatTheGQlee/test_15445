//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/index/b_plus_tree.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/logger.h"
#include "concurrency/transaction.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"

namespace bustub {

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator>

/**
 * Main class providing the API for the Interactive B+ Tree.
 *
 * Implementation of simple b+ tree data structure where internal pages direct
 * the search and leaf pages contain actual data.
 * (1) We only support unique key
 * (2) support insert & remove
 * (3) The structure should shrink and grow dynamically
 * (4) Implement index iterator for range scan
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>;

  enum class OpeType { FIND, INSERT, REMOVE };

 public:
  explicit BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                     int leaf_max_size = LEAF_PAGE_SIZE, int internal_max_size = INTERNAL_PAGE_SIZE);

  // Returns true if this B+ tree has no keys and values.
  auto IsEmpty() const -> bool;

  // Latch for B+ tree
  auto IsSafe(BPlusTreePage *page, OpeType op) -> bool;
  void UnlockPageSet(Transaction *transaction, bool is_dirty);
  auto GetPageFromTransaction(page_id_t page_id, Transaction *transaction) -> Page *;

  // Insert a key-value pair into this B+ tree.
  auto Insert(const KeyType &key, const ValueType &value, Transaction *transaction = nullptr) -> bool;
  void InsertInParent(BPlusTreePage *left_node, BPlusTreePage *right_node, const KeyType &key,
                      Transaction *transaction = nullptr);

  // Remove a key and its value from this B+ tree.
  void Remove(const KeyType &key, Transaction *transaction = nullptr);
  void HandleUnderFlow(BPlusTreePage *page, Transaction *transaction = nullptr);
  void GetSiblings(BPlusTreePage *page, page_id_t *left_page_id, page_id_t *right_page_id, Page *parent_page);
  auto TryBorrow(BPlusTreePage *page, Page *siblingpage, InternalPage *parent_page, bool is_left) -> bool;

  void Merge(BPlusTreePage *left_page, BPlusTreePage *right_page, InternalPage *parent_page);

  void UnpinSiblingPage(Page *left_page_id, Page *right_page_id);

  // return the value associated with a given key
  auto GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction = nullptr) -> bool;
  auto GetLeafPage(const KeyType &key, Transaction *transaction, OpeType op, bool is_first = true) -> Page *;

  // return the page id of the root node
  auto GetRootPageId() -> page_id_t;

  // index iterator
  auto Begin() -> INDEXITERATOR_TYPE;
  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;
  auto End() -> INDEXITERATOR_TYPE;

  // print the B+ tree
  void Print(BufferPoolManager *bpm);

  // draw the B+ tree
  void Draw(BufferPoolManager *bpm, const std::string &outf);

  // read data from file and insert one by one
  void InsertFromFile(const std::string &file_name, Transaction *transaction = nullptr);

  // read data from file and remove one by one
  void RemoveFromFile(const std::string &file_name, Transaction *transaction = nullptr);

 private:
  auto CastBPlusPage(Page *page) const -> BPlusTreePage * { return reinterpret_cast<BPlusTreePage *>(page->GetData()); }
  auto CastLeafPage(Page *page) const -> LeafPage * { return reinterpret_cast<LeafPage *>(page->GetData()); }
  auto CastInternalPage(Page *page) const -> InternalPage * {
    return reinterpret_cast<InternalPage *>(page->GetData());
  }

  auto CastLeafPage(BPlusTreePage *node) const -> LeafPage * { return static_cast<LeafPage *>(node); }
  auto CastInternalPage(BPlusTreePage *node) const -> InternalPage * { return static_cast<InternalPage *>(node); }

  void UpdateChildNode(InternalPage *node, int begin, int end);

  void UpdateRootPageId(int insert_record = 0);

  /* Debug Routines for FREE!! */
  void ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const;

  void ToString(BPlusTreePage *page, BufferPoolManager *bpm) const;

  // member variable
  std::string index_name_;
  page_id_t root_page_id_;
  BufferPoolManager *buffer_pool_manager_;
  KeyComparator comparator_;
  int leaf_max_size_;
  int internal_max_size_;

  ///

  ReaderWriterLatch root_latch_;
  // mutable std::mutex latch_;
  // class RootMutex {
  //  public:
  //   inline void WLatch() {
  //     latch_.lock();
  //     // LOG_INFO("ROOT_LATCH_LOCK");
  //   }
  //   inline void WUnLatch() {
  //     latch_.unlock();
  //     // LOG_INFO("ROOT_LATCH_UNLOCK");
  //   }
  //   inline void RLatch() { latch_.lock_shared(); }
  //   inline void RUnLatch() { latch_.unlock_shared(); }

  //  private:
  //   std::shared_mutex latch_;
  // };

  // RootMutex root_latch_;  // mutex for root_page_id_
};

}  // namespace bustub
