/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(page_id_t page_id, int position, BufferPoolManager *buffer_pool_manager,
                                  MappingType value)
    : page_id_(page_id), position_(position), buffer_pool_manager_(buffer_pool_manager), value_(std::move(value)) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return value_; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (page_id_ == INVALID_PAGE_ID) {
    return *this;
  }
  Page *page = buffer_pool_manager_->FetchPage(page_id_);
  auto node = reinterpret_cast<LeafPage *>(page->GetData());
  if (++position_ != node->GetSize()) {
    value_ = node->KeyValueAt(position_);
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return *this;
  }
  if (position_ == node->GetSize() && node->GetNextPageId() == INVALID_PAGE_ID) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    *this = {};
    return *this;
  }
  Page *next_page = buffer_pool_manager_->FetchPage(node->GetNextPageId());
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  page = next_page;
  node = reinterpret_cast<LeafPage *>(page);

  position_ = 0;
  page_id_ = page->GetPageId();
  value_ = node->KeyValueAt(position_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return *this;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
