//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }

  // TODO(students): remove this line after you have implemented the buffer pool manager
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  /**
   * TODO(P1): Add implementation
   *
   * @brief Create a new page in the buffer pool. Set page_id to the new page's id, or nullptr if all frames
   * are currently in use and not evictable (in another word, pinned).
   *
   * You should pick the replacement frame from either the free list or the replacer (always find from the free list
   * first), and then call the AllocatePage() method to get a new page id. If the replacement frame has a dirty page,
   * you should write it back to the disk first. You also need to reset the memory and metadata for the new page.
   *
   * Remember to "Pin" the frame by calling replacer.SetEvictable(frame_id, false)
   * so that the replacer wouldn't evict the frame before the buffer pool manager "Unpin"s it.
   * Also, remember to record the access history of the frame in the replacer for the lru-k algorithm to work.
   *
   * @param[out] page_id id of created page
   * @return nullptr if no new pages could be created, otherwise pointer to new page
   */
  std::scoped_lock<std::mutex> lock(latch_);
  Page *page = nullptr;
  frame_id_t frame_id = -1;
  if (!free_list_.empty()) {  // find from the free list first
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  } else if (replacer_->Evict(&frame_id)) {  // find from the replacer
    page = pages_ + frame_id;
    page_table_->Remove(page->GetPageId());
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->GetPageId(), page->data_);
      ResetPage(page);
      // FlushPgImp(page->GetPageId());
    }
  }
  if (page != nullptr) {  // created a new page successful
    page->page_id_ = AllocatePage();
    *page_id = page->page_id_;
    page_table_->Insert(*page_id, frame_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_++;
  }
  return page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  /**
   * TODO(P1): Add implementation
   *
   * @brief Fetch the requested page from the buffer pool. Return nullptr if page_id needs to be fetched from the disk
   * but all frames are currently in use and not evictable (in another word, pinned).
   *
   * First search for page_id in the buffer pool. If not found, pick a replacement frame from either the free list or
   * the replacer (always find from the free list first), read the page from disk by calling disk_manager_->ReadPage(),
   * and replace the old page in the frame. Similar to NewPgImp(), if the old page is dirty, you need to write it back
   * to disk and update the metadata of the new page
   *
   * In addition, remember to disable eviction and record the access history of the frame like you did for NewPgImp().
   *
   * @param page_id id of page to be fetched
   * @return nullptr if page_id cannot be fetched, otherwise pointer to the requested page
   */
  std::scoped_lock<std::mutex> lock(latch_);
  Page *page = nullptr;
  frame_id_t frame_id = -1;
  if (page_table_->Find(page_id, frame_id)) {
    page = pages_ + frame_id;
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_++;
    return page;
  }
  if (!free_list_.empty()) {  // find from the free list first
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = pages_ + frame_id;
  } else if (replacer_->Evict(&frame_id)) {  // find from the replacer
    page = pages_ + frame_id;
    page_table_->Remove(page->GetPageId());
    if (page->is_dirty_) {
      disk_manager_->WritePage(page->GetPageId(), page->data_);
      ResetPage(page);
      // FlushPgImp(page->GetPageId());
    }
  }
  if (page != nullptr) {  // created a new page successful
    disk_manager_->ReadPage(page_id, page->GetData());
    page->page_id_ = page_id;
    page_table_->Insert(page_id, frame_id);
    replacer_->RecordAccess(frame_id);
    replacer_->SetEvictable(frame_id, false);
    page->pin_count_++;
  }
  return page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  /**
   * TODO(P1): Add implementation
   *
   * @brief Unpin the target page from the buffer pool. If page_id is not in the buffer pool or its pin count is already
   * 0, return false.
   *
   * Decrement the pin count of a page. If the pin count reaches 0, the frame should be evictable by the replacer.
   * Also, set the dirty flag on the page to indicate if the page was modified.
   *
   * @param page_id id of page to be unpinned
   * @param is_dirty true if the page should be marked as dirty, false otherwise
   * @return false if the page is not in the page table or its pin count is <= 0 before this call, true otherwise
   */
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = nullptr;
  page = pages_ + frame_id;
  if (page->GetPinCount() > 0) {
    --page->pin_count_;
    if (is_dirty) {
      page->is_dirty_ = true;
    }
    if (page->pin_count_ == 0) {
      replacer_->SetEvictable(frame_id, true);
    }
    return true;
  }
  return false;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush the target page to disk.
   *
   * Use the DiskManager::WritePage() method to flush a page to disk, REGARDLESS of the dirty flag.
   * Unset the dirty flag of the page after flushing.
   *
   * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
   * @return false if the page could not be found in the page table, true otherwise
   */
  assert(page_id != INVALID_PAGE_ID);
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return false;
  }
  Page *page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  /**
   * TODO(P1): Add implementation
   *
   * @brief Flush all the pages in the buffer pool to disk.
   */
  std::scoped_lock<std::mutex> lock(latch_);
  for (size_t i = 0; i < pool_size_; ++i) {
    Page *page = pages_ + i;
    auto page_id = page->GetPageId();
    if (page_id != INVALID_PAGE_ID) {
      disk_manager_->WritePage(page_id, page->data_);
      page->is_dirty_ = false;
    }
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  /**
   * TODO(P1): Add implementation
   *
   * @brief Delete a page from the buffer pool. If page_id is not in the buffer pool, do nothing and return true. If the
   * page is pinned and cannot be deleted, return false immediately.
   *
   * After deleting the page from the page table, stop tracking the frame in the replacer and add the frame
   * back to the free list. Also, reset the page's memory and metadata. Finally, you should call DeallocatePage() to
   * imitate freeing the page on the disk.
   *
   * @param page_id id of page to be deleted
   * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
   */
  std::scoped_lock<std::mutex> lock(latch_);
  frame_id_t frame_id = -1;
  if (!page_table_->Find(page_id, frame_id)) {
    return true;
  }
  Page *page = pages_ + frame_id;
  if (page->pin_count_ > 0) {
    return false;
  }
  free_list_.emplace_back(frame_id);
  replacer_->Remove(frame_id);
  page_table_->Remove(page_id);
  ResetPage(page);
  DeallocatePage(page_id);
  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

// void BufferPoolManagerInstance::CheckPinCount() {
//   std::lock_guard lock(latch_);
//   bool clear = true;
//   for (size_t id = 0; id < pool_size_; ++id) {
//     Page *page = pages_ + id;
//     auto page_id = page->page_id_;
//     if (page_id != INVALID_PAGE_ID && page_id != 0 && page->pin_count_ > 0) {
//       LOG_WARN("\033[1;31m page %d pin_count %d != 0\033[0m", page->page_id_, page->pin_count_);
//       clear = false;
//     }
//   }
//   if (clear) {
//     LOG_WARN("\033[1;31m all clear \033[0m");
//   }
// }

void BufferPoolManagerInstance::ResetPage(Page *page) {
  page->ResetMemory();
  page->is_dirty_ = false;
  page->page_id_ = INVALID_PAGE_ID;
}

}  // namespace bustub
