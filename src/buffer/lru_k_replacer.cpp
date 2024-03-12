//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock<std::mutex> lock(latch_);
  bool is_evict = false;
  if (!inf_.empty()) {
    for (auto it = inf_.rbegin(); it != inf_.rend(); ++it) {
      if (entries_[*it].is_evictable_) {
        *frame_id = *it;
        inf_.erase(std::next(it).base());
        is_evict = true;
        break;
      }
    }
  }
  if (!is_evict && !kth_.empty()) {
    for (auto it = kth_.rbegin(); it != kth_.rend(); ++it) {
      if (entries_[*it].is_evictable_) {
        *frame_id = *it;
        kth_.erase(std::next(it).base());
        is_evict = true;
        break;
      }
    }
  }

  if (is_evict) {
    --curr_size_;
    entries_.erase(*frame_id);
    return true;
  }
  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalue frame_id") + std::to_string(frame_id));
  }
  size_t nums_hint = ++entries_[frame_id].hint_count_;
  if (nums_hint == 1) {
    ++curr_size_;
    inf_.emplace_front(frame_id);
    entries_[frame_id].pos_ = inf_.begin();
  } else {
    if (nums_hint == k_) {
      inf_.erase(entries_[frame_id].pos_);
      kth_.emplace_front(frame_id);
      entries_[frame_id].pos_ = kth_.begin();
    } else if (nums_hint > k_) {
      kth_.erase(entries_[frame_id].pos_);
      kth_.emplace_front(frame_id);
      entries_[frame_id].pos_ = kth_.begin();
    }
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalue frame_id") + std::to_string(frame_id));
  }
  if (entries_.find(frame_id) == entries_.end() || entries_[frame_id].is_evictable_ == set_evictable) {
    return;
  }
  if (!set_evictable) {
    curr_size_--;
    entries_[frame_id].is_evictable_ = false;
  } else {
    curr_size_++;
    entries_[frame_id].is_evictable_ = true;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock<std::mutex> lock(latch_);
  if (frame_id > static_cast<int>(replacer_size_)) {
    throw std::invalid_argument(std::string("Invalue frame_id") + std::to_string(frame_id));
  }
  if (entries_.find(frame_id) == entries_.end() || !entries_[frame_id].is_evictable_) {
    return;
  }
  if (entries_[frame_id].hint_count_ < k_) {
    inf_.erase(entries_[frame_id].pos_);
  } else {
    kth_.erase(entries_[frame_id].pos_);
  }
  --curr_size_;
  entries_.erase(frame_id);
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return this->curr_size_;
}

}  // namespace bustub
