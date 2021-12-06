//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <algorithm>
#include <cassert>
#include <iostream>

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  assert(num_pages > 0);
  this->max_pages_ = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  auto lst = &this->data_;
  if (lst->empty()) {
    frame_id = nullptr;
    return false;
  }

  auto last_elem = std::prev(lst->end());
  *frame_id = *last_elem;
  lst->erase(last_elem);
  id_position_map_.erase(*frame_id);
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(lock_);
  auto lst = &this->data_;
  if (id_position_map_.count(frame_id) == 0) {
    return;
  }
  lst->erase(id_position_map_[frame_id]);
  id_position_map_.erase(frame_id);

  // auto pos = std::find(lst->begin(), lst->end(), frame_id);
  // if (pos != lst->end()) {
  //   // found, remove from replacer
  //   lst->erase(pos);
  // }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(lock_);
  auto lst = &this->data_;
  if (id_position_map_.count(frame_id) == 0) {
    // not found, add to the first position
    while (lst->size() >= max_pages_) {
      lst->erase(std::prev(lst->end()));
    }

    lst->emplace_front(frame_id);
    id_position_map_[frame_id] = lst->begin();
  }
  // auto pos = std::find(lst->begin(), lst->end(), frame_id);
  // if (pos == lst->end()) {
  //   // not found, add to the first position
  //   while (lst->size() >= max_pages_) {
  //     lst->erase(std::prev(lst->end()));
  //   }

  //   lst->emplace_front(frame_id);
  // }
}

size_t LRUReplacer::Size() { return this->data_.size(); }

}  // namespace bustub
