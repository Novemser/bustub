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

#include <iostream>

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

bool BufferPoolManagerInstance::GetAvailablePage(frame_id_t *new_frame_id) {
  if (!free_list_.empty()) {
    *new_frame_id = *(free_list_.begin());
    free_list_.pop_front();
    return true;
  }

  bool available_buf_page = false;
  for (auto it = page_table_.begin(); it != page_table_.end(); it = std::next(it)) {
    if (pages_[it->second].GetPinCount() <= 0) {
      available_buf_page = true;
      break;
    }
  }

  if (!available_buf_page) {
    return false;
  }

  frame_id_t victim_page_id = -1;
  auto res = replacer_->Victim(&victim_page_id);
  assert(res && victim_page_id != -1);

  auto victim_page = &pages_[victim_page_id];
  if (victim_page->is_dirty_) {
    // flush back the data
    disk_manager_->WritePage(victim_page->GetPageId(), victim_page->GetData());
  }
  // leave the page from pool
  page_table_.erase(victim_page->GetPageId());
  *new_frame_id = victim_page_id;
  return true;
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_table_.count(page_id) <= 0) {
    return false;
  }
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].GetData());
  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgImp(pages_[i].GetPageId());
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // 0.   Make sure you call AllocatePage!

  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  frame_id_t new_page_frame_id = -1;
  const auto new_page_id = BufferPoolManagerInstance::AllocatePage();
  if (!GetAvailablePage(&new_page_frame_id)) {
    return nullptr;
  }

  const auto new_page = &pages_[new_page_frame_id];
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  new_page->ResetMemory();
  new_page->pin_count_++;
  new_page->is_dirty_ = false;
  *page_id = new_page->page_id_ = new_page_id;
  page_table_.emplace(std::pair<page_id_t, frame_id_t>(new_page_id, new_page_frame_id));

  replacer_->Pin(new_page_frame_id);

  return new_page;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);

  // 1.     Search the page table for the requested page (P).
  for (auto it = page_table_.begin(); it != page_table_.end(); it = std::next(it)) {
    // 1.1    If P exists, pin it and return it immediately.
    page_id_t elem_page_id = it->first;
    frame_id_t elem_frame_id = it->second;

    if (elem_page_id == page_id) {
      pages_[elem_frame_id].pin_count_++;
      replacer_->Pin(elem_frame_id);
      return &pages_[elem_frame_id];
    }
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  frame_id_t found_page_frame_id = -1;
  if (!GetAvailablePage(&found_page_frame_id)) {
    return nullptr;
  }
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  auto found_page = &pages_[found_page_frame_id];
  if (found_page->IsDirty()) {
    disk_manager_->WritePage(found_page->GetPageId(), found_page->GetData());
  }
  // 3.     Delete R from the page table and insert P.
  page_table_.erase(found_page->GetPageId());
  page_table_[page_id] = found_page_frame_id;
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  disk_manager_->ReadPage(page_id, found_page->GetData());
  found_page->page_id_ = page_id;
  found_page->pin_count_++;
  found_page->is_dirty_ = false;
  replacer_->Pin(found_page_frame_id);

  return found_page;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  std::lock_guard<std::mutex> lock(latch_);
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  bool can_delete = false;
  frame_id_t frame_id = -1;
  for (auto it = page_table_.begin(); it != page_table_.end(); it = std::next(it)) {
    if (it->first == page_id) {
      if (pages_[it->second].GetPinCount() != 0) {
        // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
        return false;
      }

      can_delete = true;
      frame_id = it->second;
      break;
    }
  }

  if (!can_delete) {
    // 1.   If P does not exist, return true.
    return true;
  }
  
  if (pages_[frame_id].IsDirty()) {
    disk_manager_->WritePage(pages_[frame_id].GetPageId(), pages_[frame_id].GetData());
  }

  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(page_id);
  DeallocatePage(pages_[frame_id].GetPageId());
  pages_[frame_id].ResetMemory();
  pages_[frame_id].is_dirty_ = false;
  replacer_->Unpin(frame_id);
  free_list_.emplace_back(frame_id);
  return true;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> lock(latch_);
  auto frame_id_iter = page_table_.find(page_id);
  // unpin non-existence page is not allowed
  if (frame_id_iter == page_table_.end()) {
    return false;
  }

  auto frame_id = (*frame_id_iter).second;
  auto pin_cnt = pages_[frame_id].GetPinCount();
  if (is_dirty) {
    pages_[frame_id].is_dirty_ = is_dirty;
  }

  if (pin_cnt <= 0) {
    return false;
  }
  pages_[frame_id].pin_count_--;

  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
