//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// parallel_buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <iostream>

#include "buffer/buffer_pool_manager_instance.h"
#include "buffer/parallel_buffer_pool_manager.h"

namespace bustub {

ParallelBufferPoolManager::ParallelBufferPoolManager(size_t num_instances, size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager) {
  // Allocate and create individual BufferPoolManagerInstances
  for (size_t i = 0; i < num_instances; i++) {
    buffer_pools_.emplace_back(new BufferPoolManagerInstance(pool_size, num_instances, i, disk_manager, log_manager));
  }
  num_instances_ = num_instances;
}

// Update constructor to destruct all BufferPoolManagerInstances and deallocate any associated memory
ParallelBufferPoolManager::~ParallelBufferPoolManager() {
  std::for_each(buffer_pools_.begin(), buffer_pools_.end(), [](BufferPoolManager *mgr) { delete mgr; });
  buffer_pools_.clear();
}

size_t ParallelBufferPoolManager::GetPoolSize() {
  // Get size of all BufferPoolManagerInstances
  return buffer_pools_[0]->GetPoolSize();
}

size_t ParallelBufferPoolManager::GetBufferPoolIndex(page_id_t page_id) {
  // std::cout << "page_bufferpool_mapping_[" << page_id << "]:" << page_bufferpool_mapping_[page_id] << std::endl;
  return page_bufferpool_mapping_[page_id];
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolAt(size_t index) {
  return buffer_pools_[GetBufferPoolIndex(index)];
}

BufferPoolManager *ParallelBufferPoolManager::GetBufferPoolManager(page_id_t page_id) {
  // Get BufferPoolManager responsible for handling given page id. You can use this method in your other methods.
  return GetBufferPoolAt(page_id);
}

Page *ParallelBufferPoolManager::FetchPgImp(page_id_t page_id) {
  // Fetch page for page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolAt(page_id)->FetchPage(page_id);
}

bool ParallelBufferPoolManager::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  // Unpin page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolAt(page_id)->UnpinPage(page_id, is_dirty);
}

bool ParallelBufferPoolManager::FlushPgImp(page_id_t page_id) {
  // Flush page_id from responsible BufferPoolManagerInstance
  return GetBufferPoolAt(page_id)->FlushPage(page_id);
}

Page *ParallelBufferPoolManager::NewPgImp(page_id_t *page_id) {
  // create new page. We will request page allocation in a round robin manner from the underlying
  // BufferPoolManagerInstances
  // 1.   From a starting index of the BPMIs, call NewPageImpl until either 1) success and return 2) looped around to
  // starting index and return nullptr
  std::lock_guard<std::mutex> lock(latch_);

  size_t current_pos = current_instance_index_;
  do {
    Page *new_page = nullptr;
    if ((new_page = buffer_pools_[current_pos]->NewPage(page_id)) != nullptr) {
      // new_page != nullptr, continue
      // std::cout << "Using pool " << current_pos << " for page " << *page_id << std::endl;
      page_bufferpool_mapping_[*page_id] = current_pos;
      current_instance_index_ = (current_instance_index_ + 1) % num_instances_;
      return new_page;
    }
    current_pos = (current_pos + 1) % num_instances_;
  } while (current_pos != current_instance_index_);

  // 2.   Bump the starting index (mod number of instances) to start search at a different BPMI each time this function
  // is called
  return nullptr;
}

bool ParallelBufferPoolManager::DeletePgImp(page_id_t page_id) {
  latch_.lock();
  page_bufferpool_mapping_.erase(page_id);
  latch_.unlock();
  return GetBufferPoolAt(page_id)->DeletePage(page_id);
}

void ParallelBufferPoolManager::FlushAllPgsImp() {
  std::lock_guard<std::mutex> lock(latch_);
  // flush all pages from all BufferPoolManagerInstances
  std::for_each(buffer_pools_.begin(), buffer_pools_.end(), [](BufferPoolManager *e) { e->FlushAllPages(); });
}

}  // namespace bustub
