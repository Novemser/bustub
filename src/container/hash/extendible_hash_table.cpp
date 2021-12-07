//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "container/hash/extendible_hash_table.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : directory_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      hash_fn_(std::move(hash_fn)) {
  //  implement me!
  // create new page for the dir page
  table_latch_.WLock();
  // init with one bucket
  auto dir_page = buffer_pool_manager_->NewPage(&directory_page_id_);
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  auto bucket_page = buffer_pool_manager_->NewPage(&bucket_page_id);
  assert(INVALID_PAGE_ID != directory_page_id_);
  assert(INVALID_PAGE_ID != bucket_page_id);

  auto dir_data = reinterpret_cast<HashTableDirectoryPage *>(dir_page->GetData());
  dir_data->Init();
  dir_data->SetPageId(dir_page->GetPageId());
  dir_data->SetBucketPageId(0, bucket_page->GetPageId());

  assert(buffer_pool_manager->UnpinPage(dir_page->GetPageId(), true));
  assert(buffer_pool_manager->UnpinPage(bucket_page->GetPageId(), true));
  table_latch_.WUnlock();
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  uint32_t mask = dir_page->GetGlobalDepthMask();
  return Hash(key) & mask;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  auto idx = KeyToDirectoryIndex(key, dir_page);
  auto directory_page = FetchDirectoryPage();
  auto bucket_page_id = directory_page->GetBucketPageId(idx);
  if (bucket_page_id == INVALID_PAGE_ID) {
    assert(false);
  }

  assert(buffer_pool_manager_->UnpinPage(directory_page->GetPageId(), false));
  return bucket_page_id;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  auto page = buffer_pool_manager_->FetchPage(directory_page_id_);
  assert(page);
  return reinterpret_cast<HashTableDirectoryPage *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  auto page = buffer_pool_manager_->FetchPage(bucket_page_id);
  if (nullptr == page) {
    return nullptr;
  }

  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(page->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::LinkBucketIdxToPage(uint32_t bucket_idx, page_id_t page_id) {
  if (bucket_page_pointed_by_.count(page_id) == 0) {
    std::set<uint32_t> s;
    bucket_page_pointed_by_[page_id] = s;
  }

  bucket_page_pointed_by_[page_id].emplace(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::UnlinkBucketIdxToPage(uint32_t bucket_idx, page_id_t page_id) {
  if (bucket_page_pointed_by_.count(page_id) == 0) {
    std::set<uint32_t> s;
    bucket_page_pointed_by_[page_id] = s;
  }

  bucket_page_pointed_by_[page_id].erase(bucket_idx);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::ChooseNextBucketToExtend(uint32_t incoming_bucket_idx, page_id_t page_id) {
  assert(bucket_page_pointed_by_.count(page_id) > 0);
  const std::set<uint32_t> &s = bucket_page_pointed_by_[page_id];
  assert(s.size() > 1);
  for (auto iter_bucket_id_set = s.begin(); iter_bucket_id_set != s.end();
       iter_bucket_id_set = std::next(iter_bucket_id_set)) {
    if (*iter_bucket_id_set != incoming_bucket_idx) {
      return *iter_bucket_id_set;
    }
  }
  assert(false);
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::ChooseNextBucketToExtend(HASH_TABLE_BUCKET_TYPE *original_bucket_page, uint32_t local_depth,
                                               std::set<uint32_t> *partion_one, std::set<uint32_t> *partion_two) {
  // only works when local_depth < global_depth_
  auto directory_page = FetchDirectoryPage();
  assert(local_depth < directory_page->GetGlobalDepth());
  assert(!original_bucket_page->IsEmpty());

  const KeyType key_pivot = original_bucket_page->PeekKey();
  const uint32_t lower_bits =
      Hash(key_pivot) & (local_depth == 0 ? 0 : (0xffffffff >> (sizeof(uint32_t) * 8 - local_depth)));
  const uint32_t part_one_lower_bits = lower_bits | (0 << local_depth);
  const uint32_t part_two_lower_bits = lower_bits | (1 << local_depth);
  const uint32_t mask = 0xffffffff >> (sizeof(uint32_t) * 8 - local_depth - 1);
  for (uint32_t bucket_idx = 0; bucket_idx < directory_page->Size(); bucket_idx++) {
    if (part_one_lower_bits == (mask & bucket_idx)) {
      partion_one->insert(bucket_idx);
    } else if (part_two_lower_bits == (mask & bucket_idx)) {
      partion_two->insert(bucket_idx);
    }
  }
  assert(!partion_one->empty());
  assert(!partion_two->empty());
  assert(buffer_pool_manager_->UnpinPage(directory_page->GetPageId(), false));
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dir_page = FetchDirectoryPage();
  auto page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);
  auto ret = bucket_page->GetValue(key, comparator_, result);
  assert(buffer_pool_manager_->UnpinPage(page_id, false));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);

  if (bucket_page->IsFull()) {
    assert(buffer_pool_manager_->UnpinPage(page_id, false));
    assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
    auto res = SplitInsert(transaction, key, value);
    table_latch_.WUnlock();
    return res;
  }

  auto res = bucket_page->Insert(key, value, comparator_);
  assert(buffer_pool_manager_->UnpinPage(page_id, true));
  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), false));
  table_latch_.WUnlock();
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // LOG_INFO("Doing split...");
  // the bucket is full, perform bucket split
  // 1. allocate a new bucket
  auto directory_page = FetchDirectoryPage();
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  if (nullptr == new_bucket_page) {
    assert(buffer_pool_manager_->UnpinPage(directory_page->GetPageId(), true));
    return false;
  }
  const auto original_bucket_idx = KeyToDirectoryIndex(key, directory_page);
  const auto original_bucket_page_id = directory_page->GetBucketPageId(original_bucket_idx);
  HASH_TABLE_BUCKET_TYPE *original_bucket_page = FetchBucketPage(original_bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *new_bucket = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(new_bucket_page->GetData());
  auto original_bucket_local_depth = directory_page->GetLocalDepth(original_bucket_idx);

  uint32_t new_bucket_idx = INVALID;
  assert(original_bucket_local_depth <= directory_page->GetGlobalDepth());

  // used for partition the old bucket into two new bucket
  std::set<uint32_t> part_one;
  std::set<uint32_t> part_two;

  // 3. If global depth <= local depth of the original bucket
  if (directory_page->GetGlobalDepth() == original_bucket_local_depth) {
    // 3.1. extend the directory page bucket array size
    directory_page->IncrGlobalDepth();
    new_bucket_idx = directory_page->Size() / 2 + original_bucket_idx;

    part_one.insert(original_bucket_idx);
    part_two.insert(new_bucket_idx);

    // 3.2. link all the buckets
    // 3.2.1. point new splitted bucket
    directory_page->IncrLocalDepth(original_bucket_idx);
    directory_page->SetLocalDepth(new_bucket_idx, directory_page->GetGlobalDepth());

    directory_page->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
    // 3.2.2. remainig buckets alias
    for (uint32_t idx_bucket = (directory_page->Size() / 2); idx_bucket < directory_page->Size(); idx_bucket++) {
      auto existing_page_id = directory_page->GetBucketPageId(idx_bucket);
      if (existing_page_id == INVALID_PAGE_ID) {
        // link to existing page, its index is the current bucket index - half the size of the bucket
        uint32_t prev_location = idx_bucket - directory_page->Size() / 2;
        assert(prev_location >= 0);
        directory_page->SetBucketPageId(idx_bucket, directory_page->GetBucketPageId(prev_location));
        directory_page->SetLocalDepth(idx_bucket, directory_page->GetLocalDepth(prev_location));
      }
    }
  } else {
    // 4. If global depth > local depth of the original bucket
    // 4.1. re-link the bucket
    ChooseNextBucketToExtend(original_bucket_page, original_bucket_local_depth, &part_one, &part_two);
    directory_page->IncrLocalDepth(original_bucket_idx);

    std::for_each(part_one.begin(), part_one.end(), [=](uint32_t bucket_idx) {
      directory_page->SetBucketPageId(bucket_idx, original_bucket_page_id);
      directory_page->SetLocalDepth(bucket_idx, directory_page->GetLocalDepth(original_bucket_idx));
    });

    std::for_each(part_two.begin(), part_two.end(), [=](uint32_t bucket_idx) {
      directory_page->SetBucketPageId(bucket_idx, new_bucket_page_id);
      // new bucket should have the same depth as the original one after extending
      directory_page->SetLocalDepth(bucket_idx, directory_page->GetLocalDepth(original_bucket_idx));
    });
  }

  // rehash the data in the original bucket to split into 2 buckets
  // LOG_INFO("Rehash data before size:%d", original_bucket_page->NumReadable());
  assert(!part_two.empty());
  for (uint32_t idx_kvpair = 0; idx_kvpair < BUCKET_ARRAY_SIZE; idx_kvpair++) {
    if (!original_bucket_page->IsReadable(idx_kvpair)) {
      continue;
    }

    auto k = original_bucket_page->KeyAt(idx_kvpair);
    // move the kv out if the key is hashed to the new bucket
    if (part_two.count(KeyToDirectoryIndex(k, directory_page)) > 0) {
      auto v = original_bucket_page->ValueAt(idx_kvpair);
      // LOG_INFO("Inserting to new bucket with idx %d:%d,%d", idx_kvpair, *((int *)&k), *((int *)&v));
      auto ins_res = new_bucket->Insert(k, v, comparator_);
      assert(ins_res);
      original_bucket_page->RemoveAt(idx_kvpair);
    }
  }

  // assume should not empty after rehash
  assert(!new_bucket->IsEmpty());
  auto page_to_install_id = KeyToPageId(key, directory_page);
  HASH_TABLE_BUCKET_TYPE *page_to_install = FetchBucketPage(page_to_install_id);
  auto res = page_to_install->Insert(key, value, comparator_);
  // after rehash, the new kv should be installed properly
  assert(res);

  assert(buffer_pool_manager_->UnpinPage(original_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(new_bucket_page_id, true));
  assert(buffer_pool_manager_->UnpinPage(page_to_install_id, true));
  assert(buffer_pool_manager_->UnpinPage(directory_page->GetPageId(), true));

  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto dir_page = FetchDirectoryPage();
  auto page_id = KeyToPageId(key, dir_page);
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(page_id);

  auto res = bucket_page->Remove(key, value, comparator_);
  if (bucket_page->IsEmpty()) {
    Merge(transaction, key, value);
  }

  assert(buffer_pool_manager_->UnpinPage(dir_page->GetPageId(), true));
  assert(buffer_pool_manager_->UnpinPage(page_id, true));
  table_latch_.WUnlock();
  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto directory_page = FetchDirectoryPage();
  auto empty_bucket_idx = KeyToDirectoryIndex(key, directory_page);
  auto local_depth = directory_page->GetLocalDepth(empty_bucket_idx);
  auto empty_bucket_page_id = directory_page->GetBucketPageId(empty_bucket_idx);
  auto current_global_depth = directory_page->GetGlobalDepth();
  if (local_depth == 0) {
    assert(buffer_pool_manager_->UnpinPage(directory_page->GetPageId(), false));
    return;
  }

  // HASH_TABLE_BUCKET_TYPE *empty_bucket_page = FetchBucketPage(empty_bucket_page_id);
  uint32_t merge_image_bucket_idx = empty_bucket_idx;
  page_id_t merge_image_page_id = directory_page->GetBucketPageId(merge_image_bucket_idx);

  for (uint32_t loop_cnt = 0; merge_image_page_id == empty_bucket_page_id && (current_global_depth - loop_cnt != 0);
       loop_cnt++) {
    // two bucket pointing to the same page that cannot be merged, scan forward
    uint32_t pivot_index = std::min(merge_image_bucket_idx, empty_bucket_idx);
    merge_image_bucket_idx = pivot_index ^ (0x1 << (current_global_depth - 1 - loop_cnt));
    merge_image_page_id = directory_page->GetBucketPageId(merge_image_bucket_idx);

    if (merge_image_page_id == empty_bucket_page_id) {
      continue;
    }

    if (local_depth != directory_page->GetLocalDepth(merge_image_bucket_idx)) {
      assert(buffer_pool_manager_->UnpinPage(directory_page->GetPageId(), false));
      return;
    }
    // merge image found
  }

  if (merge_image_page_id == empty_bucket_page_id) {
    assert(buffer_pool_manager_->UnpinPage(directory_page->GetPageId(), false));
    return;
  }

  for (uint32_t bucket_idx = 0; bucket_idx < directory_page->Size(); bucket_idx++) {
    auto bucket_page_id = directory_page->GetBucketPageId(bucket_idx);
    if (bucket_page_id == empty_bucket_page_id) {
      directory_page->SetBucketPageId(bucket_idx, merge_image_page_id);
      directory_page->SetLocalDepth(bucket_idx, local_depth - 1);
    } else if (bucket_page_id == merge_image_page_id) {
      directory_page->SetLocalDepth(bucket_idx, local_depth - 1);
    }
  }

  while (directory_page->CanShrink() && current_global_depth > 0) {
    directory_page->DecrGlobalDepth();
    for (uint32_t bucket_idx = directory_page->Size(); bucket_idx < (directory_page->Size() << 1); bucket_idx++) {
      directory_page->SetBucketPageId(bucket_idx, INVALID_PAGE_ID);
      directory_page->SetLocalDepth(bucket_idx, 0);
    }
  }

  assert(buffer_pool_manager_->UnpinPage(directory_page->GetPageId(), true));
}

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::PrintDirectory() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->PrintDirectory();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
