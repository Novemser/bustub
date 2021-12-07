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
  directory_page_ = dir_data;
  dir_data->Init();
  dir_data->SetPageId(dir_page->GetPageId());
  dir_data->SetBucketPageId(0, bucket_page->GetPageId());
  LinkBucketIdxToPage(0, bucket_page->GetPageId());
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
  if (directory_page_->GetBucketPageId(idx) == INVALID_PAGE_ID) {
    assert(false);
  }

  return directory_page_->GetBucketPageId(idx);
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

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, directory_page_));
  auto ret = bucket_page->GetValue(key, comparator_, result);
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, directory_page_));

  if (bucket_page->IsFull()) {
    auto res = SplitInsert(transaction, key, value);
    table_latch_.WUnlock();
    return res;
  }

  auto res = bucket_page->Insert(key, value, comparator_);
  table_latch_.WUnlock();
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  // LOG_INFO("Doing split...");
  // the bucket is full, perform bucket split
  // 1. allocate a new bucket
  page_id_t new_bucket_page_id = INVALID_PAGE_ID;
  auto new_bucket_page = buffer_pool_manager_->NewPage(&new_bucket_page_id);
  if (nullptr == new_bucket_page) {
    return false;
  }
  auto original_bucket_idx = KeyToDirectoryIndex(key, directory_page_);
  auto original_bucket_page_id = directory_page_->GetBucketPageId(original_bucket_idx);

  uint32_t new_bucket_idx = INVALID;
  // 3. If global depth <= local depth of the original bucket
  if (directory_page_->GetGlobalDepth() <= directory_page_->GetLocalDepth(original_bucket_idx)) {
    // 3.1. extend the directory page bucket array size
    directory_page_->IncrGlobalDepth();
    new_bucket_idx = directory_page_->Size() / 2 + original_bucket_idx;

    // 3.2. link all the buckets
    // 3.2.1. point new splitted bucket
    directory_page_->SetLocalDepth(new_bucket_idx, directory_page_->GetGlobalDepth());
    directory_page_->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
    LinkBucketIdxToPage(new_bucket_idx, new_bucket_page_id);
    // 3.2.2. remainig buckets alias
    for (uint32_t idx_bucket = (directory_page_->Size() / 2); idx_bucket < directory_page_->Size(); idx_bucket++) {
      auto existing_page_id = directory_page_->GetBucketPageId(idx_bucket);
      if (existing_page_id == INVALID_PAGE_ID) {
        // link to existing page, its index is the current bucket index - half the size of the bucket
        uint32_t prev_location = idx_bucket - directory_page_->Size() / 2;
        assert(prev_location >= 0);
        directory_page_->SetBucketPageId(idx_bucket, directory_page_->GetBucketPageId(prev_location));
        directory_page_->SetLocalDepth(idx_bucket, directory_page_->GetLocalDepth(prev_location));
        LinkBucketIdxToPage(idx_bucket, directory_page_->GetBucketPageId(prev_location));
      }
    }
  } else {
    // 4. If global depth > local depth of the original bucket
    // 4.1. re-link the bucket
    // new_bucket_idx = directory_page_->Size() / 2 + original_bucket_idx;
    new_bucket_idx = ChooseNextBucketToExtend(original_bucket_idx, original_bucket_page_id);
    UnlinkBucketIdxToPage(new_bucket_idx, original_bucket_page_id);

    directory_page_->SetBucketPageId(new_bucket_idx, new_bucket_page_id);
    directory_page_->SetLocalDepth(new_bucket_idx, directory_page_->GetGlobalDepth());
    LinkBucketIdxToPage(new_bucket_idx, new_bucket_page_id);
  }
  directory_page_->IncrLocalDepth(original_bucket_idx);

  // rehash the data in the original bucket to split into 2 buckets
  HASH_TABLE_BUCKET_TYPE *original_bucket_page = FetchBucketPage(original_bucket_page_id);
  HASH_TABLE_BUCKET_TYPE *new_bucket = FetchBucketPage(new_bucket_page_id);
  // LOG_INFO("Rehash data before size:%d", original_bucket_page->NumReadable());
  for (uint32_t idx_kvpair = 0; idx_kvpair < BUCKET_ARRAY_SIZE; idx_kvpair++) {
    if (!original_bucket_page->IsReadable(idx_kvpair)) {
      continue;
    }

    auto k = original_bucket_page->KeyAt(idx_kvpair);
    // move the kv out if the key is hashed to the new bucket
    if (KeyToDirectoryIndex(k, directory_page_) == new_bucket_idx) {
      auto v = original_bucket_page->ValueAt(idx_kvpair);
      // LOG_INFO("Inserting to new bucket with idx %d:%d,%d", idx_kvpair, *((int *)&k), *((int *)&v));
      auto ins_res = new_bucket->Insert(k, v, comparator_);
      assert(ins_res);
      original_bucket_page->RemoveAt(idx_kvpair);
    }
  }

  // assume should not empty after rehash
  assert(!new_bucket->IsEmpty());
  HASH_TABLE_BUCKET_TYPE *page_to_install = FetchBucketPage(KeyToPageId(key, directory_page_));
  auto res = page_to_install->Insert(key, value, comparator_);
  // after rehash, the new kv should be installed properly
  assert(res);

  return res;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HASH_TABLE_BUCKET_TYPE *bucket_page = FetchBucketPage(KeyToPageId(key, directory_page_));
  table_latch_.RUnlock();

  table_latch_.WLock();
  auto res = bucket_page->Remove(key, value, comparator_);
  table_latch_.WUnlock();

  return res;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {}

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
