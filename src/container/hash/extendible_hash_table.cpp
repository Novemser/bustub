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

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  HASH_TABLE_BUCKET_TYPE* bucket_page = FetchBucketPage(KeyToPageId(key, directory_page_));
  auto ret = bucket_page->GetValue(key, comparator_, result);
  table_latch_.RUnlock();
  return ret;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HASH_TABLE_BUCKET_TYPE* bucket_page = FetchBucketPage(KeyToPageId(key, directory_page_));
  table_latch_.RUnlock();

  table_latch_.WLock();
  if (bucket_page->IsFull()) {
    return SplitInsert(transaction, key, value);
  }
  
  auto res = bucket_page->Insert(key, value, comparator_);
  table_latch_.WUnlock();
  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.RLock();
  HASH_TABLE_BUCKET_TYPE* bucket_page = FetchBucketPage(KeyToPageId(key, directory_page_));
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
