//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_bucket_page.cpp
//
// Identification: src/storage/page/hash_table_bucket_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_bucket_page.h"
#include "common/logger.h"
#include "common/util/hash_util.h"
#include "storage/index/generic_key.h"
#include "storage/index/hash_comparator.h"
#include "storage/table/tmp_tuple.h"

#define OffsetAt(x) (array_ + sizeof(MappingType) * (x))

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::GetValue(KeyType key, KeyComparator cmp, std::vector<ValueType> *result) {
  bool match = false;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    auto k_position = reinterpret_cast<KeyType *>(OffsetAt(i));
    auto v_position = reinterpret_cast<const ValueType *>(OffsetAt(i) + sizeof(KeyType));
    if (!cmp(key, *k_position)) {
      result->push_back(*v_position);
      match = true;
    }
  }

  return match;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Insert(KeyType key, ValueType value, KeyComparator cmp) {
  if (IsFull()) {
    return false;
  }

  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    auto k_position = reinterpret_cast<KeyType *>(OffsetAt(i));
    auto v_position = const_cast<ValueType *>(reinterpret_cast<const ValueType *>(OffsetAt(i) + sizeof(KeyType)));
    if (IsReadable(i)) {
      // check if the same <kv> pair
      if (!cmp(key, *k_position) && value == *v_position) {
        return false;
      }
    } else {
      *k_position = key;
      *v_position = value;
      SetOccupied(i);
      SetReadable(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::Remove(KeyType key, ValueType value, KeyComparator cmp) {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsOccupied(i) || !IsReadable(i)) {
      continue;
    }

    auto k_position = reinterpret_cast<KeyType *>(OffsetAt(i));
    if (!cmp(key, *k_position)) {
      RemoveAt(i);
      return true;
    }
  }
  return false;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
KeyType HASH_TABLE_BUCKET_TYPE::KeyAt(uint32_t bucket_idx) const {
  assert(bucket_idx < BUCKET_ARRAY_SIZE);
  auto k_position = reinterpret_cast<const KeyType *>(OffsetAt(bucket_idx));
  return *k_position;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
ValueType HASH_TABLE_BUCKET_TYPE::ValueAt(uint32_t bucket_idx) const {
  assert(bucket_idx < BUCKET_ARRAY_SIZE);
  auto v_position = reinterpret_cast<const ValueType *>(OffsetAt(bucket_idx) + sizeof(KeyType));
  return *v_position;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::RemoveAt(uint32_t bucket_idx) {
  if (!IsOccupied(bucket_idx) || !IsReadable(bucket_idx)) {
    return;
  }

  // set readable to false
  const size_t byte_position = bucket_idx / 8;
  const size_t offset = bucket_idx % 8;

  const unsigned char val = 1 << offset;
  readable_[byte_position] = readable_[byte_position] ^ val;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsOccupied(uint32_t bucket_idx) const {
  const size_t byte_position = bucket_idx / 8;
  const size_t offset = bucket_idx % 8;

  const unsigned char val = 1 << offset;
  return occupied_[byte_position] & val;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetOccupied(uint32_t bucket_idx) {
  const size_t byte_position = bucket_idx / 8;
  const size_t offset = bucket_idx % 8;

  const unsigned char val = 1 << offset;
  occupied_[byte_position] = occupied_[byte_position] | val;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsReadable(uint32_t bucket_idx) const {
  const size_t byte_position = bucket_idx / 8;
  const size_t offset = bucket_idx % 8;

  const unsigned char val = 1 << offset;
  return readable_[byte_position] & val;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::SetReadable(uint32_t bucket_idx) {
  const size_t byte_position = bucket_idx / 8;
  const size_t offset = bucket_idx % 8;

  const unsigned char val = 1 << offset;
  readable_[byte_position] = readable_[byte_position] | val;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsFull() {
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (!IsReadable(i)) {
      return false;
    }
  }

  return true;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_BUCKET_TYPE::NumReadable() {
  uint32_t res = 0;
  for (size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if (IsReadable(i)) {
      res++;
    }
  }

  return res;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_BUCKET_TYPE::IsEmpty() {
  return NumReadable() == 0;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_BUCKET_TYPE::PrintBucket() {
  uint32_t size = 0;
  uint32_t taken = 0;
  uint32_t free = 0;
  for (size_t bucket_idx = 0; bucket_idx < BUCKET_ARRAY_SIZE; bucket_idx++) {
    if (!IsOccupied(bucket_idx)) {
      break;
    }

    size++;

    if (IsReadable(bucket_idx)) {
      taken++;
    } else {
      free++;
    }
  }

  LOG_INFO("Bucket Capacity: %lu, Size: %u, Taken: %u, Free: %u", BUCKET_ARRAY_SIZE, size, taken, free);
}

// DO NOT REMOVE ANYTHING BELOW THIS LINE
template class HashTableBucketPage<int, int, IntComparator>;

template class HashTableBucketPage<GenericKey<4>, RID, GenericComparator<4>>;
template class HashTableBucketPage<GenericKey<8>, RID, GenericComparator<8>>;
template class HashTableBucketPage<GenericKey<16>, RID, GenericComparator<16>>;
template class HashTableBucketPage<GenericKey<32>, RID, GenericComparator<32>>;
template class HashTableBucketPage<GenericKey<64>, RID, GenericComparator<64>>;

// template class HashTableBucketPage<hash_t, TmpTuple, HashComparator>;

}  // namespace bustub
