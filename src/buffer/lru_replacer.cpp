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
#include <assert.h>
#include <algorithm>
#include <iostream>

#include "buffer/lru_replacer.h"


namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
    assert(num_pages > 0);
    this->max_pages = num_pages;
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
    auto lst = &this->data;
    if (lst->empty()) {
        frame_id = nullptr;
        return false;
    }

    auto last_elem = std::prev(lst->end());
    *frame_id = *last_elem;
    lst->erase(last_elem);
    return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
    auto lst = &this->data;
    auto pos = std::find(lst->begin(), lst->end(), frame_id);
    if (pos != lst->end())
    {
        // found, remove from replacer
        lst->erase(pos);
    }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    auto lst = &this->data;
    auto pos = std::find(lst->begin(), lst->end(), frame_id);
    if (pos == lst->end())
    {
        // not found, add to the first position
        lst->insert(lst->begin(), frame_id);
    }
}

size_t LRUReplacer::Size() { return this->data.size(); }

}  // namespace bustub
