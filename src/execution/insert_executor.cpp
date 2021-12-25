//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::InsertTupleToIndex(const Tuple &tp, const RID &rid) {
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_);

  for (auto idx : indexes) {
    idx->index_->InsertEntry(tp, rid, exec_ctx_->GetTransaction());
  }
}

void InsertExecutor::Init() {
  // 1. insert into table directly
  // 2. insert from select xxx ...
  finished_ = false;
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (finished_) {
    return false;
  }
  auto tb_oid = plan_->TableOid();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(tb_oid);
  bool is_inserted = false;

  if (plan_->IsRawInsert()) {
    auto raw_vals = plan_->RawValues();
    for (auto &raw_val : raw_vals) {
      Tuple t(raw_val, &table_info->schema_);
      auto res = table_info->table_->InsertTuple(t, rid, exec_ctx_->GetTransaction());
      if (!res) {
        return false;
      }
      InsertTupleToIndex(t, *rid);
      is_inserted = true;
    }
  } else {
    child_executor_->Init();
    Tuple tmp_tuple;
    RID tmp_rid;
    while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
      auto res = table_info->table_->InsertTuple(tmp_tuple, &tmp_rid, exec_ctx_->GetTransaction());
      if (!res) {
        return false;
      }
      InsertTupleToIndex(tmp_tuple, tmp_rid);
      is_inserted = true;
    }
  }
  finished_ = true;
  return is_inserted;
}

}  // namespace bustub
