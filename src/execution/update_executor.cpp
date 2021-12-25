//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}
void UpdateExecutor::UpdateIndex(const Tuple &tp_old, const Tuple &tp_new, const RID &rid) {
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(exec_ctx_->GetCatalog()->GetTable(plan_->TableOid())->name_);

  for (auto idx : indexes) {
    idx->index_->DeleteEntry(tp_old, rid, exec_ctx_->GetTransaction());
    idx->index_->InsertEntry(tp_new, rid, exec_ctx_->GetTransaction());
  }
}

void UpdateExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  finished_ = false;
}

bool UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (finished_) {
    return false;
  }

  assert(child_executor_ != nullptr);
  Tuple tmp_tuple;
  RID tmp_rid;
  child_executor_->Init();

  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    Tuple update_tp(GenerateUpdatedTuple(tmp_tuple));
    bool res = table_info_->table_->UpdateTuple(update_tp, tmp_rid, exec_ctx_->GetTransaction());
    if (!res) {
      throw "Failed to update a tuple";
    }
    UpdateIndex(tmp_tuple, update_tp, tmp_rid);
  }
  finished_ = true;

  return false;
}

Tuple UpdateExecutor::GenerateUpdatedTuple(const Tuple &src_tuple) {
  const auto &update_attrs = plan_->GetUpdateAttr();
  Schema schema = table_info_->schema_;
  uint32_t col_count = schema.GetColumnCount();
  std::vector<Value> values;
  for (uint32_t idx = 0; idx < col_count; idx++) {
    if (update_attrs.find(idx) == update_attrs.cend()) {
      values.emplace_back(src_tuple.GetValue(&schema, idx));
    } else {
      const UpdateInfo info = update_attrs.at(idx);
      Value val = src_tuple.GetValue(&schema, idx);
      switch (info.type_) {
        case UpdateType::Add:
          values.emplace_back(val.Add(ValueFactory::GetIntegerValue(info.update_val_)));
          break;
        case UpdateType::Set:
          values.emplace_back(ValueFactory::GetIntegerValue(info.update_val_));
          break;
      }
    }
  }
  return Tuple{values, &schema};
}

}  // namespace bustub
