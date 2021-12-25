//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  finished_ = false;
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  if (finished_) {
    return false;
  }
  Tuple tmp_tuple;
  RID tmp_rid;
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);

  while (child_executor_->Next(&tmp_tuple, &tmp_rid)) {
    auto res = table_info->table_->MarkDelete(tmp_rid, exec_ctx_->GetTransaction());

    if (!res) {
      throw "Failed to delete tuple!";
    }

    for (auto idx : indexes) {
      idx->index_->DeleteEntry(tmp_tuple, tmp_rid, exec_ctx_->GetTransaction());
    }
  }
  finished_ = true;
  return false;
}

}  // namespace bustub
