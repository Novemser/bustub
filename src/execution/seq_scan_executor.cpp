//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan) {}

void SeqScanExecutor::Init() {
  auto tb_oid = plan_->GetTableOid();
  auto table_info = exec_ctx_->GetCatalog()->GetTable(tb_oid);
  table_info_ = table_info;
  iter_ = table_info->table_->Begin(exec_ctx_->GetTransaction());
  end_iter_ = table_info->table_->End();
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  while (iter_ != end_iter_) {
    const Tuple tp = *iter_++;
    if (plan_->GetPredicate() != nullptr) {
      if (!plan_->GetPredicate()->Evaluate(&tp, &table_info_->schema_).GetAs<bool>()) {
        continue;
      }
    }
    *rid = tp.GetRid();
    *tuple = tp;
    return true;
  }

  return false;
}

}  // namespace bustub
