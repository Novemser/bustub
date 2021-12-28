//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      child_(std::move(child)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.Begin()) {}

void AggregationExecutor::Init() {
  child_->Init();
  is_finished_ = false;
  Tuple tuple_next;
  RID rid_next;
  auto const &group_bys = plan_->GetGroupBys();
  auto const &aggregates = plan_->GetAggregates();

  while (child_->Next(&tuple_next, &rid_next)) {
    std::vector<Value> group_by_vals;
    std::vector<Value> aggregate_vals;
    for (auto const group_by : group_bys) {
      group_by_vals.emplace_back(group_by->Evaluate(&tuple_next, child_->GetOutputSchema()));
    }
    for (auto const aggregate : aggregates) {
      aggregate_vals.emplace_back(aggregate->Evaluate(&tuple_next, child_->GetOutputSchema()));
    }

    assert(!aggregate_vals.empty());
    aht_.InsertCombine({group_by_vals}, {aggregate_vals});
  }

  aht_iterator_ = aht_.Begin();
}

bool AggregationExecutor::Next(Tuple *tuple, RID *rid) {
  if (is_finished_) {
    return true;
  }

  auto out_schema = plan_->OutputSchema();
  while (aht_iterator_ != aht_.End()) {
    auto k = aht_iterator_.Key();
    auto val = aht_iterator_.Val();
    auto having_expr = plan_->GetHaving();
    if (having_expr != nullptr) {
      bool res = having_expr->EvaluateAggregate(k.group_bys_, val.aggregates_).GetAs<bool>();
      if (!res) {
        ++aht_iterator_;
        continue;
      }
    }

    // found a match
    std::vector<Value> result_vals;
    for (auto column : out_schema->GetColumns()) {
      result_vals.emplace_back(column.GetExpr()->EvaluateAggregate(k.group_bys_, val.aggregates_));
    }
    assert(!result_vals.empty());
    *tuple = Tuple(result_vals, out_schema);
    ++aht_iterator_;
    return true;
  }

  is_finished_ = true;
  return false;
}

const AbstractExecutor *AggregationExecutor::GetChildExecutor() const { return child_.get(); }

}  // namespace bustub
