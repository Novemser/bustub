//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

bool NestedLoopJoinExecutor::FindWithMatchingValue(const Schema *schema, const std::string &required_col_name,
                                                   std::vector<bustub::Value> *output, const Tuple *source) {
  for (uint32_t idx_schema_col = 0; idx_schema_col < schema->GetColumnCount(); idx_schema_col++) {
    if (schema->GetColumn(idx_schema_col).GetName() != required_col_name) {
      continue;
    }
    // column name match, put into result
    assert(source != nullptr);
    output->emplace_back(source->GetValue(schema, idx_schema_col));
    return true;
  }

  return false;
}

bool NestedLoopJoinExecutor::ProductNext(Tuple *tuple_left, RID *rid_left, Tuple *tuple_right, RID *rid_right) {
  assert(tuple_left != nullptr && tuple_right != nullptr && rid_left != nullptr && rid_right != nullptr);

  while (!outer_loop_finished_) {
    if (inner_loop_finished_) {
      bool has_next_outer = left_executor_->Next(tuple_left, rid_left);
      if (!has_next_outer) {
        outer_loop_finished_ = true;
        break;
      }
      // ref the left tuple and init the inner loop for processing
      outer_tuple_cursor_ = *tuple_left;
      outer_tuple_rid_ = *rid_left;
      right_executor_->Init();
      inner_loop_finished_ = false;
    }

    // inner loop
    while (!inner_loop_finished_) {
      bool has_next_inner = right_executor_->Next(tuple_right, rid_right);
      if (!has_next_inner) {
        inner_loop_finished_ = true;
        break;
      }
      // successfully read a pair of tuple
      // incase the outer loop tuple did not proceed to read the next tuple, assign them with previously used one
      *tuple_left = outer_tuple_cursor_;
      *rid_left = outer_tuple_rid_;
      return true;
    }
  }

  return false;
}

void NestedLoopJoinExecutor::Init() {
  assert(left_executor_ != nullptr && right_executor_ != nullptr);
  // init the outer table
  left_executor_->Init();
  outer_loop_finished_ = false;
  // inner loop init is defered until runtime
  inner_loop_finished_ = true;
  finished_ = false;
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (finished_) {
    return false;
  }

  Tuple tuple_left;
  Tuple tuple_right;
  RID rid_left;
  RID rid_right;
  auto left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto right_schema = plan_->GetRightPlan()->OutputSchema();
  auto out_schema = plan_->OutputSchema();
  assert(out_schema != nullptr && out_schema->GetColumnCount() > 0);

  while (ProductNext(&tuple_left, &rid_left, &tuple_right, &rid_right)) {
    if (!plan_->Predicate()->EvaluateJoin(&tuple_left, left_schema, &tuple_right, right_schema).GetAs<bool>()) {
      continue;
    }
    // concat each output column in the final result set.
    std::vector<bustub::Value> vals_out_tuple;
    for (uint32_t idx_out_col = 0; idx_out_col < out_schema->GetColumnCount(); idx_out_col++) {
      auto const& col_out = out_schema->GetColumn(idx_out_col);
      auto const& col_out_name = col_out.GetName();
      // find matching column in left side
      if (FindWithMatchingValue(left_schema, col_out_name, &vals_out_tuple, &tuple_left) ||
          FindWithMatchingValue(right_schema, col_out_name, &vals_out_tuple, &tuple_right)) {
        continue;
      }
      throw Exception(ExceptionType::INVALID, "Unexpected schema mismatching");
    }

    *tuple = Tuple(vals_out_tuple, out_schema);
    // TODO(novemser): what to do with rid?
    return true;
  }

  finished_ = true;
  return false;
}

}  // namespace bustub
