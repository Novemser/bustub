//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/util/join_util.h"
#include "execution/expressions/abstract_expression.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_(std::move(left_child)),
      right_child_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  assert(left_child_ != nullptr && right_child_ != nullptr);
  left_child_->Init();
  auto left_join_expr = plan_->LeftJoinKeyExpression();
  // step 1. build the hash table based on left table
  // hash table:
  // Key: join key (assume the key can not be multiple attrs and the hash table is small enough to fit into memory)
  Tuple tp_left;
  RID rid_left;
  while (left_child_->Next(&tp_left, &rid_left)) {
    // for each tuple in the driver table, hash the join attr as key and store the tuple as value
    auto join_key_left = left_join_expr->Evaluate(&tp_left, left_child_->GetOutputSchema());
    left_hash_table_[{join_key_left}].emplace_back(tp_left);
  }

  proceed_next_tuple_in_right_table_ = true;
  right_child_->Init();
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (finished_) {
    return false;
  }

  Value join_key_right;
  while (proceed_next_tuple_in_right_table_) {
    bool res = right_child_->Next(&tuple_cursor_, &rid_cursor_);
    if (!res) {
      // right table has nothing more to read
      finished_ = true;
      return false;
    }
    // found one tuple, probe matching tuple in the hash table
    // step 2. probe phase
    auto right_join_expr = plan_->RightJoinKeyExpression();
    join_key_right = right_join_expr->Evaluate(&tuple_cursor_, right_child_->GetOutputSchema());
    if (left_hash_table_.count({join_key_right}) == 0) {
      // nothing found, proceed to the next tuple
      continue;
    }

    proceed_next_tuple_in_right_table_ = false;
    // start from the first tuple among the matching ones
    matching_tuple_index_ = 0;
  }

  // found one matching tuple, construct consequent tuples
  assert(left_hash_table_.count({join_key_right}) > 0);
  const auto &matching_tuples = left_hash_table_[{join_key_right}];

  assert(!matching_tuples.empty() && matching_tuple_index_ < matching_tuples.size());
  auto left_matching_tuple = matching_tuples[matching_tuple_index_++];

  if (matching_tuple_index_ >= matching_tuples.size()) {
    // next time, proceed to the next tuple
    proceed_next_tuple_in_right_table_ = true;
  }

  // concact final tuples from cursor tuple and matching tuple
  auto left_schema = plan_->GetLeftPlan()->OutputSchema();
  auto right_schema = plan_->GetRightPlan()->OutputSchema();
  auto out_schema = plan_->OutputSchema();
  assert(out_schema != nullptr && out_schema->GetColumnCount() > 0);

  std::vector<bustub::Value> vals_out_tuple;
  JoinUtil::ConcatTuplesByOutputSchema(out_schema, left_schema, right_schema, &left_matching_tuple, &tuple_cursor_,
                                       &vals_out_tuple);
  *tuple = Tuple(vals_out_tuple, out_schema);
  return true;
}

}  // namespace bustub
