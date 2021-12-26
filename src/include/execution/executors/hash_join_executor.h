//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <map>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/aggregation_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {
/** SingleValueJoinKey represents a key in an hash join operation */
struct SingleValueJoinKey {
  Value join_key_;

  /**
   * Compares two SingleValueJoinKey keys for equality.
   * @param other the other SingleValueJoinKey key to be compared with
   * @return `true` if both SingleValueJoinKey keys have equivalent expressions, `false` otherwise
   */
  bool operator==(const SingleValueJoinKey &other) const {
    return join_key_.CompareEquals(other.join_key_) == CmpBool::CmpTrue;
  }
};
}  // namespace bustub

namespace std {
/** Implements std::hash on SingleValueJoinKey */
template <>
struct hash<bustub::SingleValueJoinKey> {
  std::size_t operator()(const bustub::SingleValueJoinKey &join_key) const {
    size_t curr_hash = 0;
    if (!join_key.join_key_.IsNull()) {
      curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&join_key.join_key_));
    }
    return curr_hash;
  }
};

}  // namespace std

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  /**
   * Construct a new HashJoinExecutor instance.
   * @param exec_ctx The executor context
   * @param plan The HashJoin join plan to be executed
   * @param left_child The child executor that produces tuples for the left side of join
   * @param right_child The child executor that produces tuples for the right side of join
   */
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  /** Initialize the join */
  void Init() override;

  /**
   * Yield the next tuple from the join.
   * @param[out] tuple The next tuple produced by the join
   * @param[out] rid The next tuple RID produced by the join
   * @return `true` if a tuple was produced, `false` if there are no more tuples
   */
  bool Next(Tuple *tuple, RID *rid) override;

  /** @return The output schema for the join */
  const Schema *GetOutputSchema() override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;
  std::unique_ptr<AbstractExecutor> left_child_;
  std::unique_ptr<AbstractExecutor> right_child_;
  std::unordered_map<SingleValueJoinKey, std::vector<Tuple>> left_hash_table_;

  Tuple tuple_cursor_;
  RID rid_cursor_;
  uint32_t matching_tuple_index_;
  bool proceed_next_tuple_in_right_table_;
  bool finished_;
};

}  // namespace bustub
