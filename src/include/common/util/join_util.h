#pragma once
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/expressions/abstract_expression.h"
#include "execution/expressions/column_value_expression.h"
#include "storage/table/tuple.h"

namespace bustub {
class JoinUtil {
 public:
  static bool FindWithMatchingValue(const Schema *schema, const std::string &required_col_name,
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

  static void ConcatTuplesByOutputSchema(const Schema *out_schema, const Schema *left_schema,
                                         const Schema *right_schema, const Tuple *tuple_left, const Tuple *tuple_right,
                                         std::vector<bustub::Value> *vals_out_tuple) {
    for (uint32_t idx_out_col = 0; idx_out_col < out_schema->GetColumnCount(); idx_out_col++) {
      auto const &col_out = out_schema->GetColumn(idx_out_col);
      auto col_val_expr =
          reinterpret_cast<ColumnValueExpression *>(const_cast<AbstractExpression *>(col_out.GetExpr()));
      auto tuple_idx = col_val_expr->GetTupleIdx();
      auto col_idx = col_val_expr->GetColIdx();
      assert(tuple_idx <= 1);
      if (tuple_idx == 0) {
        // left table
        vals_out_tuple->emplace_back(tuple_left->GetValue(left_schema, col_idx));
      } else {
        // right table
        vals_out_tuple->emplace_back(tuple_right->GetValue(right_schema, col_idx));
      }
    }
  }
};
}  // namespace bustub