#ifndef BATCHED_OPERATOR_H
#define BATCHED_OPERATOR_H

#include "cs165_api.h"

#define INITIAL_BATCHED_OPERATOR_SIZE 32
#define INITIAL_GROUPED_BATCHED_OPERATOR_SIZE 4

GroupedBatchedOperator* initialize_grouped_batched_operator();
int add_to_grouped_batched_operator(GroupedBatchedOperator* grouped_batched_operator, DbOperator* dbo);
int free_grouped_batched_operator(GroupedBatchedOperator* grouped_batched_operator);

BatchedOperator* initialize_batched_operator();
int free_batched_operator(BatchedOperator* batched_operator);
int add_to_batched_operator(BatchedOperator* batched_operator, DbOperator* dbo);

#endif
