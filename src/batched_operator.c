#include <string.h>

#include "batched_operator.h"

GroupedBatchedOperator* initialize_grouped_batched_operator() {
    GroupedBatchedOperator* grouped_batched_operator = (GroupedBatchedOperator*) malloc(sizeof(GroupedBatchedOperator));
	grouped_batched_operator->gcolumns = (GeneralizedColumn**) malloc(sizeof(GeneralizedColumn*) * INITIAL_GROUPED_BATCHED_OPERATOR_SIZE);
    grouped_batched_operator->batches = (BatchedOperator**) malloc(sizeof(BatchedOperator*) * INITIAL_GROUPED_BATCHED_OPERATOR_SIZE);
	grouped_batched_operator->capacity = INITIAL_GROUPED_BATCHED_OPERATOR_SIZE;
	grouped_batched_operator->size = 0;
	return grouped_batched_operator;
}

int append_to_grouped_batched_operator(GroupedBatchedOperator* grouped_batched_operator, DbOperator* dbo, GeneralizedColumn* gcolumn) {
	BatchedOperator* batched_operator = initialize_batched_operator();
    add_to_batched_operator(batched_operator, dbo);

    // TODO: add logic to resize if necessary
    grouped_batched_operator->gcolumns[grouped_batched_operator->size] = gcolumn;
	grouped_batched_operator->batches[grouped_batched_operator->size] = batched_operator;
    grouped_batched_operator->size++; 
	return 0;
}

int add_to_grouped_batched_operator(GroupedBatchedOperator* grouped_batched_operator, DbOperator* dbo) {
    if (dbo->type != SELECT) {
		// Not a select operator, don't batch
		append_to_grouped_batched_operator(grouped_batched_operator, dbo, NULL);
		return 0;
	}
	GeneralizedColumn* gcolumn = dbo->operator_fields.select_operator.gcolumn;
	for (int i = 0; i < grouped_batched_operator->size; i++) {
		if (gcolumn == grouped_batched_operator->gcolumns[i]) {
            return add_to_batched_operator(grouped_batched_operator->batches[i], dbo);
        }
	}
	append_to_grouped_batched_operator(grouped_batched_operator, dbo, gcolumn);
    return 0;
}

int free_grouped_batched_operator(GroupedBatchedOperator* grouped_batched_operator) {
	free(grouped_batched_operator->batches);
	free(grouped_batched_operator);
    // Doesnt free underlying dbo
    for (int i = 0; i < grouped_batched_operator->size; i++) {
		free(grouped_batched_operator->batches[i]->dbos);
	    free(grouped_batched_operator->batches[i]);
	}
	return 0;
}

BatchedOperator* initialize_batched_operator() {
	BatchedOperator* batched_operator = (BatchedOperator*) malloc(sizeof(BatchedOperator));
	batched_operator->dbos = (DbOperator**) malloc(sizeof(DbOperator*) * INITIAL_BATCHED_OPERATOR_SIZE);
	batched_operator->capacity = INITIAL_BATCHED_OPERATOR_SIZE;
	batched_operator->size = 0;
	return batched_operator;
}

int free_batched_operator(BatchedOperator* batched_operator) {
	for (int i = 0; i < batched_operator->size; i++) {
		free_db_operator(batched_operator->dbos[i]);
	}
	free(batched_operator->dbos);
	free(batched_operator);
	return 0;
}

int add_to_batched_operator(BatchedOperator* batched_operator, DbOperator* dbo) {
	// TODO: add logic to resize if necessary
	batched_operator->dbos[batched_operator->size++] = dbo;
	return 0;
}