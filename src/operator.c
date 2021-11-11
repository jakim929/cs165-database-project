#define _DEFAULT_SOURCE
#include <stdio.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <limits.h>

#include "cs165_api.h"
#include "utils.h"
#include "client_context.h"
#include "batched_operator.h"

char* batch_execute(ClientContext* client_context, BatchedOperator* batched_operator);
char* execute_load_operator(LoadOperator* load_operator);
char* execute_print_operator(PrintOperator* print_operator);
Result* execute_select_operator(SelectOperator* select_operator, Status* select_status);
Result* execute_sum_operator(SumOperator* sum_operator);
Result* execute_average_operator(AverageOperator* average_operator);
Result* execute_min_operator(MinOperator* min_operator);
Result* execute_max_operator(MaxOperator* max_operator);
Result* execute_add_operator(AddOperator* add_operator);
Result* execute_sub_operator(SubOperator* sub_operator);

/** execute_DbOperator takes as input the DbOperator and executes the query.
 * This should be replaced in your implementation (and its implementation possibly moved to a different file).
 * It is currently here so that you can verify that your server and client can send messages.
 * 
 * Getting started hints: 
 *      What are the structural attributes of a `query`?
 *      How will you interpret different queries?
 *      How will you ensure different queries invoke different execution paths in your code?
 **/

char* execute_db_operator_while_batching(DbOperator* query) {
    if (query->type == BATCH_EXECUTE) {
        return batch_execute(query->context, query->context->batched_operator);
    } else {
        if (strlen(query->handle) != 0) {
            add_placeholder_gcolumn_to_client_context(query->context, query->handle);
        }
        add_to_batched_operator(query->context->batched_operator, query);
    }
    return "";
}

// TODO: check if db_operator is freed in every case
char* execute_db_operator(DbOperator* query) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak. 
    
    if(!query)
    {
        return "";
    }

    if (query->type == BATCH_QUERIES) {
        start_batch_query(query->context);
        return "";
    } else if (query->type == CREATE){
        if (query->operator_fields.create_operator.create_type == _DB){
            if (create_db(query->operator_fields.create_operator.name).code == OK) {
                return "";
            } else {
                return "";
            }
        } else if (query->operator_fields.create_operator.create_type == _TABLE) {
            Status create_status;
            create_table(query->operator_fields.create_operator.db, 
                query->operator_fields.create_operator.name, 
                query->operator_fields.create_operator.col_count, 
                &create_status);
            if (create_status.code != OK) {
                return "";
            }
            return "";
        } else if (query->operator_fields.create_operator.create_type == _COLUMN) {
            Status create_status;
            create_column(query->operator_fields.create_operator.table, 
                query->operator_fields.create_operator.name, 
                false,
                &create_status);
            if (create_status.code != OK) {
                return "";
            }
            return "";
        }
    } else if (query->type == INSERT) {
        Status insert_status;
        insert_row(query->operator_fields.insert_operator.table, query->operator_fields.insert_operator.values, &insert_status);
        if (insert_status.code == OK) {
            return "";
        }
    } else if (query->type == SELECT) {
        Status select_status;
        Result* select_result = execute_select_operator(
            &(query->operator_fields.select_operator),
            &select_status
        );
        if (strlen(query->handle) != 0) {
            int rflag = add_result_to_client_context(query->context, select_result, query->handle);
            if (rflag < 0) {
                select_status.code = ERROR;
                return "";
            }
        }
        if (select_status.code == OK) {
            return "";
        }
    } else if (query->type == FETCH) {
        Status fetch_status;
        Result* fetch_result = execute_fetch_operator(
            query->operator_fields.fetch_operator.column,
            query->operator_fields.fetch_operator.posn_vec,
            &fetch_status
        );
        if (strlen(query->handle) != 0) {
            int rflag = add_result_to_client_context(query->context, fetch_result, query->handle);
            if (rflag < 0) {
                fetch_status.code = ERROR;
                return "";
            }
        }
    } else if (query->type == AVERAGE) {
        Result* average_result = execute_average_operator(&(query->operator_fields.average_operator));
        if (strlen(query->handle) != 0) {
            int rflag = add_result_to_client_context(query->context, average_result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query->type == SUM) {
        Result* sum_result = execute_sum_operator(&(query->operator_fields.sum_operator));
        if (strlen(query->handle) != 0) {
            int rflag = add_result_to_client_context(query->context, sum_result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query->type == MIN) {
        Result* min_result = execute_min_operator(&(query->operator_fields.min_operator));
        if (strlen(query->handle) != 0) {
            int rflag = add_result_to_client_context(query->context, min_result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query->type == MAX) {
        Result* max_result = execute_max_operator(&(query->operator_fields.max_operator));
        if (strlen(query->handle) != 0) {
            int rflag = add_result_to_client_context(query->context, max_result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query->type == ADD) {
        Result* result = execute_add_operator(&(query->operator_fields.add_operator));
        if (strlen(query->handle) != 0) {
            int rflag = add_result_to_client_context(query->context, result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query->type == SUB) {
        Result* result = execute_sub_operator(&(query->operator_fields.sub_operator));
        if (strlen(query->handle) != 0) {
            int rflag = add_result_to_client_context(query->context, result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query->type == LOAD) {
        char* load_result = execute_load_operator(&(query->operator_fields.load_operator));
        return load_result;
    } else if (query->type == PRINT) {
        char* print_result = execute_print_operator(&(query->operator_fields.print_operator));
        return print_result;
    } else if(query->type == SHUTDOWN) {
        Status shutdown_status = shutdown_server();
        if (shutdown_status.code != OK) {
            return "";
        }
        return "";
    }
    return "";
}

Result* execute_fetch_operator(Column* val_vec, GeneralizedColumn* posn_vec_gcolumn, Status* ret_status) {
    Result* posn_vec = posn_vec_gcolumn->column_pointer.result;
	Result* result = (Result*) malloc(sizeof(Result));
	int* res_vec = (int*) malloc(sizeof(int) * (posn_vec->num_tuples));
	int* data_payload = (int*) posn_vec->payload;
	for (size_t i = 0; i < posn_vec->num_tuples; i++) {
		res_vec[i] = val_vec->data[data_payload[i]];
	}

	result->num_tuples = posn_vec->num_tuples;
	result->data_type = INT;
	result->payload = res_vec;
	
	ret_status->code = OK;
	return result;
}

void get_payload_from_gcolumn(GeneralizedColumn* gcolumn, int** payload, size_t* size) {
    if (gcolumn->column_type == RESULT) {
        *payload = (int*) gcolumn->column_pointer.result->payload;
        *size = gcolumn->column_pointer.result->num_tuples;
    } else {
        *payload = gcolumn->column_pointer.column->data;
        *size = gcolumn->column_pointer.column->size;
    }
}

Result* select_from_column(GeneralizedColumn* gcolumn, NullableInt* range_start, NullableInt* range_end, Status* select_status) {
	int* data;
    size_t data_size;
    get_payload_from_gcolumn(gcolumn, &data, &data_size);

    Result* result = (Result*) malloc(sizeof(Result));
    result->num_tuples = 0;

	int* posn_vec = (int*) malloc(sizeof(int) * data_size);
	for (size_t i = 0; i < data_size; i++) {
		// TODO: Try splitting out this if statement?
		if ((range_start->is_null || data[i] >= range_start->value) && (range_end->is_null || data[i] < range_end->value)) {
            posn_vec[result->num_tuples++] = i;
		}
	}

	result->data_type = INT;
	result->payload = posn_vec;

	select_status->code = OK;
	return result;
}

Result* select_positions_from_column(Result* posn_vec, GeneralizedColumn* gcolumn, NullableInt* range_start, NullableInt* range_end, Status* select_status) {
	int* data;
    size_t data_size;
    get_payload_from_gcolumn(gcolumn, &data, &data_size);

    Result* result = (Result*) malloc(sizeof(Result));
    result->num_tuples = 0;
    int* posn_vec_val = (int*) posn_vec->payload;
	int* result_posn_vec = (int*) malloc(sizeof(int) * posn_vec->num_tuples);
	for (size_t i = 0; i < data_size; i++) {
		// TODO: Try splitting out this if statement?
		if ((range_start->is_null || data[i] >= range_start->value) && (range_end->is_null || data[i] < range_end->value)) {
            result_posn_vec[result->num_tuples++] = posn_vec_val[i];
		}
	}
	result->data_type = INT;
	result->payload = result_posn_vec;

	select_status->code = OK;
	return result;
}

Result* execute_select_operator(SelectOperator* select_operator, Status* select_status) {
    Result* res;
    if (select_operator->posn_vec == NULL) {
        res = select_from_column(
            select_operator->gcolumn,
            &(select_operator->range_start),
            &(select_operator->range_end),
            select_status
        );
    } else {
        res = select_positions_from_column(
            select_operator->posn_vec,
            select_operator->gcolumn,
            &(select_operator->range_start),
            &(select_operator->range_end),
            select_status
        );
    }
    return res;
}

Result* execute_average_operator(AverageOperator* average_operator) {
    size_t size = average_operator->val_vec->num_tuples;
    int* data = (int*) average_operator->val_vec->payload;
    long sum = 0;

    clock_t start, end;
    double cpu_time_used;
    start = clock();
    for (size_t i = 0; i < size; i++) {
        sum += (long) data[i];
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("summing took %fms\n", cpu_time_used);

    Result* result = (Result*) malloc(sizeof(Result));
    result->data_type = FLOAT;
    result->num_tuples = 1;
    result->payload = malloc(sizeof(float));
    ((float*) result->payload)[0] = (float) ((double) sum / ((double) size));
    return result;
}

Result* execute_sum_operator(SumOperator* sum_operator) {
    size_t size;
    int* data;
    if (sum_operator->val_vec->column_type == RESULT) {
        Result* val_vec_result = sum_operator->val_vec->column_pointer.result;
        size = val_vec_result->num_tuples;
        data = (int*) val_vec_result->payload;
    } else {
        // sum_operator->val_vec->column_type == COLUMN
        Column* val_vec_col = sum_operator->val_vec->column_pointer.column;
        size = val_vec_col->size;
        data = (int*) val_vec_col->data;
    }

    long sum = 0;
    for (size_t i = 0; i < size; i++) {
        sum += (long) data[i];
    }
    Result* result = (Result*) malloc(sizeof(Result));
    result->data_type = LONG;
    result->num_tuples = 1;
    result->payload = malloc(sizeof(long));
    ((long*) result->payload)[0] = sum;
    return result;
}

Result* execute_max_operator(MaxOperator* max_operator) {
    size_t size = max_operator->val_vec->num_tuples;
    int* data = (int*) max_operator->val_vec->payload;
    int max = INT_MIN;
    for (size_t i = 0; i < size; i++) {
        max = data[i] > max ? data[i] : max;
    }
    Result* result = (Result*) malloc(sizeof(Result));
    result->data_type = INT;
    result->num_tuples = 1;
    result->payload = malloc(sizeof(int));
    ((int*) result->payload)[0] = max;
    return result;
}

Result* execute_min_operator(MinOperator* min_operator) {
    size_t size = min_operator->val_vec->num_tuples;
    int* data = (int*) min_operator->val_vec->payload;
    int min = INT_MAX;
    for (size_t i = 0; i < size; i++) {
        min = data[i] < min ? data[i] : min;
    }
    Result* result = (Result*) malloc(sizeof(Result));
    result->data_type = INT;
    result->num_tuples = 1;
    result->payload = malloc(sizeof(int));
    ((int*) result->payload)[0] = min;
    return result;
}

Result* execute_add_operator(AddOperator* add_operator) {
    // Assumes both vectors are same size
    size_t size = add_operator->val_vec1->num_tuples;
    int* data1 = (int*) add_operator->val_vec1->payload;
    int* data2 = (int*) add_operator->val_vec2->payload;

    int* add_result = (int*) malloc(sizeof(int) * size);

    for (size_t i = 0; i < size; i++) {
        add_result[i] = data1[i] + data2[i];
    }
    
    Result* result = (Result*) malloc(sizeof(Result));
    result->data_type = INT;
    result->num_tuples = size;
    result->payload = (void*) add_result;
    return result;
}

Result* execute_sub_operator(SubOperator* sub_operator) {
    // Assumes both vectors are same size
    size_t size = sub_operator->val_vec1->num_tuples;
    int* data1 = (int*) sub_operator->val_vec1->payload;
    int* data2 = (int*) sub_operator->val_vec2->payload;

    int* sub_result = (int*) malloc(sizeof(int) * size);

    for (size_t i = 0; i < size; i++) {
        sub_result[i] = data1[i] - data2[i];
    }
    
    Result* result = (Result*) malloc(sizeof(Result));
    result->data_type = INT;
    result->num_tuples = size;
    result->payload = (void*) sub_result;
    return result;
}

char* execute_load_operator(LoadOperator* load_operator) {
    Status insert_status;
    int* row_buffer = (int*) malloc(load_operator->table->col_count * sizeof(int));
    char* line = NULL;
    while ((line = strsep(&(load_operator->load_data), "\n")) != NULL) {
        size_t col_inserted = 0;
        char* value = NULL;
        while ((value = strsep(&line, ",")) != NULL) {
            char* temp;
            row_buffer[col_inserted++] = (int) strtol(value, &temp, 0);
            if (col_inserted == load_operator->table->col_count) {
                col_inserted = 0;
                insert_row(load_operator->table, row_buffer, &insert_status);
            }
        }        
    }
    free(row_buffer);
    return "";
}

size_t get_gcolumn_size(GeneralizedColumn* gcolumn) {
    if (gcolumn->column_type == RESULT) {
        return gcolumn->column_pointer.result->num_tuples;
    } else if (gcolumn->column_type == COLUMN) {
        return gcolumn->column_pointer.column->size;
    }
    return 0;
}

int print_gcolumn_data(char* dest, GeneralizedColumn* gcolumn, size_t index) {
    if (gcolumn->column_type == RESULT) {
        if (gcolumn->column_pointer.result->data_type == INT) {
            int val = ((int*) gcolumn->column_pointer.result->payload)[index];
            return sprintf(dest, "%d", val);
        } else if (gcolumn->column_pointer.result->data_type == FLOAT) {
            float val = ((float*) gcolumn->column_pointer.result->payload)[index];
            return sprintf(dest, "%.2f", val);
        } else if (gcolumn->column_pointer.result->data_type == LONG) {
            long val = ((long*) gcolumn->column_pointer.result->payload)[index];
            return sprintf(dest, "%ld", val);
        }
    } else if (gcolumn->column_type == COLUMN) {
        int val = ((int*) gcolumn->column_pointer.result->payload)[index];
        return sprintf(dest, "%d", val);
    }
    return 0;
}

char* execute_print_operator(PrintOperator* print_operator) {
    int written_so_far = 0;
    int buffer_size = INITIAL_PRINT_OPERATOR_BUFFER_SIZE;
    int line_size = 128;
    char* buffer = (char*) malloc(buffer_size);

    // TODO: add logic to increase buffer size if necessary
    // Prob requires set length for each row line

    size_t row_count = get_gcolumn_size(print_operator->generalized_columns[0]);
    for (size_t i = 0; i < row_count; i++) {
        if (buffer_size  - written_so_far < line_size) {
            buffer = (char*) realloc(buffer, buffer_size*=2);
        }
        for (int j = 0; j < print_operator->generalized_columns_count; j++) {
            int newly_written = print_gcolumn_data(buffer + written_so_far, print_operator->generalized_columns[j], i);
            written_so_far += newly_written;
            buffer[written_so_far++] = j == print_operator->generalized_columns_count - 1 ? '\n' : ',';
        }
    }
    buffer[written_so_far - 1] = '\0';
    return buffer;
}

char* execute_batched_select_operator(ClientContext* client_context, BatchedOperator* batched_operator) {
    Column* column = batched_operator->dbos[0]->operator_fields.select_operator.gcolumn->column_pointer.column;
    Result** results = (Result**) malloc(sizeof(Result*) * batched_operator->size);
    for (int j = 0; j < batched_operator->size; j++) {
        results[j] = (Result*) malloc(sizeof(Result));
        results[j]->data_type = INT;
        results[j]->num_tuples = 0;
        // TODO: add dynamic resizing
        results[j]->payload = malloc(sizeof(int) * column->size);
    }
    for (size_t i = 0; i < column->size; i++) {
        for (int j = 0; j < batched_operator->size; j++) {
            SelectOperator* select_operator = &(batched_operator->dbos[j]->operator_fields.select_operator);
            if (
                (
                    select_operator->range_start.is_null ||
                    column->data[i] >= select_operator->range_start.value
                ) && (
                    select_operator->range_end.is_null ||
                    column->data[i] < select_operator->range_end.value
                )
            ) {
                ((int*) results[j]->payload)[results[j]->num_tuples++] = i;
            }
        }
    }

    for (int j = 0; j < batched_operator->size; j++) {
        add_result_to_client_context(client_context, results[j], batched_operator->dbos[j]->handle);
    }
    free(results);
	return "";
}

char* batch_execute(ClientContext* client_context, BatchedOperator* batched_operator) {
    GroupedBatchedOperator* grouped_batched_operator = initialize_grouped_batched_operator();

    for (int i = 0; i < batched_operator->size; i++) {
        add_to_grouped_batched_operator(grouped_batched_operator, batched_operator->dbos[i]);
	}

    for (int i = 0; i < grouped_batched_operator->size; i++) {
        if (grouped_batched_operator->batches[i]->dbos[0]->type == SELECT) {
            execute_batched_select_operator(client_context, grouped_batched_operator->batches[i]);
        } else {
            for (int j = 0; j < grouped_batched_operator->batches[i]->size; j++) {
                execute_db_operator(grouped_batched_operator->batches[i]->dbos[j]);
            }
        }
    }

    free_grouped_batched_operator(grouped_batched_operator);
    return "";
}

int free_db_operator(DbOperator* dbo) {
    if (dbo->type == INSERT) {
        free(dbo->operator_fields.insert_operator.values);
    } else if (dbo->type == PRINT) {
        free(dbo->operator_fields.print_operator.generalized_columns);
    }
    free(dbo);
    return 0;
}
