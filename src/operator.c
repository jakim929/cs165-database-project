#define _DEFAULT_SOURCE
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <limits.h>

#include "cs165_api.h"
#include "utils.h"
#include "client_context.h"

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

// TODO: check if db_operator is freed in every case
char* execute_db_operator(DbOperator* query) {
    // there is a small memory leak here (when combined with other parts of your database.)
    // as practice with something like valgrind and to develop intuition on memory leaks, find and fix the memory leak. 
    
    if(!query)
    {
        return "";
    }
    if(query && query->type == CREATE){
        if(query->operator_fields.create_operator.create_type == _DB){
            if (create_db(query->operator_fields.create_operator.name).code == OK) {
                return "";
            } else {
                return "";
            }
        }
        else if(query->operator_fields.create_operator.create_type == _TABLE) {
            Status create_status;
            create_table(query->operator_fields.create_operator.db, 
                query->operator_fields.create_operator.name, 
                query->operator_fields.create_operator.col_count, 
                &create_status);
            if (create_status.code != OK) {
                return "";
            }
            return "";
        } else if(query->operator_fields.create_operator.create_type == _COLUMN) {
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
    }else if(query && query->type == INSERT) {
        Status insert_status;
        insert_row(query->operator_fields.insert_operator.table, query->operator_fields.insert_operator.values, &insert_status);
        if (insert_status.code == OK) {
            return "";
        }
    }else if(query && query->type == SELECT) {
        Status select_status;
        Result* select_result = execute_select_operator(
            &(query->operator_fields.select_operator),
            &select_status
        );
        if (query->handle != NULL) {
            int rflag = add_result_to_client_context(query->context, select_result, query->handle);
            if (rflag < 0) {
                select_status.code = ERROR;
                return "";
            }
        }
        if (select_status.code == OK) {
            return "";
        }
    } else if (query && query->type == FETCH) {
        Status fetch_status;
        Result* fetch_result = fetch(
            query->operator_fields.fetch_operator.column,
            query->operator_fields.fetch_operator.posn_vec,
            &fetch_status
        );
        if (query->handle != NULL) {
            int rflag = add_result_to_client_context(query->context, fetch_result, query->handle);
            if (rflag < 0) {
                fetch_status.code = ERROR;
                return "";
            }
        }
    } else if (query && query->type == AVERAGE) {
        Result* average_result = execute_average_operator(&(query->operator_fields.average_operator));
        if (query->handle != NULL) {
            int rflag = add_result_to_client_context(query->context, average_result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query && query->type == SUM) {
        Result* sum_result = execute_sum_operator(&(query->operator_fields.sum_operator));
        if (query->handle != NULL) {
            int rflag = add_result_to_client_context(query->context, sum_result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query && query->type == MIN) {
        Result* min_result = execute_min_operator(&(query->operator_fields.min_operator));
        if (query->handle != NULL) {
            int rflag = add_result_to_client_context(query->context, min_result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query && query->type == MAX) {
        Result* max_result = execute_max_operator(&(query->operator_fields.max_operator));
        if (query->handle != NULL) {
            int rflag = add_result_to_client_context(query->context, max_result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query && query->type == ADD) {
        Result* result = execute_add_operator(&(query->operator_fields.add_operator));
        if (query->handle != NULL) {
            int rflag = add_result_to_client_context(query->context, result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query && query->type == SUB) {
        Result* result = execute_sub_operator(&(query->operator_fields.sub_operator));
        if (query->handle != NULL) {
            int rflag = add_result_to_client_context(query->context, result, query->handle);
            if (rflag < 0) {
                return "";
            }
        }
        return "";
    } else if (query && query->type == LOAD) {
        char* load_result = execute_load_operator(&(query->operator_fields.load_operator));
        return load_result;
    } else if (query && query->type == PRINT) {
        char* print_result = execute_print_operator(&(query->operator_fields.print_operator));
        return print_result;
    } else if(query && query->type == SHUTDOWN) {
        Status shutdown_status = shutdown_server();
        if (shutdown_status.code != OK) {
            return "";
        }
        return "";
    }
    return "";
}

Result* fetch(Column* val_vec, Result* posn_vec, Status* ret_status) {
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


Result* select_from_column(Column* column, NullableInt* range_start, NullableInt* range_end, Status* select_status) {
	Result* result = (Result*) malloc(sizeof(Result));
    result->num_tuples = 0;

	int* posn_vec = (int*) malloc(sizeof(int) * column->size);
	for (size_t i = 0; i < column->size; i++) {
		// TODO: Try splitting out this if statement?
		if ((range_start->is_null || column->data[i] >= range_start->value) && (range_end->is_null || column->data[i] < range_end->value)) {
            posn_vec[result->num_tuples++] = i;
		}
	}
	result->data_type = INT;
	result->payload = posn_vec;

	select_status->code = OK;
	return result;
}

Result* select_positions_from_column(Result* posn_vec, Column* column, NullableInt* range_start, NullableInt* range_end, Status* select_status) {
	Result* result = (Result*) malloc(sizeof(Result));
    result->num_tuples = 0;
	int* result_posn_vec = (int*) malloc(sizeof(int) * posn_vec->num_tuples);
	for (size_t i = 0; i < column->size; i++) {
		// TODO: Try splitting out this if statement?
		if ((range_start->is_null || column->data[i] >= range_start->value) && (range_end->is_null || column->data[i] < range_end->value)) {
            result_posn_vec[result->num_tuples++] = i;
		}
	}
	result->data_type = INT;
	result->payload = posn_vec;

	select_status->code = OK;
	return result;
}

Result* execute_select_operator(SelectOperator* select_operator, Status* select_status) {
    Result* res;
    if (select_operator->posn_vec == NULL) {
        res = select_from_column(
            select_operator->column,
            &(select_operator->range_start),
            &(select_operator->range_end),
            select_status
        );
    } else {
        res = select_positions_from_column(
            select_operator->posn_vec,
            select_operator->column,
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
    double sum = 0;

    clock_t start, end;
    double cpu_time_used;
    start = clock();
    for (size_t i = 0; i < size; i++) {
        sum += (double) data[i];
    }
    end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;
    printf("summing took %fms\n", cpu_time_used);

    Result* result = (Result*) malloc(sizeof(Result));
    result->data_type = FLOAT;
    result->num_tuples = 1;
    result->payload = malloc(sizeof(float));
    ((float*) result->payload)[0] = (float) (sum / ((double) size));
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

    int sum = 0;
    for (size_t i = 0; i < size; i++) {
        sum += data[i];
    }
    Result* result = (Result*) malloc(sizeof(Result));
    result->data_type = INT;
    result->num_tuples = 1;
    result->payload = malloc(sizeof(int));
    ((int*) result->payload)[0] = sum;
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
            row_buffer[col_inserted++] = atoi(value);
            if (col_inserted == load_operator->table->col_count) {
                col_inserted = 0;
                insert_row(load_operator->table, row_buffer, &insert_status);
            }
        }
    }
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
    char* buffer = (char*) malloc(buffer_size);

    // TODO: add logic to increase buffer size if necessary
    // Prob requires set length for each row line

    size_t row_count = get_gcolumn_size(print_operator->generalized_columns[0]);
    for (size_t i = 0; i < row_count; i++) {
        for (int j = 0; j < print_operator->generalized_columns_count; j++) {
            int newly_written = print_gcolumn_data(buffer + written_so_far, print_operator->generalized_columns[j], i);
            written_so_far += newly_written;
            buffer[written_so_far++] = j == print_operator->generalized_columns_count - 1 ? '\n' : ',';
        }
    }
    buffer[written_so_far - 1] = '\0';
    return buffer;
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
