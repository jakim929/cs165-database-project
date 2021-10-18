#include <stdio.h>

#include "cs165_api.h"
#include "utils.h"
#include "client_context.h"


char* execute_print_operator(PrintOperator* print_operator);

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
        return "165";
    }
    if(query && query->type == CREATE){
        if(query->operator_fields.create_operator.create_type == _DB){
            if (create_db(query->operator_fields.create_operator.name).code == OK) {
                return "165";
            } else {
                return "Failed";
            }
        }
        else if(query->operator_fields.create_operator.create_type == _TABLE) {
            Status create_status;
            create_table(query->operator_fields.create_operator.db, 
                query->operator_fields.create_operator.name, 
                query->operator_fields.create_operator.col_count, 
                &create_status);
            if (create_status.code != OK) {
                return "Failed";
            }
            return "165";
        } else if(query->operator_fields.create_operator.create_type == _COLUMN) {
            Status create_status;
            create_column(query->operator_fields.create_operator.table, 
                query->operator_fields.create_operator.name, 
                false,
                &create_status);
            if (create_status.code != OK) {
                return "Failed";
            }
            return "165";
        }
    }else if(query && query->type == INSERT) {
        Status insert_status;
        insert_row(query->operator_fields.insert_operator.table, query->operator_fields.insert_operator.values, &insert_status);
        if (insert_status.code == OK) {
            return "165";
        }
    }else if(query && query->type == SELECT) {
        Status select_status;
        Result* select_result = select_from_column(query->operator_fields.select_operator.column, &(query->operator_fields.select_operator.range_start), &(query->operator_fields.select_operator.range_end), &select_status);
        struct GeneralizedColumn gen_column;
        union GeneralizedColumnPointer gen_column_pointer;
        if (query->handle != NULL) {
            gen_column_pointer.result = select_result;
            gen_column.column_pointer = gen_column_pointer;
            gen_column.column_type = RESULT;
            int rflag = add_generalized_column_to_client_context(query->context, &gen_column, query->handle);
            if (rflag < 0) {
                select_status.code = ERROR;
                return "";
            }
        }

        if (select_status.code == OK) {
            return "165";
        }
    }else if (query && query->type == PRINT) {
        char* print_result = execute_print_operator(&(query->operator_fields.print_operator));
        return print_result;
    }else if(query && query->type == SHUTDOWN) {
        Status shutdown_status = shutdown_server();
        if (shutdown_status.code != OK) {
            return "Failed";
        }
        return "Bye 165";
    }
    free_db_operator(query);
    return "165";
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
