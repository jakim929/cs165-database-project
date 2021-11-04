/* 
 * This file contains methods necessary to parse input from the client.
 * Mostly, functions in parse.c will take in string input and map these
 * strings into database operators. This will require checking that the
 * input from the client is in the correct format and maps to a valid
 * database operator.
 */

#define _DEFAULT_SOURCE
#include <string.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include "cs165_api.h"
#include "parse.h"
#include "utils.h"
#include "client_context.h"

/**
 * Takes a pointer to a string.
 * This method returns the original string truncated to where its first comma lies.
 * In addition, the original string now points to the first character after that comma.
 * This method destroys its input.
 **/

char* next_token(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, ",");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

char* next_line(char** tokenizer, message_status* status) {
    char* token = strsep(tokenizer, "\n");
    if (token == NULL) {
        *status= INCORRECT_FORMAT;
    }
    return token;
}

DbOperator* parse_create_col(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* col_name = next_token(create_arguments_index, &status);
    char* db_and_table_name = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the column name free of quotation marks
    col_name = trim_quotes(col_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(db_and_table_name) - 1;
    if (db_and_table_name[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    db_and_table_name[last_char] = '\0';

    Table* selected_table = lookup_table(db_and_table_name);

    if (!selected_table) {
        cs165_log(stdout, "query unsupported. Bad table name");
        return NULL; //QUERY_UNSUPPORTED
    }

    // make create dbo for column
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _COLUMN;
    strcpy(dbo->operator_fields.create_operator.name, col_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.table = selected_table;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a table.
 * It parses those arguments, checks that they are valid, and creates a table.
 **/


DbOperator* parse_create_tbl(char* create_arguments) {
    message_status status = OK_DONE;
    char** create_arguments_index = &create_arguments;
    char* table_name = next_token(create_arguments_index, &status);
    char* db_name = next_token(create_arguments_index, &status);
    char* col_cnt = next_token(create_arguments_index, &status);

    // not enough arguments
    if (status == INCORRECT_FORMAT) {
        return NULL;
    }
    // Get the table name free of quotation marks
    table_name = trim_quotes(table_name);
    // read and chop off last char, which should be a ')'
    int last_char = strlen(col_cnt) - 1;
    if (col_cnt[last_char] != ')') {
        return NULL;
    }
    // replace the ')' with a null terminating character. 
    col_cnt[last_char] = '\0';
    lookup_db(db_name);
    // check that the database argument is the current active database
    if (!current_db || strcmp(current_db->name, db_name) != 0) {
        cs165_log(stdout, "query unsupported. Bad db name");
        return NULL; //QUERY_UNSUPPORTED
    }
    // turn the string column count into an integer, and check that the input is valid.
    int column_cnt = atoi(col_cnt);
    if (column_cnt < 1) {
        return NULL;
    }
    // make create dbo for table
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = CREATE;
    dbo->operator_fields.create_operator.create_type = _TABLE;
    strcpy(dbo->operator_fields.create_operator.name, table_name);
    dbo->operator_fields.create_operator.db = current_db;
    dbo->operator_fields.create_operator.col_count = column_cnt;
    return dbo;
}

/**
 * This method takes in a string representing the arguments to create a database.
 * It parses those arguments, checks that they are valid, and creates a database.
 **/


DbOperator* parse_create_db(char* create_arguments) {
    char *token;
    token = strsep(&create_arguments, ",");
    // not enough arguments if token is NULL
    if (token == NULL) {
        return NULL;
    } else {
        // create the database with given name
        char* db_name = token;
        // trim quotes and check for finishing parenthesis.
        db_name = trim_quotes(db_name);
        int last_char = strlen(db_name) - 1;
        if (last_char < 0 || db_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        db_name[last_char] = '\0';

        token = strsep(&create_arguments, ",");
        if (token != NULL) {
            return NULL;
        }
        // make create operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = CREATE;
        dbo->operator_fields.create_operator.create_type = _DB;
        strcpy(dbo->operator_fields.create_operator.name, db_name);
        return dbo;
    }
}

/**
 * parse_create parses a create statement and then passes the necessary arguments off to the next function
 **/
DbOperator* parse_create(char* create_arguments) {
    message_status mes_status;
    DbOperator* dbo = NULL;
    char *tokenizer_copy, *to_free;
    // Since strsep destroys input, we create a copy of our input. 
    tokenizer_copy = to_free = malloc((strlen(create_arguments)+1) * sizeof(char));
    char *token;
    strcpy(tokenizer_copy, create_arguments);
    // check for leading parenthesis after create. 
    if (strncmp(tokenizer_copy, "(", 1) == 0) {
        tokenizer_copy++;
        // token stores first argument. Tokenizer copy now points to just past first ","
        token = next_token(&tokenizer_copy, &mes_status);
        if (mes_status == INCORRECT_FORMAT) {
            return NULL;
        } else {
            // pass off to next parse function. 
            if (strcmp(token, "db") == 0) {
                dbo = parse_create_db(tokenizer_copy);
            } else if (strcmp(token, "tbl") == 0) {
                dbo = parse_create_tbl(tokenizer_copy);
            } else if (strcmp(token, "col") == 0) {
                dbo = parse_create_col(tokenizer_copy);
            } else {
                mes_status = UNKNOWN_COMMAND;
            }
        }
    } else {
        mes_status = UNKNOWN_COMMAND;
    }
    free(to_free);
    return dbo;
}

/**
 * parse_insert reads in the arguments for a create statement and 
 * then passes these arguments to a database function to insert a row.
 **/

DbOperator* parse_insert(char* query_command, message* send_message) {
    unsigned int columns_inserted = 0;
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* table_name = next_token(command_index, &send_message->status);
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }
        // lookup the table and make sure it exists. 
        Table* insert_table = lookup_table(table_name);
        if (insert_table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        // make insert operator. 
        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = INSERT;
        dbo->operator_fields.insert_operator.table = insert_table;
        dbo->operator_fields.insert_operator.values = malloc(sizeof(int) * insert_table->col_count);
        // parse inputs until we reach the end. Turn each given string into an integer. 
        while ((token = strsep(command_index, ",")) != NULL) {
            int insert_val = atoi(token);
            dbo->operator_fields.insert_operator.values[columns_inserted] = insert_val;
            columns_inserted++;
        }
        // check that we received the correct number of input values
        if (columns_inserted != insert_table->col_count) {
            send_message->status = INCORRECT_FORMAT;
            free_db_operator(dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_select(char* query_command, ClientContext* context, message* send_message) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        char* query_elements[4];

        query_elements[0] = strsep(command_index, ",");
        query_elements[1] = strsep(command_index, ",");
        query_elements[2] = strsep(command_index, ",");
        query_elements[3] = strsep(command_index, ",");

        if (
            query_elements[0] == NULL ||
            query_elements[1] == NULL ||
            query_elements[2] == NULL
        ) {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }

        char* last_param = query_elements[3] == NULL ? query_elements[2] : query_elements[3];
        int last_char = strlen(last_param) - 1;
        if (last_char < 0 || last_param[last_char] != ')') {
            send_message->status = INCORRECT_FORMAT;
            return NULL;
        }
        // replace final ')' with null-termination character.
        last_param[last_char] = '\0';

        char* posn_vec_name = NULL;
        char* gcolumn_name;
        char* range_start_value;
        char* range_end_value;

        if (query_elements[3] == NULL) {
            gcolumn_name = query_elements[0];
            range_start_value = query_elements[1];
            range_end_value = query_elements[2];
        } else {
            posn_vec_name = query_elements[0];
            gcolumn_name = query_elements[1];
            range_start_value = query_elements[2];
            range_end_value = query_elements[3];
        }

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = SELECT;
        dbo->operator_fields.select_operator.posn_vec = NULL;

        if (posn_vec_name != NULL) {
            GeneralizedColumn* gcolumn = lookup_gcolumn_by_handle(context, posn_vec_name);
            if (
                gcolumn == NULL ||
                gcolumn->column_type != RESULT ||
                gcolumn->column_pointer.result->data_type != INT
            ) {
                send_message->status = OBJECT_NOT_FOUND;
                free(dbo);
                return NULL;
            }
            dbo->operator_fields.select_operator.posn_vec = gcolumn->column_pointer.result;
        }
        GeneralizedColumn* gcolumn = lookup_gcolumn_by_handle(context, gcolumn_name);
        // lookup the table and column and make sure it exists. 
        if (gcolumn == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        dbo->operator_fields.select_operator.gcolumn = gcolumn;
        parse_nullable_int(&(dbo->operator_fields.select_operator.range_start), range_start_value);
        parse_nullable_int(&(dbo->operator_fields.select_operator.range_end), range_end_value);

        // check that range parameters make sense
        if ((!dbo->operator_fields.select_operator.range_start.is_null && !dbo->operator_fields.select_operator.range_end.is_null && dbo->operator_fields.select_operator.range_end.value < dbo->operator_fields.select_operator.range_start.value)) {
            send_message->status = INCORRECT_FORMAT;
            free (dbo);
            return NULL;
        } 
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

DbOperator* parse_fetch(char* query_command, ClientContext* context, message* send_message) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        // parse table input
        char* column_name = next_token(command_index, &send_message->status);
        char* posn_vec_name = next_token(command_index, &send_message->status);
        int last_char = strlen(posn_vec_name) - 1;
        if (last_char < 0 || posn_vec_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        posn_vec_name[last_char] = '\0';
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        Table* table = NULL;
        Column* column = NULL;
        lookup_table_and_column(&table, &column, column_name);
        GeneralizedColumn* posn_vec_gcolumn = lookup_gcolumn_by_handle(context, posn_vec_name);
        // lookup the table and column and make sure it exists. posn_vec needs to be RESULT
        if (
            table == NULL ||
            column == NULL ||
            posn_vec_gcolumn == NULL ||
            posn_vec_gcolumn->column_type != RESULT ||
            posn_vec_gcolumn->column_pointer.result->data_type != INT
        ) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = FETCH;
        dbo->operator_fields.fetch_operator.column = column;
        dbo->operator_fields.fetch_operator.posn_vec = posn_vec_gcolumn->column_pointer.result;
    
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

int parse_two_gcolumn_query(GeneralizedColumn** gcolumn1, GeneralizedColumn** gcolumn2, char* query_command, ClientContext* context, message* send_message) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        char* gcolumn1_name = next_token(command_index, &send_message->status);
        char* gcolumn2_name = next_token(command_index, &send_message->status);
        int last_char = strlen(gcolumn2_name) - 1;
        if (last_char < 0 || gcolumn2_name[last_char] != ')') {
            return -1;
        }
        // replace final ')' with null-termination character.
        gcolumn2_name[last_char] = '\0';
        if (send_message->status == INCORRECT_FORMAT) {
            return -1;
        }

        *gcolumn1 = lookup_gcolumn_by_handle(context, gcolumn1_name);
        *gcolumn2 = lookup_gcolumn_by_handle(context, gcolumn2_name);

        if (
            *gcolumn1 == NULL ||
            *gcolumn2 == NULL
        ) {
            send_message->status = OBJECT_NOT_FOUND;
            return -1;
        }

        return 0;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return -1;
    }
}

int parse_two_int_result_query(Result** result1, Result** result2, char* query_command, ClientContext* context, message* send_message) {
    GeneralizedColumn* gcolumn1;
    GeneralizedColumn* gcolumn2;
    if (parse_two_gcolumn_query(&gcolumn1, &gcolumn2, query_command, context, send_message) < 0) {
        return -1;
    }
    if (
        gcolumn1->column_type != RESULT ||
        gcolumn1->column_pointer.result->data_type != INT ||
        gcolumn2->column_type != RESULT ||
        gcolumn2->column_pointer.result->data_type != INT
    ) {
        send_message->status = OBJECT_NOT_FOUND;
        return -1;
    }
    *result1 = gcolumn1->column_pointer.result;
    *result2 = gcolumn2->column_pointer.result;
    return 0;
}

DbOperator* parse_add(char* query_command, ClientContext* context, message* send_message) {
    Result* result1;
    Result* result2;
    if (parse_two_int_result_query(&result1, &result2, query_command, context, send_message)) {
        return NULL;
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = ADD;
    dbo->operator_fields.add_operator.val_vec1 = result1;
    dbo->operator_fields.add_operator.val_vec2 = result2;
    return dbo;
}

DbOperator* parse_sub(char* query_command, ClientContext* context, message* send_message) {
    Result* result1;
    Result* result2;
    if (parse_two_int_result_query(&result1, &result2, query_command, context, send_message)) {
        return NULL;
    }

    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SUB;
    dbo->operator_fields.sub_operator.val_vec1 = result1;
    dbo->operator_fields.sub_operator.val_vec2 = result2;
    return dbo;
}

GeneralizedColumn* parse_single_gcolumn_query(char* query_command, ClientContext* context, message* send_message) {
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char* gcolumn_name = query_command;
        int last_char = strlen(gcolumn_name) - 1;
        if (last_char < 0 || gcolumn_name[last_char] != ')') {
            return NULL;
        }
        // replace final ')' with null-termination character.
        gcolumn_name[last_char] = '\0';
        if (send_message->status == INCORRECT_FORMAT) {
            return NULL;
        }

        GeneralizedColumn* gcolumn = lookup_gcolumn_by_handle(context, gcolumn_name);
        // RIGHTNOW LOOKUP for TABLE column names!


        // lookup the table and column and make sure it exists. posn_vec needs to be RESULT
        if (gcolumn == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }
        return gcolumn;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

GeneralizedColumn* parse_single_int_result_query(char* query_command, ClientContext* context, message* send_message) {
    GeneralizedColumn* gcolumn = parse_single_gcolumn_query(query_command, context, send_message);
    if (
        gcolumn == NULL ||
        gcolumn->column_type != RESULT ||
        gcolumn->column_pointer.result->data_type != INT
    ) {
        send_message->status = OBJECT_NOT_FOUND;
        return NULL;
    }
    return gcolumn;
}

DbOperator* parse_sum(char* query_command, ClientContext* context, message* send_message) {
    GeneralizedColumn* gcolumn = parse_single_gcolumn_query(query_command, context, send_message);
    if (gcolumn == NULL) {
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = SUM;
    dbo->operator_fields.sum_operator.val_vec = gcolumn;
    return dbo;
}


DbOperator* parse_average(char* query_command, ClientContext* context, message* send_message) {
    GeneralizedColumn* gcolumn = parse_single_int_result_query(query_command, context, send_message);
    if (gcolumn == NULL) {
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = AVERAGE;
    dbo->operator_fields.average_operator.val_vec = gcolumn->column_pointer.result;
    return dbo;
}

DbOperator* parse_min(char* query_command, ClientContext* context, message* send_message) {
    GeneralizedColumn* gcolumn = parse_single_int_result_query(query_command, context, send_message);
    if (gcolumn == NULL) {
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = MIN;
    dbo->operator_fields.min_operator.val_vec = gcolumn->column_pointer.result;
    return dbo;
}

DbOperator* parse_max(char* query_command, ClientContext* context, message* send_message) {
    GeneralizedColumn* gcolumn = parse_single_int_result_query(query_command, context, send_message);
    if (gcolumn == NULL) {
        return NULL;
    }
    DbOperator* dbo = malloc(sizeof(DbOperator));
    dbo->type = MAX;
    dbo->operator_fields.max_operator.val_vec = gcolumn->column_pointer.result;
    return dbo;
}

DbOperator* parse_print(char* query_command, ClientContext* context, message* send_message) {
    char* token = NULL;
    // check for leading '('
    if (strncmp(query_command, "(", 1) == 0) {
        query_command++;
        char** command_index = &query_command;
        int last_char = strlen(query_command) - 1;
        if (last_char < 0 || query_command[last_char] != ')') {
            return NULL;
        }
        query_command[last_char] = '\0';

        DbOperator* dbo = malloc(sizeof(DbOperator));
        int allocated_columns_count = INITIAL_PRINT_OPERATOR_COLUMNS_CAPACITY;
        dbo->type = PRINT;
        dbo->operator_fields.print_operator.generalized_columns_count = 0;
        dbo->operator_fields.print_operator.generalized_columns = (GeneralizedColumn**) malloc(allocated_columns_count * sizeof(GeneralizedColumn*));
        while ((token = strsep(command_index, ",")) != NULL) {
            if (dbo->operator_fields.print_operator.generalized_columns_count == allocated_columns_count) {
                allocated_columns_count *= 2;
                dbo->operator_fields.print_operator.generalized_columns = (GeneralizedColumn**) realloc(dbo->operator_fields.print_operator.generalized_columns, allocated_columns_count * sizeof(GeneralizedColumn*));
            }
            GeneralizedColumn* gen_column = lookup_gcolumn_by_handle(context, token);
            if (gen_column == NULL) {
                send_message->status = OBJECT_NOT_FOUND;
                free_db_operator(dbo);
                return NULL;
            }
            dbo->operator_fields.print_operator.generalized_columns[dbo->operator_fields.print_operator.generalized_columns_count++] = gen_column;
        }
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

Table* get_table_from_column_names(char* column_names, message* send_message) {
    char** column_name_index = &column_names;
    char* first_column_name = next_token(column_name_index, &send_message->status);
    Table* table;
    Column* column;
    lookup_table_and_column(&table, &column, first_column_name);
    return table;
}

DbOperator* parse_load(char* query_command, message* send_message) {
    if (strncmp(query_command, "\n", 1) == 0) {
        query_command++;
        char** command_index = &query_command;

        char* column_names = next_line(command_index, &send_message->status);

        Table* table = get_table_from_column_names(column_names, send_message);
        
        if (table == NULL) {
            send_message->status = OBJECT_NOT_FOUND;
            return NULL;
        }

        DbOperator* dbo = malloc(sizeof(DbOperator));
        dbo->type = LOAD;
        dbo->operator_fields.load_operator.table = table;
        dbo->operator_fields.load_operator.load_data = *command_index;
    
        return dbo;
    } else {
        send_message->status = UNKNOWN_COMMAND;
        return NULL;
    }
}

/**
 * parse_command takes as input the send_message from the client and then
 * parses it into the appropriate query. Stores into send_message the
 * status to send back.
 * Returns a db_operator.
 * 
 * Getting Started Hint:
 *      What commands are currently supported for parsing in the starter code distribution?
 *      How would you add a new command type to parse? 
 *      What if such command requires multiple arguments?
 **/
DbOperator* parse_command(char* query_command, message* send_message, int client_socket, ClientContext* context) {
    // a second option is to malloc the dbo here (instead of inside the parse commands). Either way, you should track the dbo
    // and free it when the variable is no longer needed. 
    DbOperator *dbo = NULL; // = malloc(sizeof(DbOperator));

    if (strncmp(query_command, "--", 2) == 0) {
        send_message->status = OK_DONE;
        // The -- signifies a comment line, no operator needed.  
        return NULL;
    }

    char *equals_pointer = strchr(query_command, '=');
    char *handle = query_command;
    if (equals_pointer != NULL) {
        // handle exists, store here. 
        *equals_pointer = '\0';
        cs165_log(stdout, "FILE HANDLE: %s\n", handle);
        query_command = ++equals_pointer;
    } else {
        handle = NULL;
    }

    cs165_log(stdout, "QUERY: %s\n", query_command);

    // by default, set the status to acknowledge receipt of command,
    //   indication to client to now wait for the response from the server.
    //   Note, some commands might want to relay a different status back to the client.
    send_message->status = OK_WAIT_FOR_RESPONSE;

    bool is_load_command = strncmp(query_command, "load", 4) == 0;
    query_command = is_load_command ? trim_whitespace_retain_new_line(query_command) : trim_whitespace(query_command);
    // check what command is given. 
    if (strncmp(query_command, "create", 6) == 0) {
        query_command += 6;
        dbo = parse_create(query_command);
        if(dbo == NULL){
            send_message->status = INCORRECT_FORMAT;
        }
        else{
            send_message->status = OK_DONE;
        }
    } else if (strncmp(query_command, "relational_insert", 17) == 0) {
        query_command += 17;
        dbo = parse_insert(query_command, send_message);
    } else if (strncmp(query_command, "select", 6) == 0) {
        query_command += 6;
        dbo = parse_select(query_command, context, send_message);
    } else if (strncmp(query_command, "fetch", 5) == 0) {
        query_command += 5;
        dbo = parse_fetch(query_command, context, send_message);
    } else if (strncmp(query_command, "avg", 3) == 0) {
        query_command += 3;
        dbo = parse_average(query_command, context, send_message);
    } else if (strncmp(query_command, "sum", 3) == 0) {
        query_command += 3;
        dbo = parse_sum(query_command, context, send_message);
    } else if (strncmp(query_command, "min", 3) == 0) {
        query_command += 3;
        dbo = parse_min(query_command, context, send_message);
    } else if (strncmp(query_command, "max", 3) == 0) {
        query_command += 3;
        dbo = parse_max(query_command, context, send_message);
    } else if (strncmp(query_command, "add", 3) == 0) {
        query_command += 3;
        dbo = parse_add(query_command, context, send_message);
    } else if (strncmp(query_command, "sub", 3) == 0) {
        query_command += 3;
        dbo = parse_sub(query_command, context, send_message);
    } else if (strncmp(query_command, "print", 5) == 0) {
        query_command += 5;
        dbo = parse_print(query_command, context, send_message);
    } else if (strncmp(query_command, "load", 4) == 0) {
        query_command += 4;
        dbo = parse_load(query_command, send_message);
    } else if (strncmp(query_command, "shutdown", 8) == 0) {
        query_command += 8;
        dbo = malloc(sizeof(DbOperator));
        dbo->type = SHUTDOWN;
    }
    if (dbo == NULL) {
        return dbo;
    }
    
    dbo->handle = handle;
    dbo->client_fd = client_socket;
    dbo->context = context;
    return dbo;
}
