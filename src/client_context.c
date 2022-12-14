#include <string.h>

#include "utils.h"
#include "catalog.h"
#include "client_context.h"
#include "batched_operator.h"
#include "cs165_api.h"

#define INITIAL_CHANDLE_CAPACITY 1024
#define INITIAL_LOAD_DATA_CHUNK_SIZE 8192

ClientContext* initialize_client_context() {
	ClientContext* client_context = (ClientContext*) malloc(sizeof(ClientContext));
	client_context->chandle_table = (GeneralizedColumnHandle*) malloc(INITIAL_CHANDLE_CAPACITY * sizeof(GeneralizedColumnHandle));
	client_context->chandle_slots = INITIAL_CHANDLE_CAPACITY;
	client_context->chandles_in_use = 0;
	client_context->batched_operator = NULL;
	client_context->load_operator = NULL;
	return client_context;
}

int start_load(ClientContext* client_context, LoadOperator* load_operator) {
	client_context->load_operator = (LoadOperator*) malloc(sizeof(LoadOperator));
	client_context->load_operator->table = load_operator->table;
	client_context->load_operator->data = (char*) malloc(INITIAL_LOAD_DATA_CHUNK_SIZE);
	client_context->load_operator->data[0] = '\0';
	client_context->load_operator->capacity = INITIAL_LOAD_DATA_CHUNK_SIZE;
	client_context->load_operator->size = 0;
	return 0;
}

int add_to_load_operator(ClientContext* client_context, char* payload) {
	int payload_size = strlen(payload);
	if (client_context->load_operator->size + payload_size > client_context->load_operator->capacity) {
		client_context->load_operator->data = realloc(client_context->load_operator->data, client_context->load_operator->capacity * 2);
		client_context->load_operator->capacity *= 2;
	}
	strcpy(client_context->load_operator->data + client_context->load_operator->size, payload);
	client_context->load_operator->size += payload_size;
	return 0;
}

int end_load(ClientContext* client_context) {
	free(client_context->load_operator->data);
	free(client_context->load_operator);
	client_context->load_operator = NULL;
	return 0;
}

int start_batch_query(ClientContext* client_context) {
	// TODO: add logic to resize if necessary
	client_context->batched_operator = initialize_batched_operator();
	return 0;
}

int end_batch_query(ClientContext* client_context) {
	free_batched_operator(client_context->batched_operator);
	client_context->batched_operator = NULL;
	return 0;
}

int add_result_to_client_context(ClientContext* client_context, Result* result, char* handle) {
	struct GeneralizedColumn gen_column;
    union GeneralizedColumnPointer gen_column_pointer;
	gen_column_pointer.result = result;
	gen_column.column_pointer = gen_column_pointer;
	gen_column.column_type = RESULT;
	int rflag = add_generalized_column_to_client_context(client_context, &gen_column, handle);
	return rflag;
}

int add_column_to_client_context(ClientContext* client_context, Column* column, char* handle) {
	struct GeneralizedColumn gen_column;
    union GeneralizedColumnPointer gen_column_pointer;
	gen_column_pointer.column = column;
	gen_column.column_pointer = gen_column_pointer;
	gen_column.column_type = COLUMN;
	int rflag = add_generalized_column_to_client_context(client_context, &gen_column, handle);
	return rflag;
}

int add_placeholder_gcolumn_to_client_context(ClientContext* client_context, char* handle) {
	GeneralizedColumnHandle* existing_gchandle = lookup_gchandle_by_handle(client_context, handle);
	if (existing_gchandle != NULL) {
		return 0;
	}

	struct GeneralizedColumn gen_column;
    union GeneralizedColumnPointer gen_column_pointer;
	gen_column_pointer.result = NULL;
	gen_column.column_pointer = gen_column_pointer;
	gen_column.column_type = PLACEHOLDER;
	int rflag = add_generalized_column_to_client_context(client_context, &gen_column, handle);
	return rflag;
}

int add_generalized_column_to_client_context(ClientContext* client_context, GeneralizedColumn* gen_column, char* handle) {
	GeneralizedColumnHandle* existing_gchandle = lookup_gchandle_by_handle(client_context, handle);
	if (existing_gchandle != NULL) {
		free_generalized_column(&(existing_gchandle->generalized_column));
		existing_gchandle->generalized_column.column_type = gen_column->column_type;
		existing_gchandle->generalized_column.column_pointer = gen_column->column_pointer;
		return 0;
	}
	if (client_context->chandle_slots == client_context->chandles_in_use) {
		client_context->chandle_table = (GeneralizedColumnHandle*) realloc(client_context->chandle_table, 2 * client_context->chandle_slots * sizeof(GeneralizedColumnHandle));
		client_context->chandle_slots *= 2;
	}
	GeneralizedColumnHandle* gen_chandle = &(client_context->chandle_table[client_context->chandles_in_use++]);
	strcpy(gen_chandle->name, handle);
	gen_chandle->generalized_column.column_type = gen_column->column_type;
	gen_chandle->generalized_column.column_pointer = gen_column->column_pointer;
	return 0;
}

GeneralizedColumnHandle* lookup_gchandle_by_handle(ClientContext* client_context, char* handle) {
	for (int i = 0; i < client_context->chandles_in_use; i++) {
		if (strcmp(client_context->chandle_table[i].name, handle) == 0) {
			return &(client_context->chandle_table[i]);
		}
	}
	return NULL;
}

int get_number_of_dots(char* str) {
	int number_of_dots = 0;
	char *dot = strchr(str,'.');
  	while (dot != NULL) {
		number_of_dots++;
		dot = strchr(dot+1, '.');
	}
	return number_of_dots;
}

// TODO optimize lookup with hash tables
GeneralizedColumn* lookup_gcolumn_by_handle(ClientContext* client_context, char* handle) {
	GeneralizedColumnHandle* gchandle = lookup_gchandle_by_handle(client_context, handle);
	if (gchandle == NULL) {
		// May be a column name
		if (get_number_of_dots(handle) == 2) {
			Column* column = lookup_column(handle);
			if (column != NULL) {
				add_column_to_client_context(client_context, column, handle);
				return lookup_gcolumn_by_handle(client_context, handle);
			}
		}
		return NULL;
	}
	return &(gchandle->generalized_column);
}

int free_client_context(ClientContext* client_context) {
	for(int i = 0; i < client_context->chandles_in_use; i++) {
		free_generalized_column_handle(&(client_context->chandle_table[i]));
	}
	free(client_context->chandle_table);
	free(client_context);
	return 0;
}

int free_generalized_column(GeneralizedColumn* gcolumn) {
	if (gcolumn->column_type == RESULT) {
		free(gcolumn->column_pointer.result->payload);
		free(gcolumn->column_pointer.result);
	}
	return 0;
}

int free_generalized_column_handle(GeneralizedColumnHandle* gen_chandle) {
	free_generalized_column(&(gen_chandle->generalized_column));
	return 0;
}

/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */

Column* lookup_column_in_table(Table* tbl, char* column_name) {
	for (size_t i = 0; i < tbl->col_count - tbl->columns_capacity; i++) {
		if (strcmp(tbl->columns[i].name, column_name) == 0) {
			return &(tbl->columns[i]);
		}
	}
	return NULL;
}

Table* lookup_table_in_db(Db* db, char* table_name) {
	for (size_t i = 0; i < db->tables_size - db->tables_capacity; i++) {
		if (strcmp(db->tables[i].name, table_name) == 0) {
			return &(db->tables[i]);
		}
	}
	return NULL;
}

Db* lookup_db(char* name) {
	if (current_db && strcmp(current_db->name, name) == 0) {
		return current_db;
	}

	// add freeing existing db when switching to new one to avoid memory leak;
	current_db = unpersist_db(name);
	return current_db;
}

void lookup_table_and_column(Table** table, Column** column, char* name) {
	const char delimiter[2] = ".";
	char* db_name = strtok(name, delimiter);
    char* table_name = strtok(NULL, delimiter);
	char* column_name = strtok(NULL, delimiter);

	Db* db = lookup_db(db_name);
	*table = lookup_table_in_db(db, table_name);
	*column =  lookup_column_in_table(*table, column_name);
	return;
}

// TODO: LOW PRI: Maybe update with hashtable implementation
Table* lookup_table(char *name) {
	const char delimiter[2] = ".";
    char* db_name = strtok(name, delimiter);
    char* table_name = strtok(NULL, delimiter);

	Db* db = lookup_db(db_name);
	return lookup_table_in_db(db, table_name);
}

// TODO: LOW PRI: Maybe update with hashtable implementation
Column* lookup_column(char *name) {
	char name_copy[strlen(name) + 1];
	strcpy(name_copy, name);
	const char delimiter[2] = ".";
	char* db_name = strtok(name_copy, delimiter);
    char* table_name = strtok(NULL, delimiter);
	char* column_name = strtok(NULL, delimiter);

	Db* db = lookup_db(db_name);
	Table* tbl = lookup_table_in_db(db, table_name);

	return lookup_column_in_table(tbl, column_name);
}

/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
**/
