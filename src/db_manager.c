#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>

#include <errno.h>

#include "cs165_api.h"
#include "catalog.h"
#include "utils.h"

// In this class, there will always be only one active database at a time
Db *current_db;

GeneralizedColumn* fetch(Column* val_vec, Result* posn_vec, Status* ret_status) {
	Result* result = (Result*) malloc(sizeof(Result));
	int* res_vec = (int*) malloc(sizeof(int) * (posn_vec->num_tuples));
	int* data_payload = (int*) posn_vec->payload;
	for (size_t i = 0; i < posn_vec->num_tuples; i++) {
		res_vec[i] = val_vec->data[data_payload[i]];
	}

	result->num_tuples = posn_vec->num_tuples;
	result->data_type = INT;
	result->payload = res_vec;

	GeneralizedColumn* result_gen_column = (GeneralizedColumn*) malloc(sizeof(GeneralizedColumn));
	result_gen_column->column_type = RESULT;
	result_gen_column->column_pointer.result = result;
	
	ret_status->code = OK;
	return result_gen_column;
}

Result* select_from_column(Column* column, NullableInt* range_start, NullableInt* range_end, Status* select_status) {
	Result* result = (Result*) malloc(sizeof(Result));
	int* posn_vec = (int*) malloc(sizeof(int) * (column->size));
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

void insert_row(Table* table, int* values, Status *ret_status) {
	for (size_t i = 0; i < table->col_count; i++) {
		struct Column col = table->columns[i];
		col.data[table->table_length] = values[i];
		col.size++;
	}
	table->table_length++;
	ret_status->code = OK;
	return;
}

Column* create_column(Table *table, char *name, bool sorted, Status *ret_status) {
	(void) sorted;

	if (table->columns_capacity == 0) {
		ret_status->code = ERROR;
		return 0;
	}

	struct Column* new_column = &(table->columns[table->col_count - table->columns_capacity]);
	table->columns_capacity--;

	strncpy(new_column->name, name, MAX_SIZE_NAME);
	snprintf(new_column->path, MAX_PATH_NAME_SIZE, "%s/%s.%s.data", table->base_directory, table->name, name);

	int rflag = -1;
	int fd = open(new_column->path, O_RDWR | O_CREAT, (mode_t)0600);

	if(fd == -1) {
		ret_status->code = ERROR;
		return NULL;
	}

	rflag = lseek(fd, (INITIAL_COLUMN_CAPACITY * sizeof(int)) - 1, SEEK_SET);

	if (rflag == -1) {
		close(fd);
		ret_status->code = ERROR;
		return NULL;
	}

	rflag = write(fd, "", 1);

	if (rflag == -1) {
		close(fd);
		ret_status->code = ERROR;
		return NULL;
	}

	new_column->data = (int*) mmap(0, (INITIAL_COLUMN_CAPACITY * sizeof(int)), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);

	ret_status->code = OK;
	return new_column;
}

/* 
 * Here you will create a table object. The Status object can be used to return
 * to the caller that there was an error in table creation
 */
Table* create_table(Db* db, const char* name, size_t num_columns, Status *ret_status) {
	if (db->tables_capacity == 0) {
		size_t prev_tables_size = db->tables_size;
		db->tables_size = prev_tables_size * 2;
		db->tables = (Table*) realloc(db->tables, db->tables_size);
		db->tables_capacity = db->tables_size - prev_tables_size;
	}
	struct Table* new_table = &(db->tables[db->tables_size - db->tables_capacity]);
	db->tables_capacity--;

	strncpy(new_table->name, name, MAX_SIZE_NAME);
	strncpy(new_table->base_directory, db->base_directory, MAX_PATH_NAME_SIZE);
	new_table->col_count = num_columns;
	new_table->columns = (Column*) malloc(sizeof(Column) * num_columns);
	new_table->columns_capacity = num_columns;
	new_table->table_length = 0;

	ret_status->code=OK;
	return new_table;
}

/* 
 * Similarly, this method is meant to create a database.
 */
Status create_db(const char* db_name) {
	struct Status ret_status;
	struct Db* new_db = (Db*) malloc(sizeof(Db));
	strncpy(new_db->name, db_name, MAX_SIZE_NAME);
	new_db->tables_size = INITIAL_TABLES_SIZE;
	new_db->tables_capacity = INITIAL_TABLES_SIZE;
	new_db->tables = (Table*) malloc(sizeof(Table) * INITIAL_TABLES_SIZE);
	snprintf(new_db->base_directory, MAX_PATH_NAME_SIZE, "%s/%s", BASE_CATALOG_PATH, db_name);
	maybe_create_directory(BASE_CATALOG_PATH);
	maybe_create_directory(new_db->base_directory);

	current_db = new_db;
	ret_status.code = OK;

	return ret_status;
}

int free_column(Column* column) {
	if (!column) {
		return 0;
	}
    int rflag = msync(column->data, INITIAL_COLUMN_CAPACITY * sizeof(int), MS_SYNC);
    if(rflag == -1)
    {
        return -1;
    }
    rflag = munmap(column->data, INITIAL_COLUMN_CAPACITY * sizeof(int));
    if(rflag == -1)
    {
        return -1;
    }
    return 0;
}

int free_table(Table* table) {
	persist_tbl_catalog(table);
    for (size_t i = 0; i < table->col_count; i++) {
        int rflag = free_column(&(table->columns[i]));
		if (rflag == -1) {
			return -1;
		}
    }
	free(table->columns);
	return 0;
}

int free_db(Db* db) {
	if (!db) {
		return 0;
	}

	persist_db_catalog(db);
    for (size_t i = 0; i < db->tables_size - db->tables_capacity; i++) {
        int rflag = free_table(&(db->tables[i]));
		if (rflag == -1) {
			return -1;
		}
    }
	free(db->tables);
	free(db);
	current_db = NULL;
	return 0;
}
