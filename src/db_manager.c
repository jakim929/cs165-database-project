#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "cs165_api.h"
#include "catalog.h"
#include "utils.h"
#include "column_index.h"
#include "column.h"
#include "table.h"
#include "mmap_helper.h"

// In this class, there will always be only one active database at a time
Db *current_db;

int freed_nodes = 0;
int resize_column_capacity(Column* column, size_t new_capacity);

void insert_row(Table* table, int* values, Status *ret_status) {
	if (table->table_capacity == table->table_length) {
		table->table_capacity *= 2;
		for (size_t i = 0; i < table->col_count; i++) {
			resize_column_capacity(&(table->columns[i]), table->table_capacity);
		}
	}
	for (size_t i = 0; i < table->col_count; i++) {
		table->columns[i].data[table->columns[i].size++] = values[i];
	}
	table->table_length++;
	ret_status->code = OK;
	return;
}

int resize_column_capacity(Column* column, size_t new_capacity) {
	printf("about to resize capacity\n");
	char data_path[MAX_PATH_NAME_SIZE] = "";
	get_data_path_from_column(data_path, column);

	column->data = (int*) resize_mmap_write_capacity(column->data, data_path, column->capacity * sizeof(int), new_capacity * sizeof(int));
	if (column->data == NULL) {
		return 0;
	}

	if (column->index != NULL) {
		resize_column_index(column->index, column->capacity, new_capacity);
	}
	
	column->capacity = new_capacity;
	return 0;
}

Column* create_column(Table *table, char *name, bool sorted, Status *ret_status) {
	(void) sorted;

	if (table->columns_capacity == 0) {
		ret_status->code = ERROR;
		return 0;
	}

	struct Column* new_column = &(table->columns[table->col_count - table->columns_capacity]);
	table->columns_capacity--;

	new_column->table = table;
	new_column->capacity = table->table_capacity;
	new_column->size = 0;
	new_column->index = NULL;

	strncpy(new_column->name, name, MAX_SIZE_NAME);

	char data_path[MAX_PATH_NAME_SIZE] = "";
	get_data_path_from_column(data_path, new_column);

	new_column->data = mmap_to_write(data_path, new_column->capacity * sizeof(int));
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
	new_table->name[0] = '\0';
	strncpy(new_table->name, name, MAX_SIZE_NAME);
	new_table->base_directory[0] = '\0';
	strncpy(new_table->base_directory, db->base_directory, MAX_PATH_NAME_SIZE);
	new_table->col_count = num_columns;
	new_table->columns = (Column*) malloc(sizeof(Column) * num_columns);
	new_table->columns_capacity = num_columns;
	new_table->table_length = 0;
	new_table->table_capacity = INITIAL_COLUMN_CAPACITY;
	new_table->has_clustered_index = false;
	new_table->clustered_index_id = 0;

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
	new_db->base_directory[0] = '\0';
	strcat(new_db->base_directory, BASE_CATALOG_PATH);
	strcat(new_db->base_directory, "/");
	strcat(new_db->base_directory, db_name);

	maybe_create_directory(BASE_CATALOG_PATH);
	maybe_create_directory(new_db->base_directory);

	current_db = new_db;
	ret_status.code = OK;

	return ret_status;
}

// Assumes column is empty
void load_into_column(Column* column, int* buffer, size_t size) {
	memcpy(column->data, buffer, size * sizeof(int));
	column->size = size;

	if (column->index != NULL) {
		if (column->index->type == SORTED) {
			memcpy(column->index->index_pointer.sorted_index->data, buffer, size * sizeof(int));
			for (size_t i = 0; i < size; i++) {
				column->index->index_pointer.sorted_index->positions[i] = i;
			}
			if (!column->is_clustered) {
				int* result = (int*) malloc(sizeof(int) * size);
				int* posn_vec_result = (int*) malloc(sizeof(int) * size);
				printf("hello! creating unclustered sorted index for %s\n", column->name);
				mergesort(
					size,
					column->index->index_pointer.sorted_index->data,
					result,
					column->index->index_pointer.sorted_index->positions,
					posn_vec_result
				);
				printf("%d rand\n", column->index->index_pointer.sorted_index->positions[200]);
				free(result);
				free(posn_vec_result);
			}
		} else if (column->index->type == BTREE) {
			memcpy(column->index->index_pointer.btree_index->data, buffer, size * sizeof(int));
			for (size_t i = 0; i < size; i++) {
				column->index->index_pointer.btree_index->positions[i] = i;
			}
			if (!column->is_clustered) {
				int* result = (int*) malloc(sizeof(int) * size);
				int* posn_vec_result = (int*) malloc(sizeof(int) * size);
				printf("hello! creating unclustered btree index for %s\n", column->name);
				mergesort(
					size,
					column->index->index_pointer.btree_index->data,
					result,
					column->index->index_pointer.btree_index->positions,
					posn_vec_result
				);
				printf("%d rand\n", column->index->index_pointer.btree_index->positions[200]);
				free(result);
				free(posn_vec_result);
			}
			column->index->index_pointer.btree_index->root_node = construct_btree(
				column->index->index_pointer.btree_index->data,
				column->index->index_pointer.btree_index->positions,
				size,
				BTREE_FANOUT,
				BTREE_PAGESIZE
			);
			column->index->index_pointer.btree_index->size = size;
			printf("finished creating btree after load\n");
		}
	}
} 

// Assumes table is empty
void load_into_table(Table* table, int** cols, int int_size) {
	size_t size = (size_t) int_size;
	printf("%zu size to load\n", size);
	if (table->table_capacity < size) {
	printf("%zu size to load, about to resize\n", size);

		// TODO prob should size it to be larger;
		table->table_capacity = (size_t) ((double) size * 1.2);
		for (size_t i = 0; i < table->col_count; i++) {
			resize_column_capacity(&(table->columns[i]), table->table_capacity);
		}
	}
	for (size_t i = 0; i < table->col_count; i++) {
		load_into_column(&table->columns[i], cols[i], size);
	}
	table->table_length = size;
}

int free_btree(BTreeNode* node) {
	free(node->values);
	if (node->type == INNER) {
		for(size_t i = 0; i < node->pointers_count; i++) {
			free_btree((BTreeNode*) node->pointers[i]);
		}
	}
	free(node->pointers);
	free(node);
	freed_nodes++;
	return 0;
}

int free_column_index(ColumnIndex* index, size_t capacity) {
	int rflag = 0;
	if (index->type == SORTED) {
		rflag = msync(index->index_pointer.sorted_index->data, capacity * sizeof(int), MS_SYNC);
		if(rflag == -1)
		{
			return -1;
		}
		rflag = munmap(index->index_pointer.sorted_index->data, capacity * sizeof(int));
		if(rflag == -1)
		{
			return -1;
		}
		rflag = msync(index->index_pointer.sorted_index->positions, capacity * sizeof(int), MS_SYNC);
		if(rflag == -1)
		{
			return -1;
		}
		rflag = munmap(index->index_pointer.sorted_index->positions, capacity * sizeof(int));
		if(rflag == -1)
		{
			return -1;
		}
	} else if (index->type == BTREE) {
		freed_nodes = 0;
		free_btree(index->index_pointer.btree_index->root_node);
		printf("FREED NODES: %d \n", freed_nodes);
		rflag = msync(index->index_pointer.btree_index->data, capacity * sizeof(int), MS_SYNC);
		if(rflag == -1)
		{
			return -1;
		}
		rflag = munmap(index->index_pointer.btree_index->data, capacity * sizeof(int));
		if(rflag == -1)
		{
			return -1;
		}
		rflag = msync(index->index_pointer.btree_index->positions, capacity * sizeof(int), MS_SYNC);
		if(rflag == -1)
		{
			return -1;
		}
		rflag = munmap(index->index_pointer.btree_index->positions, capacity * sizeof(int));
		if(rflag == -1)
		{
			return -1;
		}
	}
	return 0;
}

int free_column(Column* column) {
	persist_col_catalog(column);
	if (!column) {
		return 0;
	}
	int rflag = 0;
	if (column->index != NULL) {
		rflag = free_column_index(column->index, column->capacity);
		if(rflag == -1)
		{
			return -1;
		}
	}

    rflag = msync(column->data, column->capacity * sizeof(int), MS_SYNC);
    if(rflag == -1)
    {
        return -1;
    }
    rflag = munmap(column->data, column->capacity * sizeof(int));
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
