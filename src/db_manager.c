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
	int rflag = msync(column->data, column->capacity * sizeof(int), MS_SYNC);
    if(rflag == -1)
    {
        return -1;
    }
    rflag = munmap(column->data, column->capacity * sizeof(int));
    if(rflag == -1)
    {
        return -1;
    }

	int fd = open(column->path, O_RDWR | O_CREAT, (mode_t)0600);

	if(fd == -1) {
		return -1;
	}

	rflag = lseek(fd, (new_capacity * sizeof(int)) - 1, SEEK_SET);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	rflag = write(fd, "", 1);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	column->data = (int*) mmap(0, (new_capacity * sizeof(int)), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	column->capacity = new_capacity;
	close(fd);
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

	new_column->capacity = table->table_capacity;
	new_column->size = 0;

	strncpy(new_column->name, name, MAX_SIZE_NAME);
	new_column->path[0] = '\0';
	strcat(new_column->path, table->base_directory);
	strcat(new_column->path, "/");
	strcat(new_column->path, table->name);
	strcat(new_column->path, ".");
	strcat(new_column->path, name);
	strcat(new_column->path, ".data");
	
	int rflag = -1;
	int fd = open(new_column->path, O_RDWR | O_CREAT, (mode_t)0600);

	if(fd == -1) {
		ret_status->code = ERROR;
		return NULL;
	}

	rflag = lseek(fd, (new_column->capacity * sizeof(int)) - 1, SEEK_SET);

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

	new_column->data = (int*) mmap(0, (new_column->capacity * sizeof(int)), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);

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
	new_table->name[0] = '\0';
	strncpy(new_table->name, name, MAX_SIZE_NAME);
	new_table->base_directory[0] = '\0';
	strncpy(new_table->base_directory, db->base_directory, MAX_PATH_NAME_SIZE);
	new_table->col_count = num_columns;
	new_table->columns = (Column*) malloc(sizeof(Column) * num_columns);
	new_table->columns_capacity = num_columns;
	new_table->table_length = 0;
	new_table->table_capacity = INITIAL_COLUMN_CAPACITY;

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
	strcat(new_db->base_directory, BASE_CATALOG_PATH);
	strcat(new_db->base_directory, "/");
	strcat(new_db->base_directory, db_name);

	maybe_create_directory(BASE_CATALOG_PATH);
	maybe_create_directory(new_db->base_directory);

	current_db = new_db;
	ret_status.code = OK;

	return ret_status;
}

void create_column_index(CreateIndexOperator* create_index_operator) {
	ColumnIndex* index = (ColumnIndex*) malloc(sizeof(ColumnIndex));
	index->type = create_index_operator->type;
	if (index->type == BTREE) {
		BTreeIndex* btree_index = (BTreeIndex*) malloc(sizeof(BTreeIndex));
		index->index_pointer.btree_index = btree_index;

	} else {
		index->index_pointer.sorted_index = (SortedIndex*) malloc(sizeof(SortedIndex));
	}
	create_index_operator->column->index = index;
	create_index_operator->column->is_clustered = create_index_operator->is_clustered;
}

int free_column(Column* column) {
	if (!column) {
		return 0;
	}
    int rflag = msync(column->data, column->capacity * sizeof(int), MS_SYNC);
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
