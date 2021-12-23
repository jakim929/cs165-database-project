#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>

#include <errno.h>

#include "cs165_api.h"
#include "utils.h"
#include "table.h"
#include "column.h"
#include "mmap_helper.h"
#include "column_index.h"
#include "catalog.h"

void print_db_data(Db* db);

Db* unpersist_db(char* db_name) {
    char db_catalog_path[MAX_PATH_NAME_SIZE] = "";
	strcat(db_catalog_path, BASE_CATALOG_PATH);
	strcat(db_catalog_path, "/");
	strcat(db_catalog_path, db_name);
	strcat(db_catalog_path, "/");
	strcat(db_catalog_path, db_name);
	strcat(db_catalog_path, ".db");

    PersistedDbCatalog* db_catalog = (PersistedDbCatalog*) mmap_to_read(db_catalog_path, sizeof(PersistedDbCatalog));

    struct Db* db = (Db*) malloc(sizeof(Db));
    db->tables_size = db_catalog->tables_size;
	db->tables_capacity = db_catalog->tables_capacity;
	strncpy(db->name, db_catalog->name, MAX_SIZE_NAME);
    strncpy(db->base_directory, db_catalog->base_directory, MAX_SIZE_NAME);

    db->tables = (Table*) malloc(sizeof(Table) * db->tables_size);

    const char delimiter[2] = ",";
    char *tokenizer;
    char table_catalog_path[MAX_PATH_NAME_SIZE] = "";
    size_t table_i = 0;

	char* tb_names = (char*) malloc((strlen(db_catalog->table_names) + 1) * sizeof(char));
	strcpy(tb_names, db_catalog->table_names);
	
    char* table_name = strtok_r(tb_names, delimiter, &tokenizer);
    while(table_name != NULL) {
		table_catalog_path[0] = '\0';
		strcat(table_catalog_path, db->base_directory);
		strcat(table_catalog_path, "/");
		strcat(table_catalog_path, table_name);
		strcat(table_catalog_path, ".tbl");
        if (unpersist_tbl(&(db->tables[table_i]), table_catalog_path) < 0) {
            return NULL;
        }
        table_name = strtok_r(NULL, delimiter, &tokenizer);
        table_i++;
    }

	free(tb_names);
    // Don't msync since we don't want to overwrite with strtok yet
    // rflag = msync(db_catalog, sizeof(PersistedDbCatalog), MS_SYNC);
    // if(rflag == -1)
    // {
    //     return NULL;
    // }
    if (munmap(db_catalog, sizeof(PersistedDbCatalog)) < 0) {
        return NULL;
    }

	print_db_data(db);
    return db;
}

int unpersist_tbl(Table* tbl, char* tbl_catalog_path) {
    PersistedTableCatalog* tbl_catalog = (PersistedTableCatalog*) mmap_to_read(tbl_catalog_path, sizeof(PersistedTableCatalog));
	strcpy(tbl->name, tbl_catalog->name);
    strcpy(tbl->base_directory, tbl_catalog->base_directory);
    tbl->col_count = tbl_catalog->col_count;
    tbl->columns_capacity = tbl_catalog->columns_capacity;
    tbl->table_length = tbl_catalog->table_length;
	tbl->table_capacity = tbl_catalog->table_capacity;
    tbl->columns = (Column*) malloc(sizeof(Column) * tbl->col_count);

    char *tokenizer = NULL;
    const char delimiter[2] = ",";
    size_t col_i = 0;
	char* col_names = (char*) malloc((strlen(tbl_catalog->column_names) + 1) * sizeof(char));
	strcpy(col_names, tbl_catalog->column_names);

    char* col_name = strtok_r(col_names, delimiter, &tokenizer);
    while(col_name != NULL) {
        if (unpersist_col(&tbl->columns[col_i], tbl, col_name, tbl->table_length, tbl->table_capacity) < 0) {
			return -1;
        }
        col_name = strtok_r(NULL, delimiter, &tokenizer);
        col_i++;
    }
	free(col_names);
    if (munmap(tbl_catalog, sizeof(PersistedTableCatalog)) < 0) {
        return -1;
    }
    return 0;
}

int unpersist_col(Column* col, Table* table, char* column_name, size_t column_size, size_t column_capacity) {
	strcpy(col->name, column_name);
	col->size = column_size;
	col->capacity = column_capacity;
	col->table = table;

	printf("unpersisting %s with cap %zu\n", column_name, col->capacity);

	char data_path[MAX_PATH_NAME_SIZE] = "";
	get_column_data_path(data_path, table->base_directory, table->name, column_name);

	col->data = (int*) mmap_to_write(data_path, column_capacity * sizeof(int));

	char catalog_path[MAX_PATH_NAME_SIZE] = "";
	get_column_catalog_path(catalog_path, table->base_directory, table->name, column_name);
	PersistedColumnCatalog* col_catalog = (PersistedColumnCatalog*) mmap_to_read(catalog_path, sizeof(PersistedColumnCatalog));

	col->is_clustered = col_catalog->is_clustered;
	col->index = NULL;
	if (strlen(col_catalog->index_type) == 0) {
		printf("no index found for [%s]\n", column_name);
	} else {
		printf("index found for [%s][%s]\n", column_name, col_catalog->index_type);

		ColumnIndex* column_index = (ColumnIndex*) malloc(sizeof(ColumnIndex));
		if (strncmp(col_catalog->index_type, "SORTED", 6) == 0) {
			printf("sorted index found for [%s]\n", column_name);

			column_index->type = SORTED;
			column_index->index_pointer.sorted_index = create_sorted_index(
				table->base_directory,
				table->name,
				col->name,
				col->capacity
			);
		} else if (strncmp(col_catalog->index_type, "BTREE", 5) == 0) {
			column_index->type = BTREE;
			column_index->index_pointer.btree_index = create_btree_index(
				table->base_directory,
				table->name,
				col->name,
				col->capacity
			);

			column_index->index_pointer.btree_index->root_node = construct_btree(
				column_index->index_pointer.btree_index->data,
				column_index->index_pointer.btree_index->positions,
				col->size,
				BTREE_FANOUT,
				BTREE_PAGESIZE
			);
			column_index->index_pointer.btree_index->size = col->size;
		}
		col->index = column_index;
	}
	
	return 0;
}

int persist_db_catalog(Db* db) {
	char db_catalog_path[MAX_PATH_NAME_SIZE] = "";
	strcat(db_catalog_path, db->base_directory);
	strcat(db_catalog_path, "/");
	strcat(db_catalog_path, db->name);
	strcat(db_catalog_path, ".db\0");

	int rflag = -1;
	int fd = open(db_catalog_path, O_RDWR | O_CREAT, (mode_t)0600);

	if(fd == -1) {
		return -1;
	}

	rflag = lseek(fd, sizeof(PersistedDbCatalog) - 1, SEEK_SET);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	rflag = write(fd, "", 1);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	PersistedDbCatalog* db_catalog = (PersistedDbCatalog*) mmap(0, sizeof(PersistedDbCatalog), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);

	db_catalog->tables_size = db->tables_size;
	db_catalog->tables_capacity = db->tables_capacity;
	strcpy(db_catalog->name, db->name);
	strcpy(db_catalog->base_directory, db->base_directory);
	char* ptr_to_write = db_catalog->table_names;
	for (size_t i = 0; i < db->tables_size - db->tables_capacity; i++) {
		strncpy(ptr_to_write, db->tables[i].name, strlen(db->tables[i].name));
		ptr_to_write += strlen(db->tables[i].name);
		*ptr_to_write = ',';
		ptr_to_write++;
	}
	char* last_char = ptr_to_write - 1;
	*last_char = '\0';

    rflag = msync(db_catalog, sizeof(PersistedDbCatalog), MS_SYNC);
    if(rflag == -1)
    {
        return -1;
    }
    rflag = munmap(db_catalog, sizeof(PersistedDbCatalog));
    if(rflag == -1)
    {
        return -1;
    }

	return 0;
}

int persist_tbl_catalog(Table* tbl) {
	char tbl_catalog_path[MAX_PATH_NAME_SIZE] = "";
	get_catalog_path_from_table(tbl_catalog_path, tbl);

	int rflag = -1;
	int fd = open(tbl_catalog_path, O_RDWR | O_CREAT, (mode_t)0600);

	if(fd == -1) {
		return -1;
	}

	rflag = lseek(fd, sizeof(PersistedTableCatalog) - 1, SEEK_SET);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	rflag = write(fd, "", 1);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	PersistedTableCatalog* tbl_catalog = (PersistedTableCatalog*) mmap(0, sizeof(PersistedTableCatalog), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);

	printf("persisiting columns capacity %zu\n", tbl->columns_capacity);
	tbl_catalog->columns_capacity = tbl->columns_capacity;
	tbl_catalog->col_count = tbl->col_count;
	tbl_catalog->table_length = tbl->table_length;
	tbl_catalog->table_capacity = tbl->table_capacity;
	tbl_catalog->clustered_index_id = tbl->clustered_index_id;
	tbl_catalog->has_clustered_index = tbl->has_clustered_index;
	strncpy(tbl_catalog->name, tbl->name, MAX_SIZE_NAME);
    strncpy(tbl_catalog->base_directory, tbl->base_directory, MAX_SIZE_NAME);
	char* ptr_to_write = tbl_catalog->column_names;
	for (size_t i = 0; i < tbl->col_count - tbl->columns_capacity; i++) {
		strncpy(ptr_to_write, tbl->columns[i].name, strlen(tbl->columns[i].name));
		ptr_to_write += strlen(tbl->columns[i].name);
		*ptr_to_write = ',';
		ptr_to_write++;
	}
	char* last_char = ptr_to_write - 1;
	*last_char = '\0';

    rflag = msync(tbl_catalog, sizeof(PersistedTableCatalog), MS_SYNC);
    if(rflag == -1)
    {
        return -1;
    }
    rflag = munmap(tbl_catalog, sizeof(PersistedTableCatalog));
    if(rflag == -1)
    {
        return -1;
    }

	return 0;
}

int persist_col_catalog(Column* col) {
	char catalog_path[MAX_PATH_NAME_SIZE] = "";
	get_catalog_path_from_column(catalog_path, col);
	printf("catalog_path[%s]\n", catalog_path);

	PersistedColumnCatalog* col_catalog = (PersistedColumnCatalog*) mmap_to_write(catalog_path, sizeof(PersistedColumnCatalog));

	strcpy(col_catalog->name, col->name);
	col_catalog->is_clustered = col->is_clustered;
	if (col->index != NULL) {
		if (col->index->type == SORTED) {
			strcpy(col_catalog->index_type, "SORTED");
		} else {
			strcpy(col_catalog->index_type, "BTREE");
		}
	} else {
		col_catalog->index_type[0] = '\0';
	}

    int rflag = msync(col_catalog, sizeof(PersistedColumnCatalog), MS_SYNC);
    if(rflag == -1)
    {
        return -1;
    }
    rflag = munmap(col_catalog, sizeof(PersistedColumnCatalog));
    if(rflag == -1)
    {
        return -1;
    }

	return 0;
}

void print_tbl_data(Table* tbl, int preview_count) {
    printf("Table: %s [%zu rows]\n", tbl->name, tbl->table_length);
    Result posn_vec;
    posn_vec.num_tuples = preview_count;
    posn_vec.data_type = INT;
	int* payload = (int*) malloc(posn_vec.num_tuples * sizeof(int));
    posn_vec.payload = (void*) payload;
	for(int i = 0; i < preview_count; i++) {
		payload[i] = i;
	}
	struct GeneralizedColumn posn_vec_gcolumn;
	union GeneralizedColumnPointer posn_vec_gcolumn_pointer;
	posn_vec_gcolumn_pointer.result = &posn_vec;
	posn_vec_gcolumn.column_type = RESULT;
	posn_vec_gcolumn.column_pointer = posn_vec_gcolumn_pointer;

    GeneralizedColumn** fetch_gcolumns = (GeneralizedColumn**) malloc(tbl->col_count * sizeof(GeneralizedColumn*));

    for (size_t i = 0; i < tbl->col_count; i++) {
        Status ret_status;
		fetch_gcolumns[i] = (GeneralizedColumn*) malloc(sizeof(GeneralizedColumn));
        fetch_gcolumns[i]->column_type = RESULT;
        fetch_gcolumns[i]->column_pointer.result = execute_fetch_operator(&(tbl->columns[i]), &posn_vec_gcolumn, &ret_status);
	}

    PrintOperator print_operator;
    print_operator.generalized_columns = fetch_gcolumns;
    print_operator.generalized_columns_count = tbl->col_count;
    char* result_print = execute_print_operator(&print_operator);
	printf("%s\n", result_print);
	free(result_print);
    free(posn_vec.payload);
    for (size_t i =0; i < tbl->col_count; i++) {
		free(fetch_gcolumns[i]->column_pointer.result->payload);
        free(fetch_gcolumns[i]->column_pointer.result);
		free(fetch_gcolumns[i]);
    }
    free(fetch_gcolumns);
}

void print_db_data(Db* db) {
    printf("Database: %s\n", db->name);
    for(size_t i = 0; i < db->tables_size - db->tables_capacity; i++) {
		print_tbl_data(&(db->tables[i]), 4);
    }
}
