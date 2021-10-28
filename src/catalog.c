#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>

#include <errno.h>

#include "cs165_api.h"
#include "utils.h"

int unpersist_tbl(Table* tbl, char* tbl_catalog_path);
int unpersist_col(Column* col, char* name, char* col_data_path, size_t column_size, size_t column_capacity);
void print_db_data(Db* db);

void* mmap_file_for_read(char* path, size_t size) {
	int rflag = -1;
	int fd = open(path, O_RDWR, (mode_t)0600);
    if(fd == -1) {
		return NULL;
	}
	rflag = lseek(fd, size - 1, SEEK_SET);
	if (rflag == -1) {
		close(fd);
		return NULL;
	}
	rflag = write(fd, "", 1);
	if (rflag == -1) {
		close(fd);
		return NULL;
	}

    void* ptr = mmap(0, size, PROT_READ, MAP_SHARED, fd, 0);
	close(fd);
    return ptr;
}

Db* unpersist_db(char* db_name) {
    char db_catalog_path[MAX_PATH_NAME_SIZE] = "";
	strcat(db_catalog_path, BASE_CATALOG_PATH);
	strcat(db_catalog_path, "/");
	strcat(db_catalog_path, db_name);
	strcat(db_catalog_path, "/");
	strcat(db_catalog_path, db_name);
	strcat(db_catalog_path, ".db");

    PersistedDbCatalog* db_catalog = (PersistedDbCatalog*) mmap_file_for_read(db_catalog_path, sizeof(PersistedDbCatalog));

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
	
    char* table_name = strtok_r(db_catalog->table_names, delimiter, &tokenizer);
    while(table_name != NULL) {
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
    PersistedTableCatalog* tbl_catalog = (PersistedTableCatalog*) mmap_file_for_read(tbl_catalog_path, sizeof(PersistedTableCatalog));
	strcpy(tbl->name, tbl_catalog->name);
    strcpy(tbl->base_directory, tbl_catalog->base_directory);
    tbl->col_count = tbl_catalog->col_count;
    tbl->columns_capacity = tbl_catalog->columns_capacity;
    tbl->table_length = tbl_catalog->table_length;
	tbl->table_capacity = tbl_catalog->table_capacity;
    tbl->columns = (Column*) malloc(sizeof(Column) * tbl->col_count);

    char *tokenizer = NULL;
    const char delimiter[2] = ",";
    char col_data_path[MAX_PATH_NAME_SIZE] = "";
    size_t col_i = 0;
	char* col_names = (char*) malloc((strlen(tbl_catalog->column_names) + 1) * sizeof(char));
	strcpy(col_names, tbl_catalog->column_names);

    char* col_name = strtok_r(col_names, delimiter, &tokenizer);
    while(col_name != NULL) {
		col_data_path[0] = '\0';
		strcat(col_data_path, tbl->base_directory);
		strcat(col_data_path, "/");
		strcat(col_data_path, tbl->name);
		strcat(col_data_path, ".");
		strcat(col_data_path, col_name);
		strcat(col_data_path, ".data");
        if (unpersist_col(&tbl->columns[col_i], col_name, col_data_path, tbl->table_length, tbl->table_capacity) < 0) {
			return -1;
        }
        col_name = strtok_r(NULL, delimiter, &tokenizer);
        col_i++;
    }
    if (munmap(tbl_catalog, sizeof(PersistedTableCatalog)) < 0) {
        return -1;
    }
    return 0;
}

// pass initial length later
int unpersist_col(Column* col, char* name, char* col_data_path, size_t column_size, size_t column_capacity) {
	strcpy(col->name, name);
    strcpy(col->path, col_data_path);
    
    int rflag = -1;
	int fd = open(col->path, O_RDWR | O_CREAT, (mode_t)0600);

	if(fd == -1) {
		return -1;
	}

	rflag = lseek(fd, (column_capacity * sizeof(int)) - 1, SEEK_SET);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	rflag = write(fd, "", 1);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	col->data = (int*) mmap(0, (column_capacity * sizeof(int)), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	col->size = column_size;
	col->capacity = column_capacity;
	close(fd);
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
	strcat(tbl_catalog_path, tbl->base_directory);
	strcat(tbl_catalog_path, "/");
	strcat(tbl_catalog_path, tbl->name);
	strcat(tbl_catalog_path, ".tbl");

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

	tbl_catalog->columns_capacity = tbl->columns_capacity;
	tbl_catalog->col_count = tbl->col_count;
	tbl_catalog->table_length = tbl->table_length;
	tbl_catalog->table_capacity = tbl->table_capacity;
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

    GeneralizedColumn** fetch_gcolumns = (GeneralizedColumn**) malloc(tbl->col_count * sizeof(GeneralizedColumn*));

    for (size_t i = 0; i < tbl->col_count; i++) {
        Status ret_status;
		fetch_gcolumns[i] = (GeneralizedColumn*) malloc(sizeof(GeneralizedColumn));
        fetch_gcolumns[i]->column_type = RESULT;
        fetch_gcolumns[i]->column_pointer.result = fetch(&(tbl->columns[i]), &posn_vec, &ret_status);
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
