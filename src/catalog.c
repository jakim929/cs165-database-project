#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>

#include <errno.h>

#include "cs165_api.h"
#include "utils.h"

int unpersist_tbl(Table* tbl, char* tbl_catalog_path);
int unpersist_col(Column* col, char* name, char* col_data_path);

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
    char db_catalog_path[MAX_PATH_NAME_SIZE];
    snprintf(db_catalog_path, MAX_PATH_NAME_SIZE, "%s/%s/%s.db", BASE_CATALOG_PATH, db_name, db_name);

    if (!check_directory_exists(db_catalog_path)) {
        return NULL;
    }

    PersistedDbCatalog* db_catalog = (PersistedDbCatalog*) mmap_file_for_read(db_catalog_path, sizeof(PersistedDbCatalog));

    struct Db* db = (Db*) malloc(sizeof(Db));
    db->tables_size = db_catalog->tables_size;
	db->tables_capacity = db_catalog->tables_capacity;
	strncpy(db->name, db_catalog->name, MAX_SIZE_NAME);
    strncpy(db->base_directory, db_catalog->base_directory, MAX_SIZE_NAME);

    db->tables = (Table*) malloc(sizeof(Table) * db->tables_size);

    const char delimiter[2] = ",";
    char *tokenizer;
    char table_catalog_path[MAX_PATH_NAME_SIZE];
    size_t table_i = 0;

	char* tb_names = (char*) malloc(strlen(db_catalog->table_names) * sizeof(char));
	strcpy(tb_names, db_catalog->table_names);
	
    char* table_name = strtok_r(db_catalog->table_names, delimiter, &tokenizer);
    while(table_name != NULL) {
        snprintf(table_catalog_path, MAX_PATH_NAME_SIZE, "%s/%s.tbl", db->base_directory, table_name);
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
    if (!check_directory_exists(tbl_catalog_path)) {
        return -1;
    }

    PersistedTableCatalog* tbl_catalog = (PersistedTableCatalog*) mmap_file_for_read(tbl_catalog_path, sizeof(PersistedTableCatalog));
    strncpy(tbl->name, tbl_catalog->name, MAX_SIZE_NAME);
    strncpy(tbl->base_directory, tbl_catalog->base_directory, MAX_PATH_NAME_SIZE);
    tbl->col_count = tbl_catalog->col_count;
    tbl->columns_capacity = tbl_catalog->columns_capacity;
    tbl->table_length = tbl_catalog->table_length;;
    tbl->columns = (Column*) malloc(sizeof(Column) * tbl->col_count);

    char *tokenizer = NULL;
    const char delimiter[2] = ",";
    char col_data_path[MAX_PATH_NAME_SIZE];
    size_t col_i = 0;
	char* col_names = (char*) malloc(strlen(tbl_catalog->column_names) * sizeof(char));
	strcpy(col_names, tbl_catalog->column_names);

    char* col_name = strtok_r(col_names, delimiter, &tokenizer);
    while(col_name != NULL) {
        snprintf(col_data_path, MAX_PATH_NAME_SIZE, "%s/%s.%s.data", tbl->base_directory, tbl->name, col_name);
        if (unpersist_col(&tbl->columns[col_i], col_name, col_data_path) < 0) {
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
int unpersist_col(Column* col, char* name, char* col_data_path) {
	strncpy(col->name, name, MAX_SIZE_NAME);
    strncpy(col->path, col_data_path, MAX_PATH_NAME_SIZE);
    
    int rflag = -1;
	int fd = open(col->path, O_RDWR | O_CREAT, (mode_t)0600);

	if(fd == -1) {
		return -1;
	}

	rflag = lseek(fd, (INITIAL_COLUMN_CAPACITY * sizeof(int)) - 1, SEEK_SET);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	rflag = write(fd, "", 1);

	if (rflag == -1) {
		close(fd);
		return -1;
	}

	col->data = (int*) mmap(0, (INITIAL_COLUMN_CAPACITY * sizeof(int)), PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
	close(fd);
	return 0;
}

int persist_db_catalog(Db* db) {
	char db_catalog_path[MAX_PATH_NAME_SIZE];
	snprintf(db_catalog_path, MAX_PATH_NAME_SIZE, "%s/%s.db", db->base_directory, db->name);

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
	strncpy(db_catalog->name, db->name, MAX_SIZE_NAME);
	strncpy(db_catalog->base_directory, db->base_directory, MAX_PATH_NAME_SIZE);
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
	char tbl_catalog_path[MAX_PATH_NAME_SIZE];
	snprintf(tbl_catalog_path, MAX_PATH_NAME_SIZE, "%s/%s.tbl", tbl->base_directory, tbl->name);

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
