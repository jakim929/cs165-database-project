#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>

#include <errno.h>

#include "cs165_api.h"
#include "utils.h"

void get_column_base_path(char* path, char* base_directory, char* table_name, char* column_name) {
	path[0] = '\0';
	strcat(path, base_directory);
	strcat(path, "/");
	strcat(path , table_name);
	strcat(path, ".");
	strcat(path, column_name);
}

void get_column_catalog_path(char* path, char* base_directory, char* table_name, char* column_name) {
	get_column_base_path(path, base_directory, table_name, column_name);
	strcat(path, ".col");
}

void get_column_data_path(char* path, char* base_directory, char* table_name, char* column_name) {
	get_column_base_path(path, base_directory, table_name, column_name);
	strcat(path, ".data");
}

void get_catalog_path_from_column(char* path, Column* column) {
	get_column_catalog_path(path, column->table->base_directory, column->table->name, column->name);
}

void get_data_path_from_column(char* path, Column* column) {
	get_column_data_path(path, column->table->base_directory, column->table->name, column->name);
}


