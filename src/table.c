#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>

#include <errno.h>

#include "cs165_api.h"
#include "utils.h"

void get_table_catalog_path(char* path, char* base_directory, char* name) {
    path[0] = '\0';
	strcat(path, base_directory);
	strcat(path, "/");
	strcat(path, name);
	strcat(path, ".tbl");
}

void get_catalog_path_from_table(char* path, Table* table) {
    get_table_catalog_path(path, table->base_directory, table->name);
}