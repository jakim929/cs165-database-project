#ifndef TABLE_H
#define TABLE_H

#include "cs165_api.h"

void get_table_catalog_path(char* path, char* base_directory, char* name);

void get_catalog_path_from_table(char* path, Table* table);

#endif
