#ifndef COLUMN_H
#define COLUMN_H

#include "cs165_api.h"

void get_column_base_path(char* path, char* base_directory, char* table_name, char* column_name);

void get_column_catalog_path(char* path, char* base_directory, char* table_name, char* column_name);

void get_column_data_path(char* path, char* base_directory, char* table_name, char* column_name);

void get_catalog_path_from_column(char* path, Column* column);

void get_data_path_from_column(char* path, Column* column);

#endif
