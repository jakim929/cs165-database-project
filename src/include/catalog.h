#ifndef CATALOG_H
#define CATALOG_H

#include "cs165_api.h"

Db* unpersist_db(char* db_name);
int unpersist_tbl(Table* tbl, char* tbl_catalog_path);
int unpersist_col(Column* col, Table* table, char* column_name, size_t column_size, size_t column_capacity);
int persist_db_catalog(Db* db);
int persist_tbl_catalog(Table* tbl);
int persist_col_catalog(Column* col);

#endif
