#ifndef CATALOG_H
#define CATALOG_H

#include "cs165_api.h"

Db* unpersist_db(char* db_name);
int unpersist_tbl(Table* tbl, char* tbl_catalog_path);
int unpersist_col(Column* col, char* name, char* col_data_path, size_t column_size);
int persist_db_catalog(Db* db);
int persist_tbl_catalog(Table* tbl);

#endif
