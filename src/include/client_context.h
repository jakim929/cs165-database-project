#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

Table* lookup_table(char *name);

void lookup_table_and_column(Table** table, Column** column, char* name);

#endif
