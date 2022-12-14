#ifndef CLIENT_CONTEXT_H
#define CLIENT_CONTEXT_H

#include "cs165_api.h"

ClientContext* initialize_client_context();

int add_result_to_client_context(ClientContext* client_context, Result* result, char* handle);

int add_placeholder_gcolumn_to_client_context(ClientContext* client_context, char* handle);

int add_generalized_column_to_client_context(ClientContext* client_context, GeneralizedColumn* gen_column, char* handle);

GeneralizedColumn* lookup_gcolumn_by_handle(ClientContext* client_context, char* handle);

GeneralizedColumnHandle* lookup_gchandle_by_handle(ClientContext* client_context, char* handle);

int free_client_context(ClientContext* client_context);

int free_generalized_column_handle(GeneralizedColumnHandle* gen_chandle);

int free_generalized_column(GeneralizedColumn* gcolumn);

Table* lookup_table(char *name);

Column* lookup_column(char *name);

Db* lookup_db(char* name);

void lookup_table_and_column(Table** table, Column** column, char* name);

int start_batch_query(ClientContext* client_context);

int end_batch_query(ClientContext* client_context);

int start_load(ClientContext* client_context, LoadOperator* load_operator);

int add_to_load_operator(ClientContext* client_context, char* payload);

int end_load(ClientContext* client_context);

#endif
