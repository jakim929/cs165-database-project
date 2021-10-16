#include <string.h>

#include "catalog.h"
#include "client_context.h"
/* This is an example of a function you will need to
 * implement in your catalogue. It takes in a string (char *)
 * and outputs a pointer to a table object. Similar methods
 * will be needed for columns and databases. How you choose
 * to implement the method is up to you.
 * 
 */

Column* lookup_column_in_table(Table* tbl, char* column_name) {
	for (size_t i = 0; i < tbl->col_count - tbl->columns_capacity; i++) {
		if (strcmp(tbl->columns[i].name, column_name) == 0) {
			return &(tbl->columns[i]);
		}
	}
	return NULL;
}

Table* lookup_table_in_db(Db* db, char* table_name) {
	for (size_t i = 0; i < db->tables_size - db->tables_capacity; i++) {
		if (strcmp(db->tables[i].name, table_name) == 0) {
			return &(db->tables[i]);
		}
	}
	return NULL;
}

Db* lookup_db(char* name) {
	if (current_db && strcmp(current_db->name, name) == 0) {
		return current_db;
	}

	// add freeing existing db when switching to new one to avoid memory leak;
	current_db = unpersist_db(name);
	return current_db;
}

void lookup_table_and_column(Table** table, Column** column, char* name) {
	const char delimiter[2] = ".";
	char* db_name = strtok(name, delimiter);
    char* table_name = strtok(NULL, delimiter);
	char* column_name = strtok(NULL, delimiter);

	Db* db = lookup_db(db_name);
	*table = lookup_table_in_db(db, table_name);
	*column =  lookup_column_in_table(*table, column_name);

	printf("%s table loaded in lookup_table_and_column\n", (*table)->name);
	return;
}

// TODO: LOW PRI: Maybe update with hashtable implementation
Table* lookup_table(char *name) {
	const char delimiter[2] = ".";
    char* db_name = strtok(name, delimiter);
    char* table_name = strtok(NULL, delimiter);

	Db* db = lookup_db(db_name);
	return lookup_table_in_db(db, table_name);
}

// TODO: LOW PRI: Maybe update with hashtable implementation
Column* lookup_column(char *name) {
	const char delimiter[2] = ".";
	char* db_name = strtok(name, delimiter);
    char* table_name = strtok(NULL, delimiter);
	char* column_name = strtok(NULL, delimiter);

	Db* db = lookup_db(db_name);
	Table* tbl = lookup_table_in_db(db, table_name);

	return lookup_column_in_table(tbl, column_name);
}

/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
**/
