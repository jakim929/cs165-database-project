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

// TODO: LOW PRI: Maybe update with hashtable implementation
Table* lookup_table(char *name) {
	const char delimiter[2] = ".";
    char* db_name = strtok(name, delimiter);
    char* table_name = strtok(NULL, delimiter);

	unpersist_db(db_name);

	if (!current_db || strcmp(current_db->name, db_name) != 0) {
		return NULL;
	}

	for (size_t i = 0; i < current_db->tables_size - current_db->tables_capacity; i++) {
		if (strcmp(current_db->tables[i].name, table_name) == 0) {
			return &(current_db->tables[i]);
		}
	}
	return NULL;
}

/**
*  Getting started hint:
* 		What other entities are context related (and contextual with respect to what scope in your design)?
* 		What else will you define in this file?
**/
