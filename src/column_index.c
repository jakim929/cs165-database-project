#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>

#include <errno.h>

#include "cs165_api.h"
#include "catalog.h"
#include "utils.h"
#include "mmap_helper.h"
#include "column_index.h"


void get_sorted_index_path(char* data_path, char* position_path, char* base_directory, char* table_name, char* column_name) {
	char base_path[MAX_PATH_NAME_SIZE];
	base_path[0] = '\0';
	strcat(base_path, base_directory);
	strcat(base_path, "/");
	strcat(base_path, table_name);
	strcat(base_path, ".");
	strcat(base_path, column_name);
	strcat(base_path, ".sorted_index_");

	data_path[0] = '\0';
	strcat(data_path, base_path);
	strcat(data_path, "data.data");

	position_path[0] = '\0';
	strcat(position_path, base_path);
	strcat(position_path, "position.data");
}

// TODO create resizing function for the array sizes
SortedIndex* create_sorted_index(char* base_directory, char* table_name, char* column_name, size_t capacity) {
	SortedIndex* sorted_index = (SortedIndex*) malloc(sizeof(SortedIndex));

	get_sorted_index_path(sorted_index->data_path, sorted_index->position_path, base_directory, table_name, column_name);

	sorted_index->data = mmap_to_write(sorted_index->data_path, capacity * sizeof(int));
	sorted_index->positions = mmap_to_write(sorted_index->position_path, capacity * sizeof(int));

	return sorted_index;
}

void resize_column_index(ColumnIndex* index, size_t prev_capacity, size_t new_capacity) {
	printf("STARTING resize_column_index to %zu", new_capacity);
	if (index->type == SORTED) {
		printf("RESIZING SORTED_INDEX to %zu", new_capacity);
		index->index_pointer.sorted_index->positions = (int*) resize_mmap_write_capacity(
			index->index_pointer.sorted_index->positions,
			index->index_pointer.sorted_index->position_path,
			prev_capacity * sizeof(int),
			new_capacity * sizeof(int)
		);
		index->index_pointer.sorted_index->data = (int*) resize_mmap_write_capacity(
			index->index_pointer.sorted_index->data,
			index->index_pointer.sorted_index->data_path,
			prev_capacity * sizeof(int),
			new_capacity * sizeof(int)
		);
	} else {

	}
}

void create_column_index(CreateIndexOperator* create_index_operator) {
	ColumnIndex* index = (ColumnIndex*) malloc(sizeof(ColumnIndex));
	index->type = create_index_operator->type;

	if (index->type == BTREE) {
		BTreeIndex* btree_index = (BTreeIndex*) malloc(sizeof(BTreeIndex));
		index->index_pointer.btree_index = btree_index;
	} else {
		index->index_pointer.sorted_index = create_sorted_index(
			create_index_operator->table->base_directory,
			create_index_operator->table->name,
			create_index_operator->column->name,
			create_index_operator->column->capacity
		);
	}
	create_index_operator->column->index = index;
	create_index_operator->column->is_clustered = create_index_operator->is_clustered;

	if (create_index_operator->is_clustered) {
		size_t column_id = 0;
        for (size_t i = 0; i < create_index_operator->table->col_count; i++) {
            if (&create_index_operator->table->columns[i] == create_index_operator->column) {
                column_id = i;
                break;
            }
        }
		create_index_operator->table->has_clustered_index = true;
		create_index_operator->table->clustered_index_id = column_id;
	}
}
