#include<unistd.h>
#include <string.h>
#include <sys/mman.h>
#include <stdio.h>
#include <fcntl.h>
#include <time.h>

#include <errno.h>

#include "cs165_api.h"
#include "catalog.h"
#include "utils.h"
#include "mmap_helper.h"
#include "column_index.h"

int total_nodes = 0;

size_t get_ceil(size_t val, size_t div) {
	return (val / div) + ((val % div) > 0 ? 1 : 0);
}

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

void get_btree_index_path(char* data_path, char* position_path, char* base_directory, char* table_name, char* column_name) {
	char base_path[MAX_PATH_NAME_SIZE];
	base_path[0] = '\0';
	strcat(base_path, base_directory);
	strcat(base_path, "/");
	strcat(base_path, table_name);
	strcat(base_path, ".");
	strcat(base_path, column_name);
	strcat(base_path, ".btree_index_");

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

BTreeIndex* create_btree_index(char* base_directory, char* table_name, char* column_name, size_t capacity) {
	BTreeIndex* btree_index = (BTreeIndex*) malloc(sizeof(BTreeIndex));

	get_btree_index_path(btree_index->data_path, btree_index->position_path, base_directory, table_name, column_name);

	btree_index->data = mmap_to_write(btree_index->data_path, capacity * sizeof(int));
	btree_index->positions = mmap_to_write(btree_index->position_path, capacity * sizeof(int));
	btree_index->size = 0;

	return btree_index;
}

void dedup(int* deduped_data, int* deduped_positions, size_t* deduped_data_size, int* sorted_data, int* positions, size_t size) {
	deduped_data[0] = sorted_data[0];
	int cur_id = 1;
	for (size_t i = 1; i < size; i++) {
		if (sorted_data[i] != deduped_data[cur_id - 1]) {
			deduped_data[cur_id] = sorted_data[i];
			deduped_positions[cur_id] = positions[i];
			cur_id++;
		}
	}
	*deduped_data_size = cur_id;
}

// dedup (including position)
// check if position is maintained (first instance is first if duplicate)
// then divided deduped chunk into pages
// then construct b-tree from that

void construct_leaf_nodes(BTreeNode*** leaf_nodes, size_t* leaf_node_count, int* deduped_data, int* sorted_data, int* deduped_data_positions, size_t deduped_data_count, int fanout, int pagesize) {
	size_t page_count = get_ceil(deduped_data_count, pagesize);
	*leaf_node_count = get_ceil(page_count, fanout);
	*leaf_nodes = (BTreeNode**) malloc(sizeof(BTreeNode*) * *leaf_node_count);
	for(size_t i = 0; i < *leaf_node_count; i++) {
		size_t pointers_count = i >= (page_count / fanout) ? (page_count % fanout) : (size_t) fanout;
		total_nodes++;
		(*leaf_nodes)[i] = (BTreeNode*) malloc(sizeof(BTreeNode));
		(*leaf_nodes)[i]->pointers = (void**) malloc(sizeof(void*) * pointers_count);
		(*leaf_nodes)[i]->pointers_count = pointers_count;
		(*leaf_nodes)[i]->values = (int*) malloc(sizeof(int) * pointers_count);
		(*leaf_nodes)[i]->type = LEAF;

		for(size_t j = 0; j < (size_t) pointers_count; j++) {
			size_t original_node_id = i * fanout + j;
			if (original_node_id < page_count) {
				(*leaf_nodes)[i]->values[j] = deduped_data[original_node_id * pagesize];
				(*leaf_nodes)[i]->pointers[j] = (void*) &sorted_data[deduped_data_positions[original_node_id * pagesize]];
				if ((*leaf_nodes)[i]->values[j] != (*(int*) ((*leaf_nodes)[i]->pointers[j]))) {
					// printf("WRONG!: %d, %d\n", (*leaf_nodes)[i]->values[j], (*(int*) ((*leaf_nodes)[i]->pointers[j])));
				}
			}
		}
 	}
}

void construct_inner_nodes(
	BTreeNode*** inner_nodes,
	size_t* inner_node_count,
	BTreeNode** nodes,
	size_t node_count,
	int fanout
) {
	*inner_node_count = get_ceil(node_count, fanout);
	*inner_nodes = (BTreeNode**) malloc(sizeof(BTreeNode*) * *inner_node_count);
	for(size_t i = 0; i < *inner_node_count; i++) {
		size_t pointers_count = i >= (node_count / fanout) ? (node_count % fanout) : (size_t) fanout;
		if (pointers_count == 1) {
			// printf("going for the last one %zu / %zu\n", i * fanout + pointers_count, node_count);
			(*inner_nodes)[i] = nodes[i * fanout + pointers_count - 1];
			// printf("nodes %d\n", (*inner_nodes)[i]->values[0]);
		} else {
			// printf("going for regular  one\n");
			total_nodes++;
			(*inner_nodes)[i] = (BTreeNode*) malloc(sizeof(BTreeNode));
			(*inner_nodes)[i]->pointers_count = pointers_count;
			(*inner_nodes)[i]->pointers = (void**) malloc(sizeof(void*) * pointers_count);
			(*inner_nodes)[i]->values = (int*) malloc(sizeof(int) * pointers_count);
			(*inner_nodes)[i]->type = INNER;

			for(size_t j = 0; j < (size_t) pointers_count; j++) {
				size_t original_node_id = i * fanout + j;
				(*inner_nodes)[i]->values[j] = nodes[original_node_id]->values[0];
				(*inner_nodes)[i]->pointers[j] = (void*) nodes[original_node_id];
			}
		}
 	}
}

BTreeNode* construct_btree(int* sorted_data, int* sorted_positions_on_original, size_t size, int fanout, int pagesize) {
	total_nodes = 0;
	int* deduped_data = (int*) malloc(sizeof(int*) * size);
	int* deduped_positions = (int*) malloc(sizeof(int*) * size);
	size_t deduped_data_size = 0;

	int* sorted_positions = (int*) malloc(sizeof(int*) * size);
	for (size_t i = 0; i < size; i++) {
		sorted_positions[i] = i;
	}
	
	dedup(deduped_data, deduped_positions, &deduped_data_size, sorted_data, sorted_positions, size);
	// printf("dedup result\n");
	// printf("deduped_data_size: %zu\n", deduped_data_size);


	size_t node_count;
	BTreeNode** nodes;

	construct_leaf_nodes(&nodes, &node_count, deduped_data, sorted_data, deduped_positions, deduped_data_size, fanout, pagesize);

	// printf("level 0\n");
	// for (size_t i = 0; i < node_count; i++) {
	// 	for (size_t j = 0; j < nodes[i]->pointers_count; j++) {
	// 		printf("%d,", nodes[i]->values[j]);
	// 	}
	// 	printf(" / ");
	// }
	// printf("\n");
	int level = 1;
	BTreeNode** temp_nodes;
	do {
		size_t temp_node_count;
		construct_inner_nodes(&temp_nodes, &temp_node_count, nodes, node_count, fanout);
		free(nodes);
		node_count = temp_node_count;
		nodes = temp_nodes;
		// printf("level %d\n", level);
		// for (size_t i = 0; i < node_count; i++) {
		// 	for (size_t j = 0; j < nodes[i]->pointers_count; j++) {
		// 		printf("%d,", nodes[i]->values[j]);
		// 	}
		// 	printf(" / ");
		// }
		// printf("\n");
		
		// node_count != 0 && printf("node_count %zu/%zu\n", temp_node_count, node_count);
		level++;
	} while(node_count != 1);

	BTreeNode* root_node = temp_nodes[0];
	free(temp_nodes);
	// printf("finished %d\n", root_node->values[0]);

	// Testing a search through
	// int needle = 716;
	// int* res = search_btree_index(root_node, needle);
	// if (res == NULL ) {
	// 	printf("key not found!\n");
	// } else {
	// 	printf("result! %d\n", *res);
	// }

	free(deduped_data);
	free(deduped_positions);

	// printf("TOTAL NODES: %d\n", total_nodes);
	return root_node;
}

int find_pointer_index_in_node(BTreeNode* node, int needle) {
	for(int i = node->pointers_count - 1; i >= 0; i--) {
		if (needle >= node->values[i]) {
			return i;
		}
	}
	return 0;
}

int* search_leaf_node_approx(BTreeNode* node, int needle) {
	int pointer_index = find_pointer_index_in_node(node, needle);
	int* pointer = (int*) node->pointers[pointer_index];
	return pointer;
}

int* search_leaf_node(BTreeNode* node, int needle) {
	int* pointer = search_leaf_node_approx(node, needle);
	if (pointer == NULL || *pointer != needle) {
		return NULL;
	}
	return pointer;
}

BTreeNode* search_inner_node(BTreeNode* node, int needle) {
	int pointer_index = find_pointer_index_in_node(node, needle);
	return (BTreeNode*) node->pointers[pointer_index];
}

int* search_btree_index(BTreeNode* root_node, int needle) {
	BTreeNode* node = root_node;
	while (node->type != LEAF) {
		node = search_inner_node(node, needle);
	}
	return search_leaf_node(node, needle);
}

int* search_btree_index_approx(BTreeNode* root_node, int needle) {
	BTreeNode* node = root_node;
	
	while (node->type != LEAF) {
		node = search_inner_node(node, needle);
	}
	int* result = search_leaf_node_approx(node, needle);
	return result;
}

int get_index_from_btree(BTreeIndex* btree_index, int* data_pointer) {
	ptrdiff_t diff = data_pointer - btree_index->data;
	return (int) diff;
}

int search_btree_index_ge(BTreeIndex* btree_index, int needle) {
	int* pointer = search_btree_index_approx(btree_index->root_node, needle);
	ptrdiff_t diff = pointer - btree_index->data;
	size_t size_left = btree_index->size - (size_t) diff;

    clock_t start, end;
    double cpu_time_used;
    start = clock();

	int total_scanned = 0;
	for (size_t i = 0; i < size_left; i++) {
		total_scanned++;
		if (*(pointer + i) >= needle) {
			return get_index_from_btree(btree_index, pointer + i);
		}
	}

	end = clock();
    cpu_time_used = ((double) (end - start)) / CLOCKS_PER_SEC;

	printf("btree_search post scan took %fms for [%d/%zu] at %d for needle %d \n", cpu_time_used, total_scanned, size_left, *(pointer), needle);

	return get_index_from_btree(btree_index, pointer + size_left);
}

// int* search_btree_pointer_lt(BTreeIndex* btree_index, int needle) {
// 	int* pointer = search_btree_index_approx(btree_index->root_node, needle);
// 	ptrdiff_t diff = pointer - btree_index->data;
// 	size_t size_left = btree_index->size - (size_t) diff;
// 	for (size_t i = 1; i < size_left; i++) {
// 		if (*(pointer + i) >= needle) {
// 			return pointer + i;
// 		}
// 	}
// 	return pointer + size_left;
// }

int* search_btree_pointer_eq(BTreeIndex* btree_index, int needle) {
	int* pointer = search_btree_index_approx(btree_index->root_node, needle);
	ptrdiff_t diff = pointer - btree_index->data;
	size_t size_left = btree_index->size - (size_t) diff;
	for (size_t i = 0; i < size_left; i++) {
		if (*(pointer + i) == needle) {
			return pointer + i;
		} else if (*(pointer + i) > needle) {
			return NULL;
		}
	}
	return NULL;
}


int search_btree_index_position(BTreeIndex* btree_index, int needle) {
	int* needle_pointer = search_btree_index(btree_index->root_node, needle);
	ptrdiff_t diff = needle_pointer - btree_index->data;
	return *(btree_index->positions + diff);
}

void btree_get_range_of(int* start, int* end, BTreeIndex* btree_index, size_t size, NullableInt* range_start, NullableInt* range_end) {
    *start = 0;
    *end = size - 1;
    if (!range_start->is_null) {
        *start = search_btree_index_ge(btree_index, range_start->value);
    }
    if (!range_end->is_null) {
        *end = search_btree_index_ge(btree_index, range_end->value) - 1;
    }
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
		printf("RESIZING BTREE_INDEX to %zu", new_capacity);
		index->index_pointer.btree_index->positions = (int*) resize_mmap_write_capacity(
			index->index_pointer.btree_index->positions,
			index->index_pointer.btree_index->position_path,
			prev_capacity * sizeof(int),
			new_capacity * sizeof(int)
		);
		index->index_pointer.btree_index->data = (int*) resize_mmap_write_capacity(
			index->index_pointer.btree_index->data,
			index->index_pointer.btree_index->data_path,
			prev_capacity * sizeof(int),
			new_capacity * sizeof(int)
		);
	}
}

void create_column_index(CreateIndexOperator* create_index_operator) {
	ColumnIndex* index = (ColumnIndex*) malloc(sizeof(ColumnIndex));
	index->type = create_index_operator->type;

	if (index->type == BTREE) {
		index->index_pointer.btree_index = create_btree_index(
			create_index_operator->table->base_directory,
			create_index_operator->table->name,
			create_index_operator->column->name,
			create_index_operator->column->capacity
		);
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
