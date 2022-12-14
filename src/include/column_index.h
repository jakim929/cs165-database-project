#ifndef COLUMN_INDEX_H
#define COLUMN_INDEX_H

#include "cs165_api.h"

SortedIndex* create_sorted_index(char* base_directory, char* table_name, char* column_name, size_t capacity);

BTreeIndex* create_btree_index(char* base_directory, char* table_name, char* column_name, size_t capacity);

void resize_column_index(ColumnIndex* index, size_t prev_capacity, size_t new_capacity);

BTreeNode* construct_btree(int* sorted_data, int* sorted_positions_on_original, size_t size, int fanout, int pagesize);

int* search_btree_index(BTreeNode* root_node, int needle);

int search_btree_index_position(BTreeIndex* btree_index, int needle);

void btree_get_range_of(int* start, int* end, BTreeIndex* btree_index, size_t size, NullableInt* range_start, NullableInt* range_end);

#endif
