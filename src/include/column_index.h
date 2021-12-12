#ifndef COLUMN_INDEX_H
#define COLUMN_INDEX_H

#include "cs165_api.h"

SortedIndex* create_sorted_index(char* base_directory, char* table_name, char* column_name, size_t capacity);

BTreeIndex* create_btree_index(char* base_directory, char* table_name, char* column_name, size_t capacity);

void resize_column_index(ColumnIndex* index, size_t prev_capacity, size_t new_capacity);

BTreeNode* construct_btree(int* sorted_data, int* sorted_positions, size_t size);

int* search_btree_index(BTreeNode* root_node, int needle);

#endif
