#ifndef SORTED_SEARCH_H
#define SORTED_SEARCH_H

#include "cs165_api.h"

int binary_search(int* data, int i, int j, int needle);
int binary_search_approximate(int* data, int i, int j, int needle);
int range_begin_of(int* data, int size, int needle);
int range_end_of(int* data, int size, int needle);
int first_instance_of(int* data, int size, int needle);
int last_instance_of(int* data, int size, int needle);
void get_range_of(int* start, int* end, int* data, int size, NullableInt* range_start, NullableInt* range_end);

#endif
