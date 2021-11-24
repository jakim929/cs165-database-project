#include "cs165_api.h"

// Returns -1 if didn't find
int binary_search(int* data, int i, int j, int needle) {
    int midpoint = (i + j) / 2;
    if (i == j) {
        if (needle == data[i]) {
            return i;
        } else {
            return -1;
        }
    }
    if (needle == data[midpoint]) {
        return midpoint;
    }
    if (needle > data[midpoint]) {
        return binary_search(data, midpoint + 1, j, needle);
    } else {
        return binary_search(data, i, midpoint - 1, needle);
    }
}

int binary_search_approximate(int* data, int i, int j, int needle) {
    int midpoint = (i + j) / 2;
    if (i == j) {
        return i;
    }
    if (needle == data[midpoint]) {
        return midpoint;
    }
    if (needle > data[midpoint]) {
        return binary_search_approximate(data, midpoint + 1, j, needle);
    } else {
        return binary_search_approximate(data, i, midpoint - 1, needle);
    }
}


// inclusive lower bound
int range_begin_of(int* data, int size, int needle) {
    int pos = binary_search_approximate(data, 0, size - 1, needle);
    for (int i = pos; i >= 1; i--) {
        if (data[i - 1] < needle) {
            return i;
        }
    }
    return 0;
}

// exclusive upper bound
int range_end_of(int* data, int size, int needle) {
    int pos = binary_search_approximate(data, 0, size - 1, needle + 1);
    for (int i = pos; i >= 0; i--) {
        if (data[i] < needle) {
            return i;
        }
    }
    return 0;
}

int first_instance_of(int* data, int size, int needle) {
    int pos = binary_search(data, 0, size - 1, needle);
    if (pos == -1) {
        return -1;
    }
    for (int i = pos; i >= 1; i--) {
        if (data[i - 1] != needle) {
            return i;
        }
    }
    return 0;
}

int last_instance_of(int* data, int size, int needle) {
    int pos = binary_search(data, 0, size - 1, needle);
    if (pos == -1) {
        return -1;
    }
    for (int i = pos; i < size - 1; i++) {
        if (data[i + 1] != needle) {
            return i;
        }
    }
    return size - 1;
}

void get_range_of(int* start, int* end, int* data, int size, NullableInt* range_start, NullableInt* range_end) {
    *start = 0;
    *end = size - 1;
    if (!range_start->is_null) {
        *start = range_begin_of(data, size, range_start->value);
    }
    if (!range_end->is_null) {
        *end = range_end_of(data, size, range_end->value);
    }
}
