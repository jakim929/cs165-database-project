#ifndef MMAP_HELPER_H
#define MMAP_HELPER_H
#include <ctype.h>
#include <stddef.h>

void* mmap_to_write(char* path, size_t capacity);

void* mmap_to_read(char* path, size_t size);

void* resize_mmap_write_capacity(void* data, char* data_path, size_t prev_size, size_t new_size);

#endif
