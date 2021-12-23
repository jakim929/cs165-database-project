#ifndef CS165_HASH_PARTITION // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_PARTITION  

#include <stdbool.h>

typedef struct HP_Node {
    int key;
    int val;
} HP_Node;

typedef struct HP_Slot {
    int capacity;
    int size;
    int* keys;
    int* values;
} HP_Slot;

typedef struct HashPartition {
    HP_Slot* slots;
    int slot_count;
} HashPartition;

HashPartition* hp_initialize(int data_size, int slot_count);
int hp_put(HashPartition* hp, int key, int val);
int hp_free(HashPartition* hp);

#endif
