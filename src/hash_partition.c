#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include <math.h>
#include "hash_partition.h"

// https://gist.github.com/badboy/6267743
int hash(int key) {
    int c2=0x27d4eb2d; // a prime or an odd constant
    key = (key ^ 61) ^ (key >> 16);
    key = key + (key << 3);
    key = key ^ (key >> 4);
    key = key * c2;
    key = key ^ (key >> 15);
    return key;
}

int get_slot_index(HashPartition* hp, int key) {
    return hash(key) % hp->slot_count;
}

HashPartition* hp_initialize(int data_size, int slot_count) {
    HashPartition* hp = (HashPartition*) malloc(sizeof(HashPartition));
    hp->slot_count = slot_count;
    hp->slots = (HP_Slot*) malloc(hp->slot_count * sizeof(HP_Slot));

    int initial_slot_capacity = data_size / slot_count;
    for (int i = 0; i < slot_count; i++) {
        hp->slots[i].capacity = initial_slot_capacity;
        hp->slots[i].size = 0;
        hp->slots[i].keys = (int*) malloc(sizeof(int) * hp->slots[i].capacity);
        hp->slots[i].values = (int*) malloc(sizeof(int) * hp->slots[i].capacity);
    }
    return hp;
}


int hp_put(HashPartition* hp, int key, int val) {
    int slot_index = get_slot_index(hp, key);
    HP_Slot* slot = &(hp->slots[slot_index]);
    if (slot->size == slot->capacity) {
        slot->capacity *= 2;
        slot->values = (int*) realloc(slot->values, sizeof(int) * slot->capacity);
        slot->keys = (int*) realloc(slot->keys, sizeof(int) * slot->capacity);
    }
    slot->keys[slot->size] = key;
    slot->values[slot->size++] = val;
    return 0;
}

int hp_free(HashPartition* hp) {
    for (int i = 0; i < hp->slot_count; i++) {
        free(hp->slots[i].keys);
        free(hp->slots[i].values);
    }
    free(hp->slots);
    free(hp);
    return 0;
}
