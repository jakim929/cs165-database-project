#include <stdlib.h>
#include <stdio.h>
#include "hash_table.h"

// Initialize the components of a hashtable.
// The size parameter is the expected number of elements to be inserted.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the parameter passed to the method is not null, if malloc fails, etc).
int allocate(hashtable** ht, int size) {
    *ht = (hashtable*) malloc(sizeof(hashtable));
    (*ht)->size = size;
    (*ht)->table = (struct node**) malloc(sizeof(struct node*) * size);
    return 0;
}

// This method inserts a key-value pair into the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if malloc is called and fails).
int put(hashtable* ht, keyType key, valType value) {
    int hashed_index = get_node_location(ht, key);
    struct node* curr = ht->table[hashed_index];
    struct node* new_node = malloc(sizeof(struct node));
    new_node->key = key;
    new_node->val = value;
    new_node->next = curr;

    ht->table[hashed_index] = new_node;
    return 0;
}

// This method retrieves entries with a matching key and stores the corresponding values in the
// values array. The size of the values array is given by the parameter
// num_values. If there are more matching entries than num_values, they are not
// stored in the values array to avoid a buffer overflow. The function returns
// the number of matching entries using the num_results pointer. If the value of num_results is greater than
// num_values, the caller can invoke this function again (with a larger buffer)
// to get values that it missed during the first call.
// This method returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int get(hashtable* ht, keyType key, valType *values, int num_values, int* num_results) {
    int hashed_index = get_node_location(ht, key);
    struct node* curr = (ht->table)[hashed_index];
    *num_results = 0;
    while(curr) {
        if (curr->key == key) {
            if (*num_results < num_values) {
                values[*num_results] = curr->val;
            }
            (*num_results)++;
        }
        curr = curr->next;
    }
    return 0;
}

// This method erases all key-value pairs with a given key from the hash table.
// It returns an error code, 0 for success and -1 otherwise (e.g., if the hashtable is not allocated).
int erase(hashtable* ht, keyType key) {
    if (!ht) {
        return -1;
    }
    int hashed_index = get_node_location(ht, key);
    struct node* curr_node = (ht->table)[hashed_index];
    struct node** prev_node_addr = &((ht->table)[hashed_index]);
    while(curr_node) {
        if (curr_node->key == key) {
            *prev_node_addr = curr_node->next;
            free(curr_node);
        } else {
            *prev_node_addr = curr_node;
        }
        curr_node = curr_node->next;
    }
    
    return 0;
}

// This method frees all memory occupied by the hash table.
// It returns an error code, 0 for success and -1 otherwise.
int deallocate(hashtable* ht) {
    for (int i = 0; i < ht->size; i++) {
        struct node* curr_node = (ht->table)[i];
        while(curr_node) {
            struct node* temp = curr_node->next;
            free(curr_node);
            curr_node = temp;
        }
    }
    free(ht->table);
    free(ht);
    return 0;
}

int get_node_location(hashtable* ht, keyType key) {
    return hash(key) % (ht->size);
}

// https://gist.github.com/badboy/6267743
int hash(keyType key) {
    int c2=0x27d4eb2d; // a prime or an odd constant
    key = (key ^ 61) ^ (key >> 16);
    key = key + (key << 3);
    key = key ^ (key >> 4);
    key = key * c2;
    key = key ^ (key >> 15);
    return key;
}
