#ifndef CS165_HASH_TABLE // This is a header guard. It prevents the header from being included more than once.
#define CS165_HASH_TABLE  

typedef int HT_KeyType;
typedef int HT_ValType;

typedef struct HT_Node {
    HT_KeyType key;
    HT_ValType val;
    struct HT_Node* next;
} HT_Node;

typedef struct HashTable {
// define the components of the hash table here (e.g. the array, bookkeeping for number of elements, etc)
    struct HT_Node** slots;
    int expected_size;
    int total_count;
    int slot_count;
} HashTable;

int ht_allocate(HashTable** ht, int size);
int ht_put(HashTable* ht, HT_KeyType key, HT_ValType value);
int ht_get(HashTable* ht, HT_KeyType key, HT_ValType *values, int num_values, int* num_results);
int ht_erase(HashTable* ht, HT_KeyType key);
int ht_deallocate(HashTable* ht);
int ht_reallocate(HashTable* ht, int size);
int ht_initialize_table(HashTable* ht, int size);
int ht_deallocate_slots(struct HT_Node** slots, int slot_count);
int ht_hash(HT_KeyType key);
int ht_get_slot_index(HashTable* ht, HT_KeyType key);
int ht_get_slot_count(int size);

#endif
