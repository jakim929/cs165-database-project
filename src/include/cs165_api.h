

/* BREAK APART THIS API (TODO MYSELF) */
/* PLEASE UPPERCASE ALL THE STUCTS */

/*
Copyright (c) 2015 Harvard University - Data Systems Laboratory (DASLab)
Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in
the Software without restriction, including without limitation the rights to
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies
of the Software, and to permit persons to whom the Software is furnished to do
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

#ifndef CS165_H
#define CS165_H

#include <stdlib.h>
#include <stdbool.h>
#include <stdio.h>
#include <string.h>

#define BASE_CATALOG_PATH "catalog"

// Limits the size of a name in our database to 64 characters
#define MAX_SIZE_NAME 64
#define HANDLE_MAX_SIZE 64
#define MAX_PATH_NAME_SIZE 256

#define INITIAL_CHANDLE_CAPACITY 64

#define INITIAL_TABLES_SIZE 32
#define INITIAL_COLUMN_CAPACITY 1024

#define INITIAL_PRINT_OPERATOR_COLUMNS_CAPACITY 32
#define INITIAL_PRINT_OPERATOR_BUFFER_SIZE 8192

/**
 * EXTRA
 * DataType
 * Flag to mark what type of data is held in the struct.
 * You can support additional types by including this enum and using void*
 * in place of int* in db_operator simliar to the way IndexType supports
 * additional types.
 **/

typedef enum DataType {
     INT,
     LONG,
     FLOAT
} DataType;

struct Comparator;
//struct ColumnIndex;

typedef struct Column {
    char name[MAX_SIZE_NAME]; 
    char path[MAX_PATH_NAME_SIZE];
    int* data;
    // You will implement column indexes later. 
    void* index;
    size_t size;
    size_t capacity;
    //struct ColumnIndex *index;
    //bool clustered;
} Column;


/**
 * table
 * Defines a table structure, which is composed of multiple columns.
 * We do not require you to dynamically manage the size of your tables,
 * although you are free to append to the struct if you would like to (i.e.,
 * include a size_t table_size).
 * name, the name associated with the table. table names must be unique
 *     within a database, but tables from different databases can have the same
 *     name.
 * - col_count, the number of columns in the table
 * - columns this is the pointer to an array of columns contained in the table.
 * - table_length, the size of the columns in the table.
 **/

typedef struct Table {
    char name [MAX_SIZE_NAME];
    char base_directory [MAX_PATH_NAME_SIZE];
    Column *columns;
    size_t columns_capacity;
    size_t col_count;
    size_t table_length;
    size_t table_capacity;
} Table;

typedef struct PersistedTableCatalog {
    char name [MAX_SIZE_NAME];
    char base_directory [MAX_PATH_NAME_SIZE];
    size_t columns_capacity;
    size_t col_count;
    size_t table_length;
    size_t table_capacity;
    char column_names[MAX_SIZE_NAME * 256];
} PersistedTableCatalog;

/**
 * db
 * Defines a database structure, which is composed of multiple tables.
 * - name: the name of the associated database.
 * - tables: the pointer to the array of tables contained in the db.
 * - tables_size: the size of the array holding table objects
 * - tables_capacity: the amount of pointers that can be held in the currently allocated memory slot
 **/

typedef struct Db {
    char name[MAX_SIZE_NAME];
    char base_directory [MAX_PATH_NAME_SIZE];
    Table *tables;
    size_t tables_size;
    size_t tables_capacity;
} Db;

typedef struct PersistedDbCatalog {
    char name[MAX_SIZE_NAME];
    char base_directory [MAX_PATH_NAME_SIZE];
    size_t tables_size;
    size_t tables_capacity;
    char table_names[MAX_SIZE_NAME * 256];
} PersistedDbCatalog;

/**
 * Error codes used to indicate the outcome of an API call
 **/
typedef enum StatusCode {
  /* The operation completed successfully */
  OK,
  /* There was an error with the call. */
  ERROR,
} StatusCode;

// status declares an error code and associated message
typedef struct Status {
    StatusCode code;
    char* error_message;
} Status;

// Defines a comparator flag between two values.
typedef enum ComparatorType {
    NO_COMPARISON = 0,
    LESS_THAN = 1,
    GREATER_THAN = 2,
    EQUAL = 4,
    LESS_THAN_OR_EQUAL = 5,
    GREATER_THAN_OR_EQUAL = 6
} ComparatorType;

/*
 * Declares the type of a result column, 
 which includes the number of tuples in the result, the data type of the result, and a pointer to the result data
 */
typedef struct Result {
    size_t num_tuples;
    DataType data_type;
    void *payload;
} Result;

/*
 * an enum which allows us to differentiate between columns and results
 */
typedef enum GeneralizedColumnType {
    RESULT,
    COLUMN
} GeneralizedColumnType;
/*
 * a union type holding either a column or a result struct
 */
typedef union GeneralizedColumnPointer {
    Result* result;
    Column* column;
} GeneralizedColumnPointer;

/*
 * unifying type holding either a column or a result
 */
typedef struct GeneralizedColumn {
    GeneralizedColumnType column_type;
    GeneralizedColumnPointer column_pointer;
} GeneralizedColumn;

/*
 * used to refer to a column in our client context
 */

typedef struct GeneralizedColumnHandle {
    char name[HANDLE_MAX_SIZE];
    GeneralizedColumn generalized_column;
} GeneralizedColumnHandle;
/*
 * holds the information necessary to refer to generalized columns (results or columns)
 */
typedef struct ClientContext {
    GeneralizedColumnHandle* chandle_table;
    int chandles_in_use;
    int chandle_slots;
} ClientContext;

/**
 * comparator
 * A comparator defines a comparison operation over a column. 
 **/
typedef struct Comparator {
    long int p_low; // used in equality and ranges.
    long int p_high; // used in range compares. 
    GeneralizedColumn* gen_col;
    ComparatorType type1;
    ComparatorType type2;
    char* handle;
} Comparator;

/*
 * tells the databaase what type of operator this is
 */
typedef enum OperatorType {
    CREATE,
    INSERT,
    LOAD,
    SELECT,
    FETCH,
    PRINT,
    AVERAGE,
    SUM,
    MAX,
    MIN,
    ADD,
    SUB,
    SHUTDOWN,
} OperatorType;


typedef enum CreateType {
    _DB,
    _TABLE,
    _COLUMN,
} CreateType;

/*
 * necessary fields for creation
 * "create_type" indicates what kind of object you are creating. 
 * For example, if create_type == _DB, the operator should create a db named <<name>> 
 * if create_type = _TABLE, the operator should create a table named <<name>> with <<col_count>> columns within db <<db>>
 * if create_type = = _COLUMN, the operator should create a column named <<name>> within table <<table>>
 */
typedef struct CreateOperator {
    CreateType create_type; 
    char name[MAX_SIZE_NAME]; 
    Db* db;
    Table* table;
    int col_count;
} CreateOperator;

/*
 * necessary fields for insertion
 */
typedef struct InsertOperator {
    Table* table;
    int* values;
} InsertOperator;
/*
 * necessary fields for insertion
 */
typedef struct LoadOperator {
    // char* file_name;
    Table* table;
    char* load_data;
    // int** load_columns;
    // int num_rows;
} LoadOperator;
/*
 * necessary fields for select operator
 */

typedef struct NullableInt {
    bool is_null;
    int value;
} NullableInt;

typedef struct SelectOperator {
    Table* table;
    Column* column;
    NullableInt range_start;
    NullableInt range_end;
} SelectOperator;

typedef struct FetchOperator {
    Column* column;
    Result* posn_vec;
} FetchOperator;

typedef struct AverageOperator {
    Result* val_vec;
} AverageOperator;

typedef struct SumOperator {
    Result* val_vec;
} SumOperator;

typedef struct MaxOperator {
    Result* val_vec;
} MaxOperator;

typedef struct MinOperator {
    Result* val_vec;
} MinOperator;

typedef struct AddOperator {
    Result* val_vec1;
    Result* val_vec2;
} AddOperator;

typedef struct SubOperator {
    Result* val_vec1;
    Result* val_vec2;
} SubOperator;

typedef struct PrintOperator {
    GeneralizedColumn** generalized_columns;
    int generalized_columns_count;
} PrintOperator;

/*
 * union type holding the fields of any operator
 */
typedef union OperatorFields {
    CreateOperator create_operator;
    InsertOperator insert_operator;
    LoadOperator load_operator;
    SelectOperator select_operator;
    PrintOperator print_operator;
    FetchOperator fetch_operator;
    AverageOperator average_operator;
    SumOperator sum_operator;
    MaxOperator max_operator;
    MinOperator min_operator;
    AddOperator add_operator;
    SubOperator sub_operator;
} OperatorFields;
/*
 * DbOperator holds the following fields:
 * type: the type of operator to perform (i.e. insert, select, ...)
 * operator fields: the fields of the operator in question
 * client_fd: the file descriptor of the client that this operator will return to
 * context: the context of the operator in question. This context holds the local results of the client in question.
 */
typedef struct DbOperator {
    OperatorType type;
    OperatorFields operator_fields;
    int client_fd;
    ClientContext* context;
    char* handle;
} DbOperator;

extern Db *current_db;

/* 
 * Use this command to see if databases that were persisted start up properly. If files
 * don't load as expected, this can return an error. 
 */
Status db_startup();

Status create_db(const char* db_name);

Table* create_table(Db* db, const char* name, size_t num_columns, Status *status);

Column* create_column(Table *table, char *name, bool sorted, Status *ret_status);

void insert_row(Table *table, int* values, Status *ret_status);

Result* select_from_column(Column* column, NullableInt* range_start, NullableInt* range_end, Status* select_status);

Result* fetch(Column* val_vec, Result* posn_vec, Status* ret_status);

int free_db(Db* db);

Status shutdown_server();

char* execute_db_operator(DbOperator* query);

char* execute_print_operator(PrintOperator* print_operator);

int free_db_operator(DbOperator* dbo);

#endif /* CS165_H */

