#include <stdio.h>
#include <string.h>
#include <stddef.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "utils.h"
#include "cs165_api.h"

#define ANSI_COLOR_RED     "\x1b[31m"
#define ANSI_COLOR_GREEN   "\x1b[32m"
#define ANSI_COLOR_RESET   "\x1b[0m"

#define LOG 1
#define LOG_ERR 1
#define LOG_INFO 1

/* removes newline characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of newline characters.
 */ 
char* trim_newline(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '\r' || str[i] == '\n')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}
/* removes space characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of space characters.
 */ 
char* trim_whitespace(char *str)
{
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!isspace(str[i])) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

/* removes parenthesis characters from the input string.
 * Shifts characters over and shortens the length of
 * the string by the number of parenthesis characters.
 */ 
char* trim_parenthesis(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (!(str[i] == '(' || str[i] == ')')) {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}

char* trim_quotes(char *str) {
    int length = strlen(str);
    int current = 0;
    for (int i = 0; i < length; ++i) {
        if (str[i] != '\"') {
            str[current++] = str[i];
        }
    }

    // Write new null terminator
    str[current] = '\0';
    return str;
}
/* The following three functions will show output on the terminal
 * based off whether the corresponding level is defined.
 * To see log output, define LOG.
 * To see error output, define LOG_ERR.
 * To see info output, define LOG_INFO
 */
void cs165_log(FILE* out, const char *format, ...) {
#ifdef LOG
    va_list v;
    va_start(v, format);
    vfprintf(out, format, v);
    va_end(v);
#else
    (void) out;
    (void) format;
#endif
}

void log_err(const char *format, ...) {
#ifdef LOG_ERR
    va_list v;
    va_start(v, format);
    fprintf(stderr, ANSI_COLOR_RED);
    vfprintf(stderr, format, v);
    fprintf(stderr, ANSI_COLOR_RESET);
    va_end(v);
#else
    (void) format;
#endif
}

void log_info(const char *format, ...) {
#ifdef LOG_INFO
    va_list v;
    va_start(v, format);
    fprintf(stdout, ANSI_COLOR_GREEN);
    vfprintf(stdout, format, v);
    fprintf(stdout, ANSI_COLOR_RESET);
    fflush(stdout);
    va_end(v);
#else
    (void) format;
#endif
}

void parse_nullable_int(NullableInt* nullable_int, char* str) {
    if (strncmp(str, "null", 4) == 0) {
        nullable_int->is_null = true;
        return;
    }
    nullable_int->is_null = false;
    nullable_int->value = atoi(str);
    return;
}

bool check_directory_exists(const char* pathname) {
    bool found = true;
    struct stat* stat_result = malloc(sizeof(struct stat));
	if (stat(pathname, stat_result) == -1) {
		found = false;
	}
    free(stat_result);
    return found;
}

int maybe_create_directory(const char* pathname) {
    if (!check_directory_exists(pathname)) {
        mkdir(pathname, 0700);
    }
    return 1;
}

void print_db_data(Db* db) {
    printf("Database: %s\n", db->name);
    for(size_t i = 0; i < db->tables_size - db->tables_capacity; i++) {
        print_tbl_data(&(db->tables[i]));
    }
}

void print_tbl_data(Table* tbl) {
    printf("    Table: %s [%zu rows]\n", tbl->name, tbl->table_length);
    for(size_t i = 0; i < tbl->col_count - tbl->columns_capacity; i++) {
        print_col_data(&(tbl->columns[i]));
    }
}


// TODO: Print first few elements in column
void print_col_data(Column* col) {
    printf("        Column: %s\n", col->name);
    for(int i = 0; i < 3; i++) {
        printf("          [%d] %d \n", i, col->data[i]);
    }
}
