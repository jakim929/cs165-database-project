// utils.h
// CS165 Fall 2015
//
// Provides utility and helper functions that may be useful throughout.
// Includes debugging tools.

#ifndef __UTILS_H__
#define __UTILS_H__

#include <stdbool.h>
#include <stdarg.h>
#include <stdio.h>

#include "cs165_api.h"

/**
 * trims newline characters from a string (in place)
 **/

char* trim_newline(char *str);

/**
 * trims parenthesis characters from a string (in place)
 **/

char* trim_parenthesis(char *str);

/**
 * trims whitespace characters from a string (in place)
 **/

char* trim_whitespace(char *str);

char* trim_whitespace_retain_new_line(char *str);

/**
 * trims quotations characters from a string (in place)
 **/

char* trim_quotes(char *str);

// cs165_log(out, format, ...)
// Writes the string from @format to the @out pointer, extendable for
// additional parameters.
//
// Usage: cs165_log(stderr, "%s: error at line: %d", __func__, __LINE__);
void cs165_log(FILE* out, const char *format, ...);

// log_err(format, ...)
// Writes the string from @format to stderr, extendable for
// additional parameters. Like cs165_log, but specifically to stderr.
//
// Usage: log_err("%s: error at line: %d", __func__, __LINE__);
void log_err(const char *format, ...);

// log_info(format, ...)
// Writes the string from @format to stdout, extendable for
// additional parameters. Like cs165_log, but specifically to stdout.
// Only use this when appropriate (e.g., denoting a specific checkpoint),
// else defer to using printf.
//
// Usage: log_info("Command received: %s", command_string);
void log_info(const char *format, ...);

void parse_nullable_int(NullableInt* nullable_int, char* str);

bool check_directory_exists(const char* pathname);

size_t read_file_to_buffer(char* buffer, const char* pathname);

int maybe_create_directory(const char* pathname);

void print_db_data(Db* db);

void print_tbl_data(Table* tbl);

void print_col_data(Column* col);

#endif /* __UTILS_H__ */
