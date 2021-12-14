#ifndef PARSE_H__
#define PARSE_H__
#include "cs165_api.h"
#include "message.h"
#include "client_context.h"

DbOperator* parse_command(char* query_command, message* send_message, int client, ClientContext* context);

char* next_token(char** tokenizer, message_status* status);

#endif
