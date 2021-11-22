/* This line at the top is necessary for compilation on the lab machine and many other Unix machines.
Please look up _XOPEN_SOURCE for more details. As well, if your code does not compile on the lab
machine please look into this as a a source of error. */
#define _XOPEN_SOURCE

/**
 * client.c
 *  CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a client
 * used in an interactive client-server database.
 * The client receives input from stdin and sends it to the server.
 * No pre-processing is done on the client-side.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdio.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>

#include "common.h"
#include "message.h"
#include "utils.h"

#define DEFAULT_STDIN_BUFFER_SIZE 1024
#define MAX_FILE_LINE_SIZE 1024
#define MAX_LOAD_PAYLOAD_SIZE 16384

/**
 * connect_client()
 *
 * This sets up the connection on the client side using unix sockets.
 * Returns a valid client socket fd on success, else -1 on failure.
 *
 **/
int connect_client() {
    int client_socket;
    size_t len;
    struct sockaddr_un remote;

    log_info("-- Attempting to connect...\n");

    if ((client_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    remote.sun_family = AF_UNIX;
    strncpy(remote.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    len = strlen(remote.sun_path) + sizeof(remote.sun_family) + 1;
    if (connect(client_socket, (struct sockaddr *)&remote, len) == -1) {
        log_err("client connect failed: ");
        return -1;
    }

    log_info("-- Client connected at socket: %d.\n", client_socket);
    return client_socket;
}

void send_message_to_server(int client_socket, message* send_message, message* recv_message) {
    int len = 0;
    // Send the message_header, which tells server payload size
    if (send(client_socket, send_message, sizeof(message), 0) == -1) {
        log_err("Failed to send message header.");
        exit(1);
    }

    // Send the payload (query) to server
    if (send(client_socket, send_message->payload, send_message->length, 0) == -1) {
        log_err("Failed to send query payload.");
        exit(1);
    }

    // Always wait for server response (even if it is just an OK message)
    if ((len = recv(client_socket, recv_message, sizeof(message), 0)) > 0) {
        if ((recv_message->status == OK_WAIT_FOR_RESPONSE || recv_message->status == OK_DONE) &&
            (int) recv_message->length > 0) {
            // Calculate number of bytes in response package
            int num_bytes = (int) recv_message->length;
            int written_bytes = 0;
            char payload[num_bytes + 1];
        
            while(num_bytes > written_bytes) {
                // Receive the payload and print it out
                if ((len = recv(client_socket, payload + written_bytes, num_bytes, 0)) > 0) {
                    written_bytes += len;
                }
            }
            payload[num_bytes] = '\0';
            printf("%s\n", payload);
        }
    }
    else {
        if (len < 0) {
            log_err("Failed to receive message.");
        }
        else {
            log_info("-- Server closed connection\n");
        }
        exit(1);
    }
}

/**
 * Getting Started Hint:
 *      What kind of protocol or structure will you use to deliver your results from the server to the client?
 *      What kind of protocol or structure will you use to interpret results for final display to the user?
 *      
**/
int main(void)
{
    int client_socket = connect_client();
    if (client_socket < 0) {
        exit(1);
    }

    message send_message;
    message recv_message;

    // Always output an interactive marker at the start of each command if the
    // input is from stdin. Do not output if piped in from file or from other fd
    char* prefix = "";
    if (isatty(fileno(stdin))) {
        prefix = "db_client > ";
    }

    char *output_str = NULL;

    // Continuously loop and wait for input. At each iteration:
    // 1. output interactive marker
    // 2. read from stdin until eof.
    char read_buffer[DEFAULT_STDIN_BUFFER_SIZE];
    send_message.payload = read_buffer;
    send_message.status = 0;

    while (printf("%s", prefix), output_str = fgets(read_buffer,
           DEFAULT_STDIN_BUFFER_SIZE, stdin), !feof(stdin)) {
        if (output_str == NULL) {
            log_err("fgets failed.\n");
            break;
        }

        // Only process input that is greater than 1 character.
        // Convert to message and send the message and the
        // payload directly to the server.
        send_message.length = strlen(read_buffer);
        if (send_message.length > 1) {
            if (strncmp(read_buffer, "load", 4) == 0) {
                char file_name[MAX_PATH_NAME_SIZE] = "";
                strcpy(file_name, trim_newline(trim_parenthesis(trim_quotes(read_buffer + 4))));

                FILE* fp = fopen(file_name, "r");
                if (fp == NULL) {
                    log_err("file [%s] not found.\n", file_name);
                    fclose(fp);
                    break;
                }

                size_t line_buffer_size = MAX_FILE_LINE_SIZE;
                char* line_buffer = (char*) malloc(sizeof(char) * line_buffer_size);
                
                if (getline(&line_buffer, &line_buffer_size, fp) == -1) {
                    log_err("file incorrect format.\n");
                    fclose(fp);
                    free(line_buffer);
                    break;
                }

                char load_payload[MAX_LOAD_PAYLOAD_SIZE] = "";
                load_payload[0] = '\0';

                // Send start_load(tbl_name) message
                strcat(load_payload, "start_load(");
                strcat(load_payload, line_buffer);
                load_payload[strlen(load_payload) - 1] = ')';
                send_message.payload = load_payload;
                send_message.length = strlen(load_payload) + 1;
                send_message_to_server(client_socket, &send_message, &recv_message);
                load_payload[0] = '\0';

                size_t payload_size = 0;
                ssize_t read_size;

                // ADD BUFFER PER LINE

                while ((read_size = getline(&line_buffer, &line_buffer_size, fp)) != -1) {
                    payload_size += read_size;
                    strcat(load_payload, line_buffer);

                    // Can't read another line into payload buffer. Send current payload
                    if (MAX_LOAD_PAYLOAD_SIZE - payload_size < line_buffer_size) {
                        send_message.payload = load_payload;
                        send_message.length = strlen(load_payload) + 1;
                        send_message_to_server(client_socket, &send_message, &recv_message);
                        
                        payload_size = 0;
                        load_payload[0] = '\0';
                    }
                }
                if (payload_size > 0) {
                    send_message.payload = load_payload;
                    send_message.length = strlen(load_payload) + 1;
                    send_message_to_server(client_socket, &send_message, &recv_message);
                }

                // Send end_load() message
                load_payload[0] = '\0';
                strcat(load_payload, "end_load()");
                send_message.payload = load_payload;
                send_message.length = strlen(load_payload) + 1;
                send_message_to_server(client_socket, &send_message, &recv_message);

                fclose(fp);
                free(line_buffer);
                // Return message buffer to original buffer;
                send_message.payload = read_buffer;
            } else {
                send_message_to_server(client_socket, &send_message, &recv_message);
            }
        }
    }
    close(client_socket);
    return 0;
}
