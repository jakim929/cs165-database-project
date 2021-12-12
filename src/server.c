/** server.c
 * CS165 Fall 2018
 *
 * This file provides a basic unix socket implementation for a server
 * used in an interactive client-server database.
 * The client should be able to send messages containing queries to the
 * server.  When the server receives a message, it must:
 * 1. Respond with a status based on the query (OK, UNKNOWN_QUERY, etc.)
 * 2. Process any appropriate queries, if applicable.
 * 3. Return the query response to the client.
 *
 * For more information on unix sockets, refer to:
 * http://beej.us/guide/bgipc/output/html/multipage/unixsock.html
 **/
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <string.h>
#include <dirent.h>
#include <time.h>

#include "common.h"
#include "parse.h"
#include "cs165_api.h"
#include "message.h"
#include "utils.h"
#include "client_context.h"
#include "catalog.h"
#include "thread_pool.h"

#define DEFAULT_QUERY_BUFFER_SIZE 1024
#define CONCURRENT_CLIENTS_SIZE 32

ThreadPool* tpool;

/**
 * handle_client(client_socket)
 * This is the execution routine after a client has connected.
 * It will continually listen for messages from the client and execute queries.
 **/
int handle_client(int client_socket) {
    int done = 0;
    int length = 0;
    int did_shutdown = 0;

    log_info("Connected to socket: %d.\n", client_socket);

    // Create two messages, one from which to read and one from which to receive
    message send_message;
    message recv_message;

    // create the client context here
    ClientContext* client_context = initialize_client_context();

    // Continually receive messages from client and execute queries.
    // 1. Parse the command
    // 2. Handle request if appropriate
    // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
    // 4. Send response to the request.
    do {
        length = recv(client_socket, &recv_message, sizeof(message), 0);
        if (length < 0) {
            log_err("Client connection closed!\n");
            exit(1);
        } else if (length == 0) {
            done = 1;
        }

        if (!done) {
            char recv_buffer[recv_message.length + 1];
            length = recv(client_socket, recv_buffer, recv_message.length,0);
            recv_message.payload = recv_buffer;
            recv_message.payload[recv_message.length] = '\0';
            
            DbOperator* query = parse_command(recv_message.payload, &send_message, client_socket, client_context);

            char* result;
            if (client_context->batched_operator != NULL) {
                result = execute_db_operator_while_batching(query);
            } else if (client_context->load_operator != NULL) {
                result = execute_db_operator_while_loading(client_context, query, recv_message.payload);
            } else {
                result = execute_db_operator(query);
            }

            if (query && query->type == SHUTDOWN) {
                did_shutdown = 1;
                done = 1;
            }

            send_message.length = strlen(result);
            char send_buffer[send_message.length + 1];
            strcpy(send_buffer, result);
            send_message.payload = send_buffer;
            send_message.status = OK_WAIT_FOR_RESPONSE;

            if (query) {
                if (client_context->batched_operator != NULL) {
                    if (query->type == BATCH_QUERIES) {
                        free_db_operator(query);
                    } else if (query->type == BATCH_EXECUTE) {
                        end_batch_query(client_context);
                        free_db_operator(query);
                    }
                } else {
                    if (query->type == SHUTDOWN) {
                        did_shutdown = 1;
                        done = 1;
                    } else if (query->type == PRINT) {
                        free(result);
                    }
                    free_db_operator(query);
                }
            }
            
            // 3. Send status of the received message (OK, UNKNOWN_QUERY, etc)
            if (send(client_socket, &(send_message), sizeof(message), 0) == -1) {
                log_err("Failed to send message.");
                exit(1);
            }

            int send_block_size = 4096;
            int sent_so_far = 0;
            while (sent_so_far < send_message.length) {
                int amount_left = send_message.length - sent_so_far;
                int amount_to_send = amount_left > send_block_size ? send_block_size: amount_left;
                // 4. Send response to the request
                if (send(client_socket, send_message.payload + sent_so_far, amount_to_send, 0) == -1) {
                    log_err("Failed to send message.");
                    exit(1);
                }
                sent_so_far += amount_to_send;
            }
        }
    } while (!done);

    free_client_context(client_context);
    log_info("Connection closed at socket %d!\n", client_socket);
    close(client_socket);
    return did_shutdown;
}

/**
 * setup_server()
 *
 * This sets up the connection on the server side using unix sockets.
 * Returns a valid server socket fd on success, else -1 on failure.
 **/
int setup_server() {
    int server_socket;
    size_t len;
    struct sockaddr_un local;

    size_t size = 300;
    int* arr = (int*) malloc(sizeof(int) * size);
    int* positions = (int*) malloc(sizeof(int) * size);

    for (size_t i = 0; i < size; i+=2) {
        arr[i] = i;
        arr[i + 1] = i + 1;
        positions[i] = i;
        positions[i + 1] = i + 1;
    }

    construct_btree(arr, positions, size);

    log_info("Attempting to setup server...\n");

    if ((server_socket = socket(AF_UNIX, SOCK_STREAM, 0)) == -1) {
        log_err("L%d: Failed to create socket.\n", __LINE__);
        return -1;
    }

    local.sun_family = AF_UNIX;
    strncpy(local.sun_path, SOCK_PATH, strlen(SOCK_PATH) + 1);
    unlink(local.sun_path);

    /*
    int on = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
    {
        log_err("L%d: Failed to set socket as reusable.\n", __LINE__);
        return -1;
    }
    */

    len = strlen(local.sun_path) + sizeof(local.sun_family) + 1;
    if (bind(server_socket, (struct sockaddr *)&local, len) == -1) {
        log_err("L%d: Socket failed to bind.\n", __LINE__);
        return -1;
    }

    if (listen(server_socket, 5) == -1) {
        log_err("L%d: Failed to listen on socket.\n", __LINE__);
        return -1;
    }

    return server_socket;
}

Status shutdown_server() {
    struct Status ret_status;
    int rflag = free_db(current_db);
    ret_status.code = rflag == 0 ? OK : ERROR;
    return ret_status;
}

// Currently this main will setup the socket and accept a single client.
// After handling the client, it will exit.
// You WILL need to extend this to handle MULTIPLE concurrent clients
// and remain running until it receives a shut-down command.
// 
// Getting Started Hints:
//      How will you extend main to handle multiple concurrent clients? 
//      Is there a maximum number of concurrent client connections you will allow?
//      What aspects of siloes or isolation are maintained in your design? (Think `what` is shared between `whom`?)
int main(void)
{
    int server_socket = setup_server();
    if (server_socket < 0) {
        exit(1);
    }

    log_info("Waiting for a connection %d ...\n", server_socket);

    struct sockaddr_un remote;
    socklen_t t = sizeof(remote);

    int client_socket = 0;
    int did_shutdown = 0;

    long number_of_processors = sysconf(_SC_NPROCESSORS_ONLN);
    printf("%lu cores found\n", number_of_processors);
    tpool = initialize_thread_pool(7);

    while(!did_shutdown && (client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) != -1) {         
        did_shutdown = handle_client(client_socket);
    }

    log_info("Shutting down...\n");

    exit(1);

    // if ((client_socket = accept(server_socket, (struct sockaddr *)&remote, &t)) == -1) {
    //     log_err("L%d: Failed to accept a new connection.\n", __LINE__);
    //     exit(1);
    // }


    return 0;
}
