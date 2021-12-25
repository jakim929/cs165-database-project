#ifndef THREAD_POOL_H
#define THREAD_POOL_H

#include <pthread.h>

#include "cs165_api.h"

typedef struct Task Task;
typedef struct ThreadPool_Thread ThreadPool_Thread;

// struct Task {
//     int k;
//     Task* next;
// };

// struct Task {
//     int* data;
//     pthread_mutex_t* write_mutex;
//     size_t start_position;
//     Result* result;
//     SelectOperator* select_operator;
//     Task* next;
//     int vector_size;
// };

typedef struct SelectTaskParams {
    int* data;
    size_t start_position;
    int num_operators;
    pthread_mutex_t* write_mutexes;
    Result** results;
    SelectOperator** select_operators;
    int scan_size;
} SelectTaskParams;

typedef struct GraceJoinTaskParams {
    JoinParams* join_params;
    pthread_mutex_t* inner_result_write_mutex;
    pthread_mutex_t* outer_result_write_mutex;
} GraceJoinTaskParams;

struct Task {
    Task* next;
    void (*fn)(void* arg);
    void* arg;
};

typedef struct TaskQueue {
    pthread_cond_t cond;
    pthread_mutex_t mutex;
    Task* start;
    Task* end;
    int size;
} TaskQueue;

typedef struct ThreadPool {
    pthread_mutex_t active_thread_count_mutex;
    pthread_cond_t no_active_thread_cond;
    int num_threads;
    int active_thread_count;
    ThreadPool_Thread* threads;
    TaskQueue* task_queue;
} ThreadPool;

struct ThreadPool_Thread {
    pthread_t pthread;
    ThreadPool* tpool;
    int id;
};

void start_thread(ThreadPool_Thread* thread);
ThreadPool* initialize_thread_pool(int num_threads);
void add_task_to_thread_pool(ThreadPool* tpool, Task* task);
void wait_until_thread_pool_idle(ThreadPool* tpool);

TaskQueue* initialize_task_queue();
void free_task_queue(TaskQueue* tqueue);
void push_to_task_queue(TaskQueue* tqueue, Task* task);
Task* pop_from_task_queue(TaskQueue* tqueue);

extern ThreadPool *tpool;

#endif
