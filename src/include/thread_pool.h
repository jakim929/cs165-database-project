#ifndef BATCHED_OPERATOR_H
#define BATCHED_OPERATOR_H

#include <pthread.h>

#include "cs165_api.h"


typedef struct Task Task;
typedef struct ThreadPool_Thread ThreadPool_Thread;

struct Task {
    int k;
    Task* next;
};

// typedef struct Task {
//     int* data;
//     Result* result;
//     NullableInt* range_start;
//     NullableInt* range_end;
//     Task* next;
//     int vector_size;
// } Task;

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
void wait_until_task_queue_empty(ThreadPool* tpool);

TaskQueue* initialize_task_queue();
void free_task_queue(TaskQueue* tqueue);
void push_to_task_queue(TaskQueue* tqueue, Task* task);
Task* pop_from_task_queue(TaskQueue* tqueue);

#endif
