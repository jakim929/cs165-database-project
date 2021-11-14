#include <pthread.h>

#include "cs165_api.h"
#include "thread_pool.h"

void start_thread(ThreadPool_Thread* thread) {
    while (1) {
        pthread_mutex_lock(&(thread->tpool->task_queue->mutex));
        while(thread->tpool->task_queue->size == 0) {
            pthread_cond_wait(&(thread->tpool->task_queue->cond), &(thread->tpool->task_queue->mutex));
        }
        Task* task = pop_from_task_queue(thread->tpool->task_queue);
        pthread_mutex_unlock(&(thread->tpool->task_queue->mutex));
        
        pthread_mutex_lock(&(thread->tpool->active_thread_count_mutex));
        thread->tpool->active_thread_count++;
        pthread_mutex_unlock(&(thread->tpool->active_thread_count_mutex));
        sleep(1);
        printf("val from thread %d: [%d]\n", thread->id, task->k);

        pthread_mutex_lock(&(thread->tpool->active_thread_count_mutex));
        thread->tpool->active_thread_count--;
        if (thread->tpool->active_thread_count == 0) {
            pthread_cond_signal(&thread->tpool->no_active_thread_cond);
        }
        pthread_mutex_unlock(&(thread->tpool->active_thread_count_mutex));
    }
}

ThreadPool* initialize_thread_pool(int num_threads) {
    ThreadPool* tpool = (ThreadPool*) malloc(sizeof(ThreadPool));
    pthread_mutex_init(&tpool->active_thread_count_mutex, NULL);
    pthread_cond_init(&tpool->no_active_thread_cond, NULL);
    tpool->num_threads = num_threads;
    tpool->active_thread_count = 0;
    tpool->threads = (ThreadPool_Thread*) malloc(num_threads * sizeof(ThreadPool_Thread));
    tpool->task_queue = initialize_task_queue();
    for (int i = 0; i < num_threads; i++) {
        printf("creating %d\n", i);
        tpool->threads[i].id = i;
        tpool->threads[i].tpool = tpool;
        pthread_create(&(tpool->threads[i].pthread), NULL, (void * (*)(void *)) start_thread, &(tpool->threads[i]));
    }
    return tpool;
}

void add_task_to_thread_pool(ThreadPool* tpool, Task* task) {
    pthread_mutex_lock(&tpool->task_queue->mutex);
    push_to_task_queue(tpool->task_queue, task);
    pthread_cond_signal(&tpool->task_queue->cond);
    pthread_mutex_unlock(&tpool->task_queue->mutex);
}

void wait_until_thread_pool_idle(ThreadPool* tpool) {
    pthread_mutex_lock(&(tpool->active_thread_count_mutex));
    while(tpool->task_queue->size != 0 || tpool->active_thread_count != 0) {
        pthread_cond_wait(&(tpool->no_active_thread_cond), &(tpool->active_thread_count_mutex));
    }
    if (tpool->active_thread_count == 0) {
        printf("IDLE NOW!\n");

    }
    pthread_mutex_unlock(&(tpool->active_thread_count_mutex));
}

TaskQueue* initialize_task_queue() {
    TaskQueue* tqueue = (TaskQueue*) malloc(sizeof(TaskQueue));
    pthread_mutex_init(&tqueue->mutex, NULL);
    pthread_cond_init(&tqueue->cond, NULL);
    tqueue->size = 0;
    tqueue->start = NULL;
    tqueue->end = NULL;
    return tqueue;
}

void free_task_queue(TaskQueue* tqueue) {
    pthread_mutex_destroy(&tqueue->mutex);
    pthread_cond_destroy(&tqueue->cond);
    free(tqueue);
}

// REQUIRES THREAD TO HAVE MUTEX LOCK
void push_to_task_queue(TaskQueue* tqueue, Task* task) {
    if (tqueue->size == 0) {
        tqueue->start = task;
        tqueue->end = task;
    } else {
        tqueue->end->next = task;
        tqueue->end = task;
    }
    tqueue->size++;
    task->next = NULL;
}

// REQUIRES THREAD TO HAVE MUTEX LOCK
Task* pop_from_task_queue(TaskQueue* tqueue) {
    Task* task;
    if (tqueue->size == 0) {
        task = NULL;
    } else if (tqueue->size == 1) {
        tqueue->size = 0;
        task = tqueue->start;
        tqueue->start = NULL;
        tqueue->end = NULL;
    } else {
        tqueue->size--;
        task = tqueue->start;
        tqueue->start = task->next;
    }
    return task;
}
