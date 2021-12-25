#include <pthread.h>

#include "cs165_api.h"
#include "thread_pool.h"

// OG
// void start_thread(ThreadPool_Thread* thread) {
//     while (1) {
//         pthread_mutex_lock(&(thread->tpool->task_queue->mutex));
//         while(thread->tpool->task_queue->size == 0) {
//             pthread_cond_wait(&(thread->tpool->task_queue->cond), &(thread->tpool->task_queue->mutex));
//         }
//         Task* task = pop_from_task_queue(thread->tpool->task_queue);
//         pthread_mutex_unlock(&(thread->tpool->task_queue->mutex));
        
//         pthread_mutex_lock(&(thread->tpool->active_thread_count_mutex));
//         thread->tpool->active_thread_count++;
//         pthread_mutex_unlock(&(thread->tpool->active_thread_count_mutex));
//         SelectOperator* select_operator = task->select_operator;
//         for (size_t i = task->start_position; i < task->start_position + task->vector_size; i++) {
//             if (
//                 (
//                     select_operator->range_start.is_null ||
//                     task->data[i] >= select_operator->range_start.value
//                 ) && (
//                     select_operator->range_end.is_null ||
//                     task->data[i] < select_operator->range_end.value
//                 )
//             ) {
//                 pthread_mutex_lock(task->write_mutex);
//                 ((int*) task->result->payload)[task->result->num_tuples++] = i;
//                 pthread_mutex_unlock(task->write_mutex);

//             }
//         }
//         free(task);
//         pthread_mutex_lock(&(thread->tpool->active_thread_count_mutex));
//         thread->tpool->active_thread_count--;
//         if (thread->tpool->active_thread_count == 0) {
//             pthread_cond_signal(&thread->tpool->no_active_thread_cond);
//         }
//         pthread_mutex_unlock(&(thread->tpool->active_thread_count_mutex));
//     }
// }

// Chunked operator
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
        
        task->fn(task->arg);

        free(task->arg);
        free(task);
        pthread_mutex_lock(&(thread->tpool->active_thread_count_mutex));
        thread->tpool->active_thread_count--;
        if (thread->tpool->active_thread_count == 0) {
            pthread_cond_signal(&thread->tpool->no_active_thread_cond);
        }
        pthread_mutex_unlock(&(thread->tpool->active_thread_count_mutex));
    }
}

// One scan per core
// void start_thread(ThreadPool_Thread* thread) {
//     while (1) {
//         pthread_mutex_lock(&(thread->tpool->task_queue->mutex));
//         while(thread->tpool->task_queue->size == 0) {
//             pthread_cond_wait(&(thread->tpool->task_queue->cond), &(thread->tpool->task_queue->mutex));
//         }
//         Task* task = pop_from_task_queue(thread->tpool->task_queue);
//         pthread_mutex_unlock(&(thread->tpool->task_queue->mutex));
        
//         pthread_mutex_lock(&(thread->tpool->active_thread_count_mutex));
//         thread->tpool->active_thread_count++;
//         pthread_mutex_unlock(&(thread->tpool->active_thread_count_mutex));

//         printf("thread[%d]: startpos[%zu] num_operators[%d] scan_size[%d]\n", thread->id, task->start_position, task->num_operators, task->scan_size);
//         int vector_size = 4096;
//         int end_position = task->start_position + task->scan_size;
//         for (int j = task->start_position; j < end_position; j+= vector_size) {
//             int intermediate_vector_size = j + vector_size > end_position ? end_position - j : vector_size;
//             for(int i = j; i < j + intermediate_vector_size; i++) {
//                 for (int k = 0; k < task->num_operators; k++) {
//                     SelectOperator* select_operator = task->select_operators[k];
//                     if (
//                         (
//                             select_operator->range_start.is_null ||
//                             task->data[i] >= select_operator->range_start.value
//                         ) && (
//                             select_operator->range_end.is_null ||
//                             task->data[i] < select_operator->range_end.value
//                         )
//                     ) {
//                         pthread_mutex_lock(&task->write_mutexes[k]); 
//                         ((int*) task->results[k]->payload)[task->results[k]->num_tuples++] = i;
//                         pthread_mutex_unlock(&task->write_mutexes[k]); 
//                     }
//                 }
//             }
//         }
        
//         pthread_mutex_lock(&(thread->tpool->active_thread_count_mutex));
//         thread->tpool->active_thread_count--;
//         if (thread->tpool->active_thread_count == 0) {
//             pthread_cond_signal(&thread->tpool->no_active_thread_cond);
//         }
//         pthread_mutex_unlock(&(thread->tpool->active_thread_count_mutex));
//     }
// }

ThreadPool* initialize_thread_pool(int num_threads) {
    ThreadPool* tpool = (ThreadPool*) malloc(sizeof(ThreadPool));
    pthread_mutex_init(&tpool->active_thread_count_mutex, NULL);
    pthread_cond_init(&tpool->no_active_thread_cond, NULL);
    tpool->num_threads = num_threads;
    tpool->active_thread_count = 0;
    tpool->threads = (ThreadPool_Thread*) malloc(num_threads * sizeof(ThreadPool_Thread));
    tpool->task_queue = initialize_task_queue();
    for (int i = 0; i < num_threads; i++) {
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
