/* 
 * $HEADER$
 */
#ifndef OMPI_CONDITION_SPINLOCK_H
#define OMPI_CONDITION_SPINLOCK_H

#include "ompi_config.h"
#include <sys/time.h>
#include "threads/condition.h"
#include "threads/mutex.h"
#include "runtime/ompi_progress.h"

struct ompi_condition_t {
    ompi_object_t super;
    volatile int c_waiting;
    volatile int c_signaled;
};
typedef struct ompi_condition_t ompi_condition_t;

OBJ_CLASS_DECLARATION(ompi_condition_t);


static inline int ompi_condition_wait(ompi_condition_t *c, ompi_mutex_t *m)
{
    c->c_waiting++;
    if (ompi_using_threads()) {
        while (c->c_signaled == 0) {
            ompi_mutex_unlock(m);
            ompi_progress();
            ompi_mutex_lock(m);
        }
    } else {
        while (c->c_signaled == 0) {
            ompi_progress();
        }
    }
    c->c_signaled--;
    c->c_waiting--;
    return 0;
}

static inline int ompi_condition_timedwait(ompi_condition_t *c,
                                           ompi_mutex_t *m,
                                           const struct timespec *abstime)
{
    struct timeval tv;
    uint32_t secs = abstime->tv_sec;
    uint32_t usecs = abstime->tv_nsec * 1000;
    gettimeofday(&tv,NULL);

    c->c_waiting++;
    if (ompi_using_threads()) {
        while (c->c_signaled == 0 &&  
               (tv.tv_sec <= secs ||
               (tv.tv_sec == secs && tv.tv_usec < usecs))) {
            ompi_mutex_unlock(m);
            ompi_progress();
            gettimeofday(&tv,NULL);
            ompi_mutex_lock(m);
        }
    } else {
        while (c->c_signaled == 0 &&  
               (tv.tv_sec <= secs ||
               (tv.tv_sec == secs && tv.tv_usec < usecs))) {
            ompi_progress();
            gettimeofday(&tv,NULL);
        }
    }
    c->c_signaled--;
    c->c_waiting--;
    return 0;
}

static inline int ompi_condition_signal(ompi_condition_t *c)
{
    if (c->c_waiting) {
        c->c_signaled++;
    }
    return 0;
}

static inline int ompi_condition_broadcast(ompi_condition_t *c)
{
    c->c_signaled += c->c_waiting;
    return 0;
}

#endif

