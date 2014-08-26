/*
 * ? Copyrights ?
 */

#ifndef OPAL_SYS_TIMING_H
#define OPAL_SYS_TIMING_H

#include "opal/class/opal_list.h"

#define OPAL_ENABLE_DEBUG 1

#if OPAL_ENABLE_DEBUG

#define OPAL_TIMING_DESCR_MAX 1024
#define OPAL_TIMING_BUFSIZE 32
#define OPAL_TIMING_OUTBUF_SIZE (10*1024)

typedef enum { TEVENT, TBEGIN, TEND } opal_event_type_t;


typedef struct {
    opal_list_item_t super;
    int fib;
    opal_event_type_t type;
    char *func;
    char *file;
    int line;
    double ts, ts_ovh;
    char descr[OPAL_TIMING_DESCR_MAX];
    int id;
} opal_timing_event_t;

typedef struct opal_timing_t
{
    int cur_id;
    opal_list_t *events;
    opal_timing_event_t *buffer;
    size_t buffer_offset, buffer_size;
} opal_timing_t;

typedef struct {
    opal_timing_t *t;
    opal_timing_event_t *ev;
} opal_timing_prep_t;

void opal_timing_set(char *nodename, char *jobid, double bias);
void opal_timing_set_bias(double bias);

void opal_timing_init(opal_timing_t *t);

opal_timing_prep_t opal_timing_prep_ev(opal_timing_t *t, const char *fmt, ...);
void opal_timing_add_step(opal_timing_prep_t p, char *func, char *file, int line);

/*
opal_timing_prep_t opal_timing_prep_end(opal_timing_t *t, int id, const char *fmt, ...);
int opal_timing_begin(opal_timing_t *t, char *file, int line);
void opal_timing_end(opal_timing_prep_t p, char *file, int line);
*/

int opal_timing_report(opal_timing_t *t, const char *prefix, char *fname);
void opal_timing_release(opal_timing_t *t);

#define OPAL_TIMING_INIT(t) opal_timing_init(t)

#define OPAL_TIMING_EVENT(x) opal_timing_add_step( opal_timing_prep_ev x, __FUNCTION__, __FILE__, __LINE__)

#define OPAL_TIMING_REPORT_OUT(t, prefix) opal_timing_report(t, prefix, NULL)

#define OPAL_TIMING_REPORT_FILE(t, file) opal_timing_report(t, NULL, file)

#define OPAL_TIMING_RELEASE(t) opal_timing_release(t)

#else

#define OPAL_TIMING_INIT(t)

#define OPAL_TIMING_EVENT(x)

#define OPAL_TIMING_REPORT_OUT(t, prefix)

#define OPAL_TIMING_REPORT_FILE(t, file)

#define OPAL_TIMING_RELEASE(t)

#endif

#endif
