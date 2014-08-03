/*
 * Copyrights ?
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */


#include "orte_config.h"
#include "orte/constants.h"

#include <string.h>
#include <stdlib.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_SYS_TYPES_H
#include <sys/types.h>
#endif
#ifdef HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef HAVE_SYS_PARAM_H
#include <sys/param.h>
#endif
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef HAVE_NETDB_H
#include <netdb.h>
#endif
#include <errno.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <stdlib.h>
#include <netdb.h>
#include <string.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>

#include "opal/util/output.h"
#include "orte/util/clock_sync.h"
#include "opal/mca/event/event.h"
#include "opal/mca/dstore/dstore.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/state/state_types.h"
#include "orte/mca/state/state.h"

#define MAX_COUNT 100
#define BIAS_MAX_STR 32

static void debug_hang(int val)
{
    while(val){
        sleep(1);
    }
}

inline static void _clksync_output(char *fmt, ... )
{
    char *pref = "%s [%s]: ";
    char *suf = "\n";
    va_list args;
    va_start( args, fmt );
    int size = strlen(fmt);
    size += strlen(pref) + strlen(suf) + 10;
    char *tbuf = malloc( sizeof(char) * size);

    if( tbuf ){
        snprintf(tbuf, size, "%s%s%s", pref, fmt, suf);
        opal_output(0, tbuf, ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __FUNCTION__, args );
        free(tbuf);
    }else{
        opal_output(0, "%s [%s]: Cannot allocate memory!\n", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __FUNCTION__ );
    }
    va_end( args );
}

#define CLKSYNC_OUTPUT( x ) _clksync_output x

#define PROC_NAME_CMP(p1, p2) ( p1.jobid == p2.jobid && p1.vpid == p2.vpid )

typedef enum { no_sync, rml_direct, sock_direct, rml_tree, sock_tree } sync_strategy_t;

typedef enum { init, req_measure, resp_init, resp_serve, finalized } clock_sync_state_t;

typedef enum { bias_in_progress, bias_calculated } measure_status_t;



static sync_strategy_t clksync_sync_strategy = sock_direct;
unsigned int clksync_rtt_measure_count = 100;
unsigned int clksync_bias_measure_count = 100;
unsigned int clksync_timeout = 10000;

typedef struct {
    bool is_hnp;
    clock_sync_state_t state;
    orte_process_name_t parent;
    int cur_daemon;
    double bias, par_bias;
    measure_status_t req_state;
    uint32_t snd_count;
    opal_pointer_array_t *childs;
    opal_pointer_array_t *results;
    orte_state_caddy_t *caddy;
    // direct info
    int fd;
    unsigned short port, par_port;
    char *par_uri;
    opal_event_t *ev;
} clock_sync_t;

typedef struct {
    double rtt, bias;
} measurement_t;

// Utilitys
char *state_to_str(clock_sync_state_t st);

// Socket manipulation
static char *addrinfo2string(struct addrinfo *addr);
static int create_listen_sock(int *ofd, unsigned short *oport);
static int connect_nb(int sock, const struct sockaddr *addr,
                      socklen_t addrlen, struct timeval timeout);

// Common routines
inline static double timing_get_ts(void);
inline static int put_ts(opal_buffer_t *buffer);
inline static int extract_ts(opal_buffer_t *buffer, double *ret);
static opal_buffer_t *form_measurement_request(clock_sync_t *cs);
static int form_measurement_reply(clock_sync_t *cs, opal_buffer_t *buffer,
                                  measure_status_t *state, opal_buffer_t *rbuffer );
static int extract_measurement_reply(clock_sync_t *cs, opal_buffer_t *buffer, measurement_t *result);
static int calculate_bias(clock_sync_t *cs, double *bias);
static int read_opal_buffer(int fd, opal_buffer_t *buffer);

// State machine
orte_process_name_t current_orted(clock_sync_t *cs);
orte_process_name_t next_orted(clock_sync_t *cs);
static int responder_init(clock_sync_t *cs);
static int responder_activate(clock_sync_t *cs);
static int requester_init(clock_sync_t *cs, opal_buffer_t *buffer);
static int base_hnp_init_direct(clock_sync_t *cs);
static int hnp_init_state(clock_sync_t **cs);
static int base_orted_init_direct(clock_sync_t *cs);
static int orted_init_state(clock_sync_t **cs);

// RML routines
static void rml_callback(int status, orte_process_name_t* sender,
                         opal_buffer_t *buffer, orte_rml_tag_t tag,
                         void* cbdata);
static int rml_request(clock_sync_t *cs);
static int rml_process(clock_sync_t *cs, orte_process_name_t* sender, opal_buffer_t *buffer);
static int rml_respond(clock_sync_t *cs, orte_process_name_t* sender,
                       opal_buffer_t *buffer);
// Socket routines
static void sock_callback(int status, orte_process_name_t* sender,
                          opal_buffer_t *buffer, orte_rml_tag_t tag,
                          void* cbdata);
static int sock_parent_addrs(clock_sync_t *cs, opal_pointer_array_t *array);
static int sock_choose_addr(clock_sync_t *cs, opal_pointer_array_t *addrs, struct addrinfo *out_addr);
static int sock_connect_to_parent(clock_sync_t *cs);
static int sock_one_measurement(clock_sync_t *cs, int fd, measurement_t *m);
static int sock_measure_rtt(clock_sync_t *cs, int fd, double *rtt);
static int sock_measure_bias(clock_sync_t *cs, opal_buffer_t *buffer);
static void sock_respond(int fd, short flags, void* cbdata);

// Interface
int orte_util_clock_sync_hnp_init(orte_state_caddy_t *caddy);
int orte_util_clock_sync_orted_init(void);

//---------------- Utility functions -------------------------

char *state_to_str(clock_sync_state_t st)
{
    switch(st){
    case init: return "init"; break;
    case req_measure: return "req_measure"; break;
    case resp_init: return "resp_init"; break;
    case resp_serve: return "resp_serve"; break;
    case finalized: return "finalized"; break;
    }
    return NULL;
}

// -------------------- Socket manipulation ---------------------------

static char *addrinfo2string(struct addrinfo *addr)
{
    char *buf = NULL;
    if( addr->ai_family == AF_INET ){
        buf = malloc( INET_ADDRSTRLEN );
        struct sockaddr_in *sin = (struct sockaddr_in *)addr->ai_addr;
        if( NULL == inet_ntop(addr->ai_family, &sin->sin_addr.s_addr, buf, INET_ADDRSTRLEN ) ){
            sprintf(buf, "-");
        }
    }else if( addr->ai_family == AF_INET6 ){
        buf = malloc( INET6_ADDRSTRLEN );
        struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)addr->ai_addr;
        if( NULL == inet_ntop(addr->ai_family, &sin6->sin6_addr.__in6_u, buf, INET6_ADDRSTRLEN ) ){
            sprintf(buf, "-");
        }
    }
    return buf;
}

static int create_listen_sock(int *ofd, unsigned short *oport)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if( fd < 0 ){
        // TODO: error handling
        return ORTE_ERROR;
    }
    struct sockaddr_in sa;
    sa.sin_family = AF_INET;
    sa.sin_port = 0;
    sa.sin_addr.s_addr = INADDR_ANY;

    if( bind(fd, (struct sockaddr*)&sa, sizeof(sa)) < 0 ){
        // TODO: error handling
        return -1;
    }


    if( listen(fd, 16) < 0 ){
        // TODO: error handling
        return ORTE_ERROR;
    }

    memset(&sa, 0, sizeof(sa));
    sa.sin_port = 0;
    socklen_t len = sizeof(sa);
    if( getsockname(fd, (struct sockaddr*)&sa, &len) ){
        // TODO: error handling
        return ORTE_ERROR;
    }
    *oport = ntohs(sa.sin_port);
    *ofd = fd;
    return 0;
}

static int connect_nb(int sock, const struct sockaddr *addr,
                      socklen_t addrlen, struct timeval timeout)
{
    extern int errno;
    int status;

    fd_set set;
    FD_ZERO(&set);
    FD_SET(sock, &set);
    fcntl(sock, F_SETFL, O_NONBLOCK);

    if( ( status = connect(sock, addr, addrlen) == -1 ) ){
        if ( errno != EINPROGRESS )
            return status;
    }
    status = select(sock+1, NULL, &set, NULL, &timeout);
    if( status <= 0){
        // no connection was established
        return -1;
    }
    return 0;
}

// ------------------ Common routines ---------------------------

inline static double timing_get_ts(void){
    struct timeval tv;
    gettimeofday(&tv,NULL);
    double ret = tv.tv_sec + tv.tv_usec*1E-6;
    return ret;
}

inline static int put_ts(opal_buffer_t *buffer)
{
    struct timeval tv;
    gettimeofday(&tv,NULL);

    uint32_t buf32;
    int rc;

    // put seconds part
    buf32 = tv.tv_sec;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &buf32, 1, OPAL_UINT32 ))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    // put milliseconds part
    buf32 = tv.tv_usec;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &buf32, 1, OPAL_UINT32 ))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    return 0;
}

inline static int extract_ts(opal_buffer_t *buffer, double *ret)
{
    int idx = 1;
    uint32_t buf32;
    int rc;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &buf32, &idx, OPAL_UINT32))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    *ret = buf32;

    idx = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &buf32, &idx, OPAL_UINT32))) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }
    *ret = *ret + buf32 * 1E-6;
    return 0;
}

static opal_buffer_t *form_measurement_request(clock_sync_t *cs)
{
    opal_buffer_t *buffer = OBJ_NEW(opal_buffer_t);
    int rc;

    if( buffer == NULL ){
        ORTE_ERROR_LOG(ORTE_ERR_MEM_LIMIT_EXCEEDED);
        return NULL;
    }
    uint32_t buf32 = cs->req_state;

    // Send current status
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &buf32, 1, OPAL_UINT32 ))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return NULL;
    }

    // Put timestamp to receive it in response
    if( ( rc = put_ts(buffer) ) ) {
        OBJ_RELEASE(buffer);
        return NULL;
    }

    return buffer;
}

static int form_measurement_reply(clock_sync_t *cs, opal_buffer_t *buffer,
                                  measure_status_t *state, opal_buffer_t *rbuffer )
{
    uint32_t buf32;
    int idx = 1, rc;

    // Check for measurement completition
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &buf32, &idx, OPAL_UINT32)) ) {
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    *state = buf32;
    if( *state == bias_calculated ){
        return 0;
    }

    if( ( rc = opal_dss.copy_payload(rbuffer, buffer) ) ){
        ORTE_ERROR_LOG(rc);
        goto err_exit;
    }

    if( ( rc = put_ts(rbuffer) ) ){
        goto err_exit;
    }
    return 0;

err_exit:
    return rc;
}


static int extract_measurement_reply(clock_sync_t *cs, opal_buffer_t *buffer, measurement_t *result)
{
    double lsnd, rsnd, lrcv = timing_get_ts();
    int rc;

    if( ( rc = extract_ts(buffer, &lsnd) ) ){
        return rc;
    }

    if( ( rc = extract_ts(buffer, &rsnd) ) ){
        return rc;
    }

    double rtt = lrcv - lsnd;
    double bias = rsnd - (lsnd + (rtt)/2);

    measurement_t mes = { rtt, bias };
    *result = mes;

    CLKSYNC_OUTPUT( ("rtt = %15.15lf, bias = %15.15lf", rtt, bias) );

    return 0;
}

static int calculate_bias(clock_sync_t *cs, double *bias)
{
    double cum_bias;
    double min_rtt;
    int i, count = 0;

    int size = opal_pointer_array_get_size(cs->results);
    if( size <= 0 ) {
        // TODO: Verbose output to explain the error
        return -1;
    }

    measurement_t *ptr;
    if( NULL == (ptr = (measurement_t *)opal_pointer_array_get_item(cs->results, 0))) {
        // TODO: Verbose output to explain the error
        return -1;
    }
    min_rtt = ptr->rtt;

    for(i=1;i<size; i++){
        if( NULL == (ptr = (measurement_t *)opal_pointer_array_get_item(cs->results, i))) {
            // TODO: Verbose output to explain the error
            return -1;
        }
        if( min_rtt > ptr->rtt ){
            min_rtt = ptr->rtt;
        }
    }

    for(i=0;i<size; i++){
        if( NULL == (ptr = (measurement_t *)opal_pointer_array_get_item(cs->results, i))) {
            // TODO: Verbose output to explain the error
            return -1;
        }
        if( ptr->rtt <= min_rtt*1.05 ){
            cum_bias += ptr->rtt;
            count++;
        }
    }

    *bias = cum_bias / count + cs->par_bias;
    return 0;
}

static int read_opal_buffer(int fd, opal_buffer_t *buffer)
{
    int rc = 0;
    char tbuf[64];
    char *buf = malloc(sizeof(tbuf));

    if( buf == NULL ){
        rc = ORTE_ERR_MEM_LIMIT_EXCEEDED;
        goto err_exit;
    }

    int size = 0;
    int ret = 0;
    while( (ret = read(fd,tbuf, sizeof(tbuf))) ){
        memcpy(buf + size, tbuf, ret);
        size += ret;
        buf = realloc(buf, size);
        if( buf == NULL ){
            rc = ORTE_ERR_MEM_LIMIT_EXCEEDED;
            goto err_exit;
        }
    }
    if( size ){
        rc = opal_dss.load(buffer, buf, size);
    } else {
        rc = ORTE_ERR_BAD_PARAM;
    }

    free(buf);
err_exit:
    if( rc )
        ORTE_ERROR_LOG(rc);
    return rc;
}

//----------------- State machine ----------------

orte_process_name_t current_orted(clock_sync_t *cs)
{
    int size = opal_pointer_array_get_size(cs->childs);
    if( size <= cs->cur_daemon ){
        return orte_name_invalid;
    }
    orte_proc_t *daemon=NULL;
    if( NULL == (daemon = (orte_proc_t*)opal_pointer_array_get_item(cs->childs, cs->cur_daemon))) {
        return orte_name_invalid;
    }
    return daemon->name;
}

orte_process_name_t next_orted(clock_sync_t *cs)
{
    cs->cur_daemon++;
    int size = opal_pointer_array_get_size(cs->childs);
    if( size <= cs->cur_daemon ){
        return orte_name_invalid;
    }
    orte_proc_t *daemon=NULL;
    if( NULL == (daemon = (orte_proc_t*)opal_pointer_array_get_item(cs->childs, cs->cur_daemon))) {
        return orte_name_invalid;
    }
    return daemon->name;
}


static int responder_init(clock_sync_t *cs)
{
    int rc;

    switch( clksync_sync_strategy ){
    case sock_direct:
    case sock_tree:{
        // setup working socket event
        if( (rc = create_listen_sock(&cs->fd, &cs->port) ) )
            return rc;

        cs->ev = opal_event_alloc();
        if( cs->ev == NULL ){
            rc = ORTE_ERR_MEM_LIMIT_EXCEEDED;
            ORTE_ERROR_LOG(rc);
            goto eexit;
        }
        opal_event_set(opal_event_base, cs->ev, cs->fd, OPAL_EV_READ | OPAL_EV_PERSIST,
                       sock_respond, cs);
        opal_event_add(cs->ev,0);
        break;
    }
    case rml_direct:
    case rml_tree:
        break;
    default:
        return -1;
    }

    return responder_activate(cs);

eexit:
    close(cs->fd);
    return rc;
}

static int responder_activate(clock_sync_t *cs)
{
    int rc;

    orte_process_name_t dname = current_orted(cs);
    if( PROC_NAME_CMP(dname,orte_name_invalid) ){
        // Nothing to do, all orteds was served
        cs->state = finalized;
        return 0;
    }

    // Create initiation buffer
    opal_buffer_t *buffer = OBJ_NEW(opal_buffer_t);
    char *bias = malloc(sizeof(char) * BIAS_MAX_STR );
    sprintf(bias, "%15.15lf", cs->bias );
    opal_dss.pack(buffer, &bias, 1, OPAL_STRING );
    free(bias);

    switch( clksync_sync_strategy ){
    case rml_direct:
    case rml_tree:
        break;
    case sock_direct:
    case sock_tree:{
        // Send contact information
        char *ptr = orte_rml.get_contact_info();
        opal_dss.pack(buffer, ptr, 1, OPAL_STRING);
        free(ptr);
        opal_dss.pack(buffer,&cs->port, 1, OPAL_UINT16);
        break;
    }
    default:
        return -1;
    }

    if (0 > (rc = orte_rml.send_buffer_nb(&dname, buffer,
                                          ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return rc;
    }
    return 0;
}


static int requester_init(clock_sync_t *cs, opal_buffer_t *buffer)
{
    int idx = 1, rc;

    char *bias = malloc(sizeof(char) * BIAS_MAX_STR );

    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &bias, &idx, OPAL_STRING ) ) ){
        ORTE_ERROR_LOG(rc);
        return rc;
    }

    if( 1 != sscanf(bias, "%lf", &cs->par_bias ) ){
        return ORTE_ERROR;
    }

    switch( clksync_sync_strategy ){
    case rml_direct:
    case rml_tree:
        break;
    case sock_direct:
    case sock_tree:{
        idx = 1;
        if( ( rc = opal_dss.unpack(buffer, &cs->par_uri, &idx, OPAL_STRING) ) ){
            ORTE_ERROR_LOG(rc);
            return -1;
        }
        idx = 1;
        if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &cs->par_port, &idx, OPAL_UINT16 ) ) ){
            ORTE_ERROR_LOG(rc);
            return rc;
        }
        break;
    }
    default:
        return ORTE_ERROR;
    }
    return 0;
}

static int base_hnp_init_direct(clock_sync_t *cs)
{
    memset(cs, 0, sizeof(*cs));
    cs->is_hnp = true;
    cs->state = resp_init;
    cs->bias = 0;
    cs->par_bias = 0;
    cs->cur_daemon = 0;
    cs->childs = OBJ_NEW(opal_pointer_array_t);
    cs->results = OBJ_NEW(opal_pointer_array_t);
    cs->req_state = bias_calculated;
    cs->parent = *ORTE_PROC_MY_NAME;

    if( cs->childs == NULL || cs->results == NULL ){
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    orte_job_t *jorted = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    int dsize = opal_pointer_array_get_size(jorted->procs);
    int i;

    /* put all daemons into childs list */
    orte_proc_t *daemon=NULL;
    for(i = 1; i < dsize; i++){
        if( NULL == (daemon = (orte_proc_t*)opal_pointer_array_get_item(jorted->procs, i))) {
            // TODO: Can this happen?
            continue;
        }
        int rc;
        if( (rc = opal_pointer_array_add(cs->childs, daemon)) ){
            OPAL_ERROR_LOG(rc);
            OBJ_RELEASE(cs->childs);
            return rc;
        }
    }
    return ORTE_SUCCESS;
}

static int hnp_init_state(clock_sync_t **cs)
{
    *cs = malloc(sizeof(clock_sync_t));
    if( *cs == NULL ){
        return ORTE_ERR_OUT_OF_RESOURCE;
    }
    return  base_hnp_init_direct(*cs);
}


static int base_orted_init_direct(clock_sync_t *cs)
{
    memset(cs, 0, sizeof(*cs));
    cs->is_hnp = false;
    cs->state = init;
    cs->bias = 0;
    cs->par_bias = 0;
    cs->cur_daemon = 0;
    cs->childs = OBJ_NEW(opal_pointer_array_t);
    cs->results = OBJ_NEW(opal_pointer_array_t);
    cs->req_state = bias_in_progress;
    cs->parent = *ORTE_PROC_MY_HNP;

    if( (cs->childs == NULL) || (cs->results == NULL) ){
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    return 0;
}

static int orted_init_state(clock_sync_t **cs)
{
    *cs = malloc(sizeof(clock_sync_t));
    if( *cs == NULL ){
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    return base_orted_init_direct(*cs);
}

// -------------------- RML routines ------------------------------------------

static void rml_callback(int status, orte_process_name_t* sender,
                         opal_buffer_t *buffer, orte_rml_tag_t tag,
                         void* cbdata)
{
    clock_sync_t *cs = cbdata;

    switch(cs->state){
    case init:
        requester_init(cs, buffer);
        cs->state = req_measure;
        if( rml_request(cs) ){
            cs->state = finalized;
        }
        break;
    case req_measure:
        if( rml_process(cs, sender, buffer) ){
            cs->state = finalized;
        }
        break;
    case resp_serve:
        rml_respond(cs, sender, buffer);
        break;
    case resp_init:
        break;
    default:
        CLKSYNC_OUTPUT( ("This state is not allowed here: %s", state_to_str(cs->state)) );
        break;
    }


    switch(cs->state){
    case req_measure:
        if( rml_request(cs) ){
            cs->state = finalized;
        }
        break;
    case resp_init:
        responder_activate(cs);
        cs->state = resp_serve;
        break;
    case finalized:
        if( cs->is_hnp ){
            cs->caddy->jdata->state = ORTE_JOB_STATE_DAEMONS_REPORTED;
            ORTE_ACTIVATE_JOB_STATE(cs->caddy->jdata, ORTE_JOB_STATE_VM_READY);
        }
        orte_rml.recv_cancel(ORTE_NAME_WILDCARD, ORTE_RML_TAG_TIMING_CLOCK_SYNC);
        double bias;
        if( calculate_bias(cs, &bias) ){
            CLKSYNC_OUTPUT( ("cannot calculate bias\n") );
            bias = 0;
            return;
        }
        CLKSYNC_OUTPUT( ("result bias = %15.15lf\n",bias) );
        break;
    default:
        CLKSYNC_OUTPUT( ("This state is not allowed here: %s", state_to_str(cs->state)) );
        break;
    }

    CLKSYNC_OUTPUT( ("callback is called, final state = %s\n", state_to_str(cs->state)) );
}

static int rml_request(clock_sync_t *cs)
{
    int rc;
    opal_buffer_t *buffer = form_measurement_request(cs);
    if( buffer == NULL ){
        return ORTE_ERROR;
    }

    if (0 > (rc = orte_rml.send_buffer_nb(&cs->parent, buffer, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return rc;
    }
    cs->snd_count++;

    CLKSYNC_OUTPUT( ("send measurement request to %s", ORTE_NAME_PRINT(&cs->parent)) );
    return 0;
}


static int rml_process(clock_sync_t *cs, orte_process_name_t* sender, opal_buffer_t *buffer)
{
    int rc;

    CLKSYNC_OUTPUT( ("process %s reply", ORTE_NAME_PRINT(sender)) );

    measurement_t *result = malloc( sizeof(*result) );
    if( ( rc = extract_measurement_reply(cs, buffer, result) ) ){
        return rc;
    }

    if( ( rc = opal_pointer_array_add(cs->results, result ) ) ){
        OPAL_ERROR_LOG(rc);
        return ORTE_ERROR;
    }

    if( cs->snd_count >= MAX_COUNT ){
        if( opal_pointer_array_get_size(cs->childs) ){
            cs->state = resp_serve;
        }else{
            cs->state = finalized;
        }
    }

    return 0;
}

static int rml_respond(clock_sync_t *cs,
                       orte_process_name_t* sender,
                       opal_buffer_t *buffer)
{
    int rc = 0;
    measure_status_t state;
    opal_buffer_t *rbuffer = OBJ_NEW(opal_buffer_t);

    CLKSYNC_OUTPUT( ("called by %s", ORTE_NAME_PRINT(sender)) );

    if( ( rc = form_measurement_reply(cs, buffer, &state, rbuffer ) ) ){
        goto eexit;
    }

    if( state == bias_calculated ){
        orte_process_name_t next = next_orted(cs);
        if( PROC_NAME_CMP(next, orte_name_invalid) ){
            cs->state = finalized;
        }else{
            cs->state = resp_init;
        }
        goto eexit;
    }

    if (0 > (rc = orte_rml.send_buffer_nb(sender, rbuffer, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        goto eexit;
    }

    CLKSYNC_OUTPUT( ("reply to measurement request from %s", ORTE_NAME_PRINT(sender)) );
eexit:
    OBJ_RELEASE(rbuffer);
    return rc;
}

// ------------------------- Socket routines --------------------------------------------

static void sock_callback(int status, orte_process_name_t* sender,
                          opal_buffer_t *buffer, orte_rml_tag_t tag,
                          void* cbdata)
{
    clock_sync_t *cs = cbdata;

    if(cs->state != init ){
        CLKSYNC_OUTPUT( ("This state is not allowed here: %s", state_to_str(cs->state)) );
        return;
    }

    requester_init(cs, buffer);

    if( sock_measure_bias(cs, buffer) ){
        CLKSYNC_OUTPUT( ("Cannot measure bias") );
        return;
    }

    // If this process has childrens assigned - initiate their testing
    if( opal_pointer_array_get_size(cs->childs) ){
        if( responder_init(cs) ){
            CLKSYNC_OUTPUT( ("Cannot initiate responder") );
            return;
        }
        cs->state = resp_serve;
    } else {
        cs->state = finalized;
    }

    CLKSYNC_OUTPUT( ("callback is called, final state = %s\n", state_to_str(cs->state)) );
}

static int sock_parent_addrs(clock_sync_t *cs, opal_pointer_array_t *array)
{
    int rc = 0;
    if( cs->par_uri == NULL )
    {
        rc = ORTE_ERR_BAD_PARAM;
        goto eexit1;
    }

    char *uri = strdup(cs->par_uri);
    char *host, *ports;

    uint16_t af_family = AF_UNSPEC;
    if (0 == strncmp(uri, "tcp:", 4)) {
        af_family = AF_INET;
        host = uri + strlen("tcp://");
    }
#if OPAL_ENABLE_IPV6
    else if (0 == strncmp(uris[i], "tcp6:", 5)) {
        af_family = AF_INET6;
        host = uri + strlen("tcp6://");
    }
#endif

    // separate the ports from the network addrs
    ports = strrchr(uri, ':');
    *ports = '\0';

    // split the addrs
    if (NULL == host || 0 == strlen(host)) {
        rc = ORTE_ERR_BAD_PARAM;
        goto eexit2;
    }

    // if this is a tcp6 connection, the first one will have a '['
    // at the beginning of it, and the last will have a ']' at the
    // end - we need to remove those extra characters
    char *hptr = host;
    if (AF_INET6 == af_family) {
        if ('[' == host[0]) {
            hptr = &host[1];
        }
        if (']' == host[strlen(host)-1]) {
            host[strlen(host)-1] = '\0';
        }
    }
    char **addrs = opal_argv_split(hptr, ',');

    // cycle across the provided addrs
    int j;
    for (j=0; NULL != addrs[j]; j++) {
        char *ptr = strdup(addrs[j]);
        if( ptr == NULL ){
            rc = ORTE_ERR_MEM_LIMIT_EXCEEDED;
            goto eexit3;
        }
        opal_pointer_array_add(array, (void*)ptr);
    }
eexit3:
    opal_argv_free(addrs);
eexit2:
    free(uri);
eexit1:
    if( rc )
        ORTE_ERROR_LOG(rc);
    return rc;
}

static int sock_choose_addr(clock_sync_t *cs, opal_pointer_array_t *addrs, struct addrinfo *out_addr)
{
    struct addrinfo *result, *rp;
    int rc = 0;

    // Prepare timeout
    struct timeval  timeout;
    timeout.tv_sec = clksync_timeout / 1000000;
    timeout.tv_usec = clksync_timeout % 1000000;

    // Prepare service
    int max_port_len = 10;
    char service[max_port_len];
    snprintf(service, max_port_len, "%hu", cs->par_port);

    // Analyse each peer's address choose the one with best RTT
    struct addrinfo best_addr;
    double best_rtt = -1;
    memset(&best_addr, 0, sizeof(best_addr));
    unsigned int i = 0;
    size_t asize = opal_pointer_array_get_size(addrs);
    for( i = 0; i < asize; i++){
        char *host = (char*)opal_pointer_array_get_item(addrs,i);
        /* Obtain address(es) matching host/port */
        struct addrinfo hints;
        memset(&hints, 0, sizeof(struct addrinfo));
        hints.ai_family = AF_UNSPEC;    /* Allow IPv4 or IPv6 */
        hints.ai_socktype = SOCK_STREAM; /* Stream socket */
        hints.ai_flags = 0;
        hints.ai_protocol = 0;          /* Any protocol */

        int s;
        if( ( s = getaddrinfo(host, service, &hints, &result) ) ) {
            CLKSYNC_OUTPUT( ( "getaddrinfo: %s", gai_strerror(s) ) );
            continue;
        }

        for (rp = result; rp != NULL; rp = rp->ai_next) {
            int fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
            if ( fd < 0 ){
                CLKSYNC_OUTPUT( ( "Cannot create socket: %s", strerror(errno) ) );
                rc = ORTE_ERROR;
                goto eexit;
            }

            if ( connect_nb(fd, rp->ai_addr, rp->ai_addrlen, timeout) == 0){
                double rtt;
                if( sock_measure_rtt(cs, fd, &rtt) ){
                    char *buf = addrinfo2string(rp);
                    CLKSYNC_OUTPUT( ( "Cannot connect to %s", buf) );
                    free(buf);
                    close(fd);
                    rc = ORTE_ERROR;
                    goto eexit;
                }
                if( rtt >= 0 && (rtt < best_rtt || best_rtt == -1) ){
                    best_rtt = rtt;
                    if( best_addr.ai_addrlen < rp->ai_addrlen ){
                        best_addr.ai_addr = realloc(best_addr.ai_addr, rp->ai_addrlen);
                    }
                    memcpy(best_addr.ai_addr, rp->ai_addr, rp->ai_addrlen);
                    best_addr.ai_addrlen = rp->ai_addrlen;
                    best_addr.ai_family = rp->ai_family;
                }
            }else{
                char *buf = addrinfo2string(rp);
                CLKSYNC_OUTPUT( ( "Cannot connect to %s", buf) );
                free(buf);
            }
            close(fd);
        }
        freeaddrinfo(result);
        result = NULL;
    }
    if( best_rtt >= 0 )
        *out_addr = best_addr;
eexit:
    if( result )
        freeaddrinfo(result);
    return rc;
}

static int sock_connect_to_parent(clock_sync_t *cs)
{
    // Find the best connection to measure bias
    opal_pointer_array_t *addrs = OBJ_NEW(opal_pointer_array_t);
    struct addrinfo ainfo;

    if( sock_parent_addrs(cs, addrs) ){
        CLKSYNC_OUTPUT( ("CLOCK SYNC: cannot get parent URI list") );
        OBJ_RELEASE(addrs);
        return ORTE_ERROR;
    }

    if( sock_choose_addr(cs, addrs, &ainfo) ){
        OBJ_RELEASE(addrs);
        return -1;
    }
    OBJ_RELEASE(addrs);

    int fd = socket(ainfo.ai_family, ainfo.ai_socktype, 0);
    if( fd < 0 ){
        CLKSYNC_OUTPUT( ("CLOCK SYNC: cannot create socket") );
        return ORTE_ERROR;
    }

    // Prepare timeout
    struct timeval  timeout;
    timeout.tv_sec = clksync_timeout / 1000000;
    timeout.tv_usec = clksync_timeout % 1000000;
    if( connect_nb(fd, ainfo.ai_addr, ainfo.ai_addrlen, timeout ) ){
        char *buf = addrinfo2string(&ainfo);
        CLKSYNC_OUTPUT( ( "Cannot connect to %s", buf) );
        free(buf);
        return -1;
    }
    return fd;
}

static int sock_one_measurement(clock_sync_t *cs, int fd, measurement_t *m)
{
    opal_buffer_t *buffer;
    int rc = 0;

    if( m == NULL ){
        return ORTE_ERR_BAD_PARAM;
    }

    buffer = form_measurement_request(cs);
    if( buffer == NULL ){
        return ORTE_ERROR;
    }
    void *ptr;
    int32_t size;
    if( (rc = opal_dss.unload(buffer,&ptr,&size) ) ){
        goto eexit;
    }

    if( write(fd,ptr,size) < size ){
        // TODO: cycle until we send everything
        CLKSYNC_OUTPUT(("Cannot write to fd = %d", fd));
        rc = ORTE_ERROR;
        goto eexit2;
    }


    if( (rc = read_opal_buffer(fd, buffer) ) ){
        CLKSYNC_OUTPUT(("Cannot read from fd = %d", fd));
        goto eexit2;
    }

    if( ( rc = extract_measurement_reply(cs, buffer, m) ) ){
        goto eexit2;
    }

eexit2:
    free(ptr);
eexit:
    OBJ_RELEASE(buffer);
    return rc;
}

static int sock_measure_rtt(clock_sync_t *cs, int fd, double *rtt)
{
    unsigned int i;
    int rc;
    double min_rtt = -1;
    for(i = 0 ; i < clksync_rtt_measure_count; i++){
        measurement_t result;
        if( ( rc = sock_one_measurement(cs, fd, &result) ) ){
            return rc;
        }
        if( min_rtt > result.rtt ){
            min_rtt = result.rtt;
        }
    }

    *rtt = min_rtt;
    return 0;
}

static int sock_measure_bias(clock_sync_t *cs, opal_buffer_t *buffer)
{
    int rc = 0;

    if( cs->req_state == bias_calculated ){
        return 0;
    }

    int fd = sock_connect_to_parent( cs );
    if( fd < 0 ){
        return ORTE_ERROR;
    }

    unsigned int i;
    for(i=0; i < clksync_bias_measure_count; i++){
        measurement_t m;
        if( ( rc = sock_one_measurement(cs, fd, &m) ) ){
            goto eexit;
        }
        measurement_t *ptr = malloc(sizeof(*ptr));
        if( ptr == NULL ){
            rc = ORTE_ERR_MEM_LIMIT_EXCEEDED;
            ORTE_ERROR_LOG(rc);
            goto eexit;
        }
        *ptr = m;
        if( ( rc = opal_pointer_array_add(cs->results, ptr) ) ){
            goto eexit;
        }
    }

eexit:
    OBJ_RELEASE(cs->results);
    cs->results = NULL;
    close(fd);
    return rc;
}

static void sock_respond(int fd, short flags, void* cbdata)
{
    int rc = 0;
    clock_sync_t *cs = cbdata;
    struct sockaddr_in addr;
    opal_socklen_t addrlen = sizeof(addr);

    int cfd = accept(fd, (struct sockaddr*)&addr, &addrlen);
    if(cfd < 0) {
        CLKSYNC_OUTPUT( ("Cannot accept: %s", strerror(errno)) );
        return;
    }

    opal_buffer_t *buffer = OBJ_NEW(opal_buffer_t);
    if( (rc = read_opal_buffer(cfd, buffer) ) ){
        CLKSYNC_OUTPUT(("Cannot read from fd = %d", cfd));
        goto eexit;
    }

    opal_buffer_t *rbuffer = OBJ_NEW(opal_buffer_t);
    measure_status_t state;
    if( ( rc = form_measurement_reply(cs, buffer, &state, rbuffer ) ) ){
        CLKSYNC_OUTPUT(("Cannot form_measurement_reply"));
        goto eexit2;
    }

    if( state == bias_calculated ){
        orte_process_name_t next = next_orted(cs);
        if( PROC_NAME_CMP(next, orte_name_invalid) ){
            cs->state = finalized;
        }else{
            responder_activate(cs);
        }
        goto eexit2;
    }

    void *ptr;
    int32_t size;
    if( (rc = opal_dss.unload(buffer,&ptr,&size) ) ){
        ORTE_ERROR_LOG(rc);
        goto eexit2;
    }

    if( write(cfd, ptr, size) < size ){
        CLKSYNC_OUTPUT(("Cannot write to fd = %d", fd));
        rc = ORTE_ERROR;
        goto eexit3;
    }

eexit3:
    free(ptr);
eexit2:
    OBJ_RELEASE(rbuffer);
eexit:
    close(cfd);
}

int orte_util_clock_sync_hnp_init(orte_state_caddy_t *caddy)
{

    debug_hang(1);

    clock_sync_t *cs;
    if( hnp_init_state(&cs) ){
        return -1;
    }
    cs->caddy = caddy;

    switch( clksync_sync_strategy ){
    case rml_direct:
    case rml_tree:
        if( opal_pointer_array_get_size(cs->childs) ){
            orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                    ORTE_RML_PERSISTENT, rml_callback, cs);
            if( responder_activate(cs) ){
                goto err_exit;
            }
        }else{
            goto err_exit;
        }
        break;
    case sock_direct:
    case sock_tree:
        if( opal_pointer_array_get_size(cs->childs) ){
            if( responder_activate(cs) ){
                goto err_exit;
            }
        }
    default:
        opal_output(0,"BAD sync_strategy VALUE %d!", (int)clksync_sync_strategy);
        return -1;
    }

    return 0;
err_exit:
    free(cs);
    return -1;
}

int orte_util_clock_sync_orted_init()
{
    clock_sync_t *cs = NULL;

    debug_hang(1);

    int rc = orted_init_state(&cs);
    if( rc ){
        return rc;
    }

    switch( clksync_sync_strategy ){
    case rml_direct:
    case rml_tree:
        if( opal_pointer_array_get_size(cs->childs) ){
            orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                    ORTE_RML_PERSISTENT, rml_callback, cs);
        }else{
            goto err_exit;
        }
        break;
    case sock_direct:
    case sock_tree:
        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                0, sock_callback, cs);
        break;
    default:
        opal_output(0,"BAD sync_strategy VALUE %d!", (int)clksync_sync_strategy);
        return -1;
    }

    CLKSYNC_OUTPUT( ("callback is installed") );
    return 0;

err_exit:
    free(cs);
    return -1;
}
