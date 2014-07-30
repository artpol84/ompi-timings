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



static sync_strategy_t sync_strategy = sock_direct;

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
    opal_event_t *ev;
} clock_sync_t;

typedef struct {
    double rtt, bias;
} measurement_t;

char *state_to_str(clock_sync_state_t st);
inline static double timing_get_ts(void);
static int hnp_init_rml_direct(clock_sync_t *cs);
static int hnp_init_state(clock_sync_t **cs);
static int orted_init_rml_direct(clock_sync_t *cs);
static int orted_init_state(clock_sync_t **cs);
orte_process_name_t current_orted(clock_sync_t *cs);
orte_process_name_t next_orted(clock_sync_t *cs);
int responder_initiate(clock_sync_t *cs);
static int orted_init_measure(clock_sync_t *cs, opal_buffer_t *buffer);
inline static int put_ts(opal_buffer_t *buffer);
inline static int extract_ts(opal_buffer_t *buffer, double *ret);
static int send_measurement_request(clock_sync_t *cs);
static int process_measurement_reply(clock_sync_t *cs, orte_process_name_t* sender, opal_buffer_t *buffer);
static int serve_measurement_requests(clock_sync_t *cs,
                                orte_process_name_t* sender,
                                opal_buffer_t *buffer);
static int calculate_bias(clock_sync_t *cs, double *bias);
static void rml_callback(int status, orte_process_name_t* sender,
                      opal_buffer_t *buffer, orte_rml_tag_t tag,
                                            void* cbdata);

static int sock_measure_bias(clock_sync_t *cs);
static void sock_measure_responder(int fd, short flags, void* cbdata);
static int sock_connect_to_parent(clock_sync_t *cs);

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
                                  measure_status_t *state, opal_buffer_t **rbuffer )
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

    *rbuffer = OBJ_NEW(opal_buffer_t);
    if( ( rc = opal_dss.copy_payload(*rbuffer, buffer) ) ){
        ORTE_ERROR_LOG(rc);
        goto err_exit;
    }

    if( ( rc = put_ts(*rbuffer) ) ){
        goto err_exit;
    }
    return 0;

err_exit:
    OBJ_RELEASE(*rbuffer);
    return rc;
}


static int extract_measurement_reply(clock_sync_t *cs, opal_buffer_t *buffer)
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

    measurement_t mes = { rtt, bias }, *ptr;
    ptr = malloc(sizeof(measurement_t));
    *ptr = mes;
    if( ( rc = opal_pointer_array_add(cs->results, ptr) ) ){
        OPAL_ERROR_LOG(rc);
        return ORTE_ERROR;
    }

    CLKSYNC_OUTPUT( ("rtt = %15.15lf, bias = %15.15lf", rtt, bias) );

    return 0;
}

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


int responder_initiate(clock_sync_t *cs)
{
    int rc;

    orte_process_name_t dname = current_orted(cs);
    if( PROC_NAME_CMP(dname,orte_name_invalid) ){
        // Nothing to do, all orteds was served
        cs->state = finalized;
        return 0;
    }

    // For the measure initiate buffer
    opal_buffer_t *buffer = OBJ_NEW(opal_buffer_t);
    char *bias = malloc(sizeof(char) * BIAS_MAX_STR );
    sprintf(bias, "%15.15lf", cs->bias );
    opal_dss.pack(buffer, &bias, 1, OPAL_STRING );
    free(bias);

    switch( sync_strategy ){
    case rml_direct:
    case rml_tree:
        break;
    case sock_direct:
    case sock_tree:{
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

static int orted_init_measure(clock_sync_t *cs, opal_buffer_t *buffer)
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

    switch( sync_strategy ){
    case rml_direct:
    case rml_tree:
        break;
    case sock_direct:
    case sock_tree:{
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

//----------------- State machine initialization ----------------

static int hnp_init_rml_direct(clock_sync_t *cs)
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
    return 0;
}

static int create_listen_socket(int *ofd, unsigned short *oport)
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

static int hnp_init_sock_direct(clock_sync_t *cs)
{
    int rc;

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

    if( (cs->childs == NULL) || (cs->results == NULL) ){
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    orte_job_t *jorted = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
    int dsize = opal_pointer_array_get_size(jorted->procs);
    int i;

    /* setup the primary daemon command receive function */
    orte_proc_t *daemon=NULL;
    for(i = 1; i < dsize; i++){
        if( NULL == (daemon = (orte_proc_t*)opal_pointer_array_get_item(jorted->procs, i))) {
            // TODO: Can this happen?
            continue;
        }
        int rc;
        if( (rc = opal_pointer_array_add(cs->childs, daemon)) ){
            goto err_exit;
        }
    }

    if( (rc = create_listen_socket(&cs->fd, &cs->port) ) )
        goto err_exit;

    return 0;

err_exit:
    OBJ_RELEASE(cs->childs);
    OBJ_RELEASE(cs->results);
    return rc;
}


static int hnp_init_state(clock_sync_t **cs)
{
    *cs = malloc(sizeof(clock_sync_t));
    if( *cs == NULL ){
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    switch( sync_strategy ){
    case rml_direct:
        return hnp_init_rml_direct(*cs);
    case sock_direct:
        return hnp_init_sock_direct(*cs);
    default:
        return -1;
    }
}

static int orted_init_rml_direct(clock_sync_t *cs)
{
    memset(cs, 0, sizeof(*cs));
    cs->is_hnp = false;
    cs->state = init;
    cs->bias = 0;
    cs->par_bias = 0;
    cs->cur_daemon = 0;
    cs->childs = OBJ_NEW(opal_pointer_array_t);
    cs->req_state = bias_in_progress;
    cs->parent = *ORTE_PROC_MY_HNP;

    if( cs->childs == NULL ){
        return ORTE_ERR_OUT_OF_RESOURCE;
    }

    return 0;
}

static int orted_init_sock_direct(clock_sync_t *cs)
{
    memset(cs, 0, sizeof(*cs));
    cs->is_hnp = false;
    cs->state = init;
    cs->bias = 0;
    cs->par_bias = 0;
    cs->cur_daemon = 0;
    cs->childs = OBJ_NEW(opal_pointer_array_t);
    cs->req_state = bias_in_progress;
    cs->parent = *ORTE_PROC_MY_HNP;

    if( cs->childs == NULL ){
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

    switch( sync_strategy ){
    case rml_direct:
        return orted_init_rml_direct(*cs);
    case sock_direct:
        return orted_init_sock_direct(*cs);
    default:
        return -1;
    }
}


static int send_measurement_request(clock_sync_t *cs)
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


static int process_measurement_reply(clock_sync_t *cs, orte_process_name_t* sender, opal_buffer_t *buffer)
{
    int rc;

    CLKSYNC_OUTPUT( ("process %s reply", ORTE_NAME_PRINT(sender)) );

    if( ( rc = extract_measurement_reply(cs, buffer) ) ){
        return rc;
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

static int serve_measurement_requests(clock_sync_t *cs,
                                orte_process_name_t* sender,
                                opal_buffer_t *buffer)
{
    int rc;
    measure_status_t state;
    opal_buffer_t *rbuffer;

    CLKSYNC_OUTPUT( ("called by %s", ORTE_NAME_PRINT(sender)) );

    if( ( rc = form_measurement_reply(cs, buffer, &state, &rbuffer ) ) ){
        return rc;
    }

    if( state == bias_calculated ){
        orte_process_name_t next = next_orted(cs);
        if( PROC_NAME_CMP(next, orte_name_invalid) ){
            cs->state = finalized;
        }else{
            cs->state = resp_init;
        }
        return 0;
    }

    if (0 > (rc = orte_rml.send_buffer_nb(sender, rbuffer, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        goto err_exit;
    }

    CLKSYNC_OUTPUT( ("reply to measurement request from %s", ORTE_NAME_PRINT(sender)) );
    return 0;
err_exit:
    OBJ_RELEASE(rbuffer);
    return ORTE_ERROR;
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


static void rml_callback(int status, orte_process_name_t* sender,
                      opal_buffer_t *buffer, orte_rml_tag_t tag,
                                            void* cbdata)
{
    clock_sync_t *cs = cbdata;

    switch(cs->state){
    case init:
        orted_init_measure(cs, buffer);
        cs->state = req_measure;
        if( send_measurement_request(cs) ){
            cs->state = finalized;
        }
        break;
    case req_measure:
        if( process_measurement_reply(cs, sender, buffer) ){
            cs->state = finalized;
        }
        break;
    case resp_serve:
        serve_measurement_requests(cs, sender, buffer);
        break;
    case resp_init:
        break;
    default:
        CLKSYNC_OUTPUT( ("This state is not allowed here: %s", state_to_str(cs->state)) );
        break;
    }


    switch(cs->state){
    case req_measure:
        if( send_measurement_request(cs) ){
            cs->state = finalized;
        }
        break;
    case resp_init:
        responder_initiate(cs);
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

static void sock_callback(int status, orte_process_name_t* sender,
                      opal_buffer_t *buffer, orte_rml_tag_t tag,
                                            void* cbdata)
{
    clock_sync_t *cs = cbdata;

    if(cs->state != init ){
        CLKSYNC_OUTPUT( ("This state is not allowed here: %s", state_to_str(cs->state)) );
        return;
    }

    orted_init_measure(cs, buffer);

    if( sock_measure_bias(cs) ){
        CLKSYNC_OUTPUT( ("Cannot measure bias") );
        return;
    }

    // If this process has childrens assigned - prepare to test them
    if( opal_pointer_array_get_size(cs->childs) ){
        // setup working socket event
        cs->ev = opal_event_alloc();
        opal_event_set(opal_event_base, cs->ev, cs->fd, OPAL_EV_READ | OPAL_EV_PERSIST,
                        sock_measure_responder, cs);
        opal_event_add(cs->ev,0);

        if( responder_initiate(cs) ){
            CLKSYNC_OUTPUT( ("Cannot initiate responder") );
            return;
        }
        cs->state = resp_serve;
    }

    CLKSYNC_OUTPUT( ("callback is called, final state = %s\n", state_to_str(cs->state)) );
}

static int peer_addrs_list(orte_process_name_t *peer, opal_list_t *list, uint16_t *afamily)
{
    char *uri = NULL;
    char *host, *ports;
    opal_list_t myvals;
    OBJ_CONSTRUCT(&myvals, opal_list_t);
    if (OPAL_SUCCESS != opal_dstore.fetch(opal_dstore_internal,
                                          (opal_identifier_t*)peer,
                                          ORTE_DB_RMLURI, &myvals)) {
        return ORTE_ERROR;
    }
    opal_value_t *kv = (opal_value_t*)opal_list_get_first(&myvals);
    uri = strdup(kv->data.string);
    OPAL_LIST_DESTRUCT(&myvals);

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

    /* separate the ports from the network addrs */
    ports = strrchr(uri, ':');
    *ports = '\0';

    /* split the addrs */
    if (NULL == host || 0 == strlen(host)) {
        return ORTE_ERR_BAD_PARAM;
    }

    /* if this is a tcp6 connection, the first one will have a '['
     * at the beginning of it, and the last will have a ']' at the
     * end - we need to remove those extra characters
     */
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

    /* cycle across the provided addrs */
    int j;
    for (j=0; NULL != addrs[j]; j++) {
        opal_list_append(list, (void*)addrs[j]);
    }
    free(addrs);
    *afamily = af_family;
    return 0;
}

static int sock_connect_to_parent(clock_sync_t *cs)
{
    orte_process_name_t par = orte_routed.get_route(ORTE_PROC_MY_HNP);
    opal_list_t *addrs = OBJ_NEW(opal_list_t);
    uint16_t af_family;
    int rc;

    if( peer_addrs_list(&par, addrs, &af_family) ){
        CLKSYNC_OUTPUT( ("CLOCK SYNC: cannot get parent URI list") );
        return ORTE_ERROR;
    }

    int fd = socket(af_family, SOCK_STREAM, 0);
    if( fd < 0 ){
        CLKSYNC_OUTPUT( ("CLOCK SYNC: cannot create socket") );
        goto err_exit1;
    }

    int connected = 0;
    char *addr;
    OPAL_LIST_FOREACH(addr, addrs, char) {
        struct sockaddr *sa;
        int alen = 0;
        if( af_family == AF_INET ){
            struct sockaddr_in *sin = malloc(sizeof(struct sockaddr_in));
            if( sin == NULL ){
                goto err_exit2;
            }
            sin->sin_port = cs->par_port;
            rc = inet_pton(af_family, addr, &sin->sin_addr.s_addr);
            if( rc <= 0 ){
                if( rc == 0 ){
                    CLKSYNC_OUTPUT( ("CLOCK SYNC: cannot convert address %s to binary form: bad format", addr) );
                }else{
                    CLKSYNC_OUTPUT( ("CLOCK SYNC: cannot convert address %s to binary form: %s",
                                     addr, strerror(errno)) );
                }
                return ORTE_ERROR;
            }
            sa = (struct sockaddr*)sin;
            alen = sizeof(*sin);
        }
        if( connect(fd, sa, alen) < 0) {
            CLKSYNC_OUTPUT( ("CLOCK SYNC: error connecting to %s: %s",
                             addr, strerror(errno)) );
            continue;
        }
        connected = 1;
        break;

    }

    if( connected ){
        OBJ_RELEASE(addrs);
        return fd;
    }

err_exit2:
    close(fd);
err_exit1:
    OBJ_RELEASE(addrs);
    return ORTE_ERROR;
}

static int sock_measure_bias(clock_sync_t *cs)
{
    int rc;
    if( cs->req_state != bias_calculated ){
        int fd = sock_connect_to_parent(cs);

    }

}

static void sock_measure_responder(int fd, short flags, void* cbdata)
{
    clock_sync_t *cs = cbdata;
    struct sockaddr_in addr;
    opal_socklen_t addrlen = sizeof(addr);

    int cli_fd = accept(fd, (struct sockaddr*)&addr, &addrlen);
    if(cli_fd < 0) {
        CLKSYNC_OUTPUT( ("Cannot accept: %s", strerror(errno)) );
        return;
    }


}


int orte_util_clock_sync_hnp_init(orte_state_caddy_t *caddy)
{

    debug_hang(1);

    clock_sync_t *cs;
    if( hnp_init_state(&cs) ){
        return -1;
    }
    cs->caddy = caddy;

    switch( sync_strategy ){
    case rml_direct:
    case rml_tree:
        if( opal_pointer_array_get_size(cs->childs) ){
            orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                    ORTE_RML_PERSISTENT, rml_callback, cs);
            if( responder_initiate(cs) ){
                goto err_exit;
            }
        }else{
            goto err_exit;
        }
        break;
    case sock_direct:
    case sock_tree:
        if( opal_pointer_array_get_size(cs->childs) ){
            // setup working socket event
            cs->ev = opal_event_alloc();
            opal_event_set(opal_event_base, cs->ev, cs->fd, OPAL_EV_READ | OPAL_EV_PERSIST,
                            sock_measure_responder, cs);
            opal_event_add(cs->ev,0);

            if( responder_initiate(cs) ){
                goto err_exit;
            }
        }
    default:
        opal_output(0,"BAD sync_strategy VALUE %d!", (int)sync_strategy);
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

    switch( sync_strategy ){
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
        opal_output(0,"BAD sync_strategy VALUE %d!", (int)sync_strategy);
        return -1;
    }

    CLKSYNC_OUTPUT( ("callback is installed") );
    return 0;

err_exit:
    free(cs);
    return -1;
}
