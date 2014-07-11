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

#include "opal/util/output.h"
#include "orte/util/clock_sync.h"

#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/routed/routed.h"


#define MAX_COUNT 100

typedef enum { req_init, req_measure, req_finalized, resp_init, resp_switch, resp_serve, resp_finalized } clock_sync_state_t;


typedef struct {
    clock_sync_state_t req_state;
    int next_daemon;
    double bias, par_bias;
    opal_pointer_array_t childs;
} clock_sync_t;

char *req_state_to_str(clock_sync_state_t st)
{
    switch(st){
    case req_init: return "req_init"; break;
    case req_measure: return "req_measure"; break;
    case req_finalized: return "req_finalized"; break;
    case resp_init: return "resp_init"; break;
    case resp_switch: return "resp_switch"; break;
    case resp_serve: return "resp_serve"; break;
    case resp_finalized: return "resp_finalized"; break;
    }
    return NULL;
}

double opal_timing_get_ts(){
    struct timeval tv;
    gettimeofday(&tv,NULL);
    double ret = tv.tv_sec + tv.tv_usec*1E-6;
    return ret;
}

static int send_measurement_request(orte_process_name_t *dname, clock_sync_state_t status)
{
    opal_buffer_t *buffer = OBJ_NEW(opal_buffer_t);
    int rc;
    
    uint32_t tbuf = status;
    /* insert our name for rollup purposes */
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &tbuf, 1, OPAL_UINT32 ))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return -1;
    }


    struct timeval tv;
    gettimeofday(&tv,NULL);
    tbuf = tv.tv_sec;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &tbuf, 1, OPAL_UINT32 ))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return -1;
    }
    tbuf = tv.tv_usec;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(buffer, &tbuf, 1, OPAL_UINT32 ))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return -1;
    }

    if (0 > (rc = orte_rml.send_buffer_nb(dname, buffer, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return -1;
    }
    opal_output(0,"%s: send measurement request to %s.\n",ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(dname));
}

static void reply_measurement_request(int status, orte_process_name_t* sender,
                      opal_buffer_t *buffer, orte_rml_tag_t tag,
                                            void* cbdata)
{
    hnp_peer_status *pstat = cbdata;
    opal_buffer_t *newbuffer = OBJ_NEW(opal_buffer_t);

    opal_output(0,"%s [%s]: called by %s.\n",ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
            __FUNCTION__,ORTE_NAME_PRINT(sender));
    
    opal_dss.copy_payload(newbuffer, buffer);

    // Check for measurement completition
    int idx = 1, rc;
    uint32_t tbuf;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &tbuf, &idx, OPAL_UINT32)) ) {
        // TODO: handle bad buffer format
    }
    clock_sync_state_t state = tbuf;
    if( state == finished ){
        pstat->finished = true;
        goto exit;
    }


    struct timeval tv;
    gettimeofday(&tv,NULL);
    tbuf = tv.tv_sec;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(newbuffer, &tbuf, 1, OPAL_UINT32 ))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return;
    }
    tbuf = tv.tv_usec;
    if (ORTE_SUCCESS != (rc = opal_dss.pack(newbuffer, &tbuf, 1, OPAL_UINT32 ))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        return;
    }

    if (0 > (rc = orte_rml.send_buffer_nb(sender, newbuffer, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                          orte_rml_send_callback, NULL))) {
        ORTE_ERROR_LOG(rc);
        OBJ_RELEASE(buffer);
        goto exit;
    }
    
    opal_output(0,"%s: reply to measurement request to %s.\n",ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(sender));
exit:
    pstat->sync_wait = false;
    return;
}

static void process_measurement_reply(orte_process_name_t* sender, opal_buffer_t *buffer, double *rtt, double *bias)
{

    int idx = 1, rc;
    char ts_str[32];
    double lsnd, rsnd, lrcv = opal_timing_get_ts();

    // remove status field
    uint32_t tbuf;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &tbuf, &idx, OPAL_UINT32))) {
        // TODO: handle bad buffer format
    }

    struct timeval tv;
    idx = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &tbuf, &idx, OPAL_UINT32))) {
        // TODO: handle bad buffer format
    }
    tv.tv_sec = tbuf;
    idx = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &tbuf, &idx, OPAL_UINT32))) {
        // TODO: handle bad buffer format
    }
    tv.tv_usec = tbuf;
    lsnd = tv.tv_sec + tv.tv_usec*1E-6;

    idx = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &tbuf, &idx, OPAL_UINT32))) {
        // TODO: handle bad buffer format
    }
    tv.tv_sec = tbuf;
    idx = 1;
    if (OPAL_SUCCESS != (rc = opal_dss.unpack(buffer, &tbuf, &idx, OPAL_UINT32))) {
        // TODO: handle bad buffer format
    }
    tv.tv_usec = tbuf;
    rsnd = tv.tv_sec + tv.tv_usec*1E-6;

    *rtt = lrcv - lsnd;
    *bias = rsnd - (lsnd + (*rtt)/2);

    opal_output(0,"%s(%s): process %s reply: rtt = %15.15lf, bias = %15.15lf\n",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __FUNCTION__,
            ORTE_NAME_PRINT(sender), rtt, bias);
}



void orte_util_clock_recv(int status, orte_process_name_t* sender,
                      opal_buffer_t *buffer, orte_rml_tag_t tag,
                                            void* cbdata)
{
    static clock_sync_state_t state = begin;
    static int count = 0;
    static double min_rtt = 1E20;
    static double final_bias = 0;
    orte_process_name_t dname;

    opal_output(0, "%s[%s]: callback is called, state = %s\n",ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __FUNCTION__, state_to_str(state));

{
    static int i = 1;
    while( i ){
        sleep(1);
    }
}

    switch( state ){
    case begin:{
        int idx = 1, rc;
        if (OPAL_SUCCESS == (rc = opal_dss.unpack(buffer, &dname, &idx, ORTE_NAME))) {
            opal_output(0, "%s: %s contacted me!\n",ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&dname));
        }
        state = measure;
        break;
    }
    case measure:{
        char ts[64];
        int idx = 1, rc;
        double rtt, bias;
        process_measurement_reply(sender, buffer, &rtt, &bias);
        if( rtt < min_rtt ){
            min_rtt = rtt;
            bool *sync_waiting = (bool*)cbdata;
            final_bias = bias;
        }
        if( count > MAX_COUNT )
            state = finished;
        count++;
        break;
    }
    }

    send_measurement_request(sender, state);
    if( state == finished ){
        opal_output(0, "%s: final bias relative to %s is %15.15lf/%15.15lf\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(sender),
                final_bias, min_rtt);
    }

}

int orte_util_clock_sync_init()
{
    /* setup the primary daemon command receive function */
    orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                            ORTE_RML_PERSISTENT, orte_util_clock_recv, NULL);
    opal_output(0, "%s[%s]: callback is installed\n",ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __FILE__);
}


static void my_rml_send_callback(int status, orte_process_name_t* sender,
                      opal_buffer_t *buffer, orte_rml_tag_t tag,
                                            void* cbdata)
{
    opal_output(0, "%s[%s]: send callback is called\n",ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __FILE__);
    orte_rml_send_callback(status, sender, buffer, tag, cbdata);
}

int orte_util_clock_sync_hnp_part()
{
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
        opal_output(0, "%d: %s\n",i, daemon->rml_uri);
        int ret;
        orte_process_name_t dname = *ORTE_PROC_MY_NAME;
        dname.vpid = i;
        opal_buffer_t *buffer = OBJ_NEW(opal_buffer_t);
        /* insert our name for rollup purposes */
        if (ORTE_SUCCESS != (ret = opal_dss.pack(buffer, ORTE_PROC_MY_NAME, 1, ORTE_NAME))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(buffer);
            return -1;
        }
        
        opal_output(0,"%s: start measurement with %s!\n",ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&dname));


        orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                     ORTE_RML_PERSISTENT, reply_measurement_request, NULL);


        if (0 > (ret = orte_rml.send_buffer_nb(&dname, buffer, ORTE_RML_TAG_TIMING_CLOCK_SYNC,
                                              my_rml_send_callback, NULL))) {
            ORTE_ERROR_LOG(ret);
            OBJ_RELEASE(buffer);
            return -1;
        }
        
    }
    return 0;
}