/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2008 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Voltaire All rights reserved.
 * Copyright (c) 2006-2009 University of Houston.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/dss/dss.h"
#include "orte/types.h"
#include "ompi/communicator/communicator.h"
#include "ompi/op/op.h"
#include "ompi/constants.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_list.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/request/request.h"
#include "ompi/runtime/mpiruntime.h"
#include "ompi/mca/dpm/dpm.h"

#include "orte/mca/rml/rml.h"

BEGIN_C_DECLS

/**
 * These functions make sure, that we determine the global result over 
 * an intra communicators (simple), an inter-communicator and a
 * pseudo inter-communicator described by two separate intra-comms
 * and a bridge-comm (intercomm-create scenario).
 */

static int cid_block_start = 28;

static int ompi_comm_cid_checkforreuse ( int c_id_start_index, int block );


typedef int ompi_comm_cid_allredfct (int *inbuf, int* outbuf, 
                                     int count, struct ompi_op_t *op, 
                                     ompi_communicator_t *comm,
                                     ompi_communicator_t *bridgecomm, 
                                     void* lleader, void* rleader, 
                                     int send_first );

static int ompi_comm_allreduce_intra (int *inbuf, int* outbuf, 
                                      int count, struct ompi_op_t *op, 
                                      ompi_communicator_t *intercomm,
                                      ompi_communicator_t *bridgecomm, 
                                      void* local_leader, 
                                      void* remote_ledaer,
                                      int send_first );

static int ompi_comm_allreduce_inter (int *inbuf, int *outbuf, 
                                      int count, struct ompi_op_t *op, 
                                      ompi_communicator_t *intercomm,
                                      ompi_communicator_t *bridgecomm, 
                                      void* local_leader, 
                                      void* remote_leader,
                                      int send_first );

static int ompi_comm_allreduce_intra_bridge(int *inbuf, int* outbuf, 
                                            int count, struct ompi_op_t *op, 
                                            ompi_communicator_t *intercomm,
                                            ompi_communicator_t *bridgecomm, 
                                            void* local_leader, 
                                            void* remote_leader,
                                            int send_first);

static int ompi_comm_allreduce_intra_oob (int *inbuf, int* outbuf, 
                                          int count, struct ompi_op_t *op, 
                                          ompi_communicator_t *intercomm,
                                          ompi_communicator_t *bridgecomm, 
                                          void* local_leader, 
                                          void* remote_leader, 
                                          int send_first );

static int      ompi_comm_register_cid (uint32_t contextid);
static int      ompi_comm_unregister_cid (uint32_t contextid);
static uint32_t ompi_comm_lowest_cid ( void );

struct ompi_comm_reg_t{
    opal_list_item_t super;
    uint32_t           cid;
};
typedef struct ompi_comm_reg_t ompi_comm_reg_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_comm_reg_t);

static void ompi_comm_reg_constructor(ompi_comm_reg_t *regcom);
static void ompi_comm_reg_destructor(ompi_comm_reg_t *regcom);

OBJ_CLASS_INSTANCE (ompi_comm_reg_t,
                    opal_list_item_t,
                    ompi_comm_reg_constructor,
                    ompi_comm_reg_destructor );

static opal_mutex_t ompi_cid_lock;
static opal_list_t ompi_registered_comms;


int ompi_comm_nextcid ( ompi_communicator_t* newcomm, 
                        ompi_communicator_t* comm, 
                        ompi_communicator_t* bridgecomm, 
                        void* local_leader,
                        void* remote_leader,
                        int mode, int send_first )
{
    int nextcid, block;
    int global_block_start;
    bool flag;
    
    ompi_comm_cid_allredfct* allredfnct;

    /** 
     * Determine which implementation of allreduce we have to use
     * for the current scenario 
     */
    switch (mode) 
    {
        case OMPI_COMM_CID_INTRA: 
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra;
            break;
        case OMPI_COMM_CID_INTER:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_inter;
            break;
        case OMPI_COMM_CID_INTRA_BRIDGE: 
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_bridge;
            break;
        case OMPI_COMM_CID_INTRA_OOB: 
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_oob;
            break;
        default: 
            return MPI_UNDEFINED;
            break;
    }

    /**
     * In case multi-threading is enabled, we revert to the old algorithm
     * starting from cid_block_start
     */
    if (MPI_THREAD_MULTIPLE == ompi_mpi_thread_provided) {
        int nextlocal_cid;
        int done=0;
        int response, glresponse=0;
        int start;
        unsigned int i;
        
        do {
            /* Only one communicator function allowed in same time on the
             * same communicator.
             */
            OPAL_THREAD_LOCK(&ompi_cid_lock);
            response = ompi_comm_register_cid (comm->c_contextid);
            OPAL_THREAD_UNLOCK(&ompi_cid_lock);
        } while (OMPI_SUCCESS != response );
        start = ompi_mpi_communicators.lowest_free;

        while (!done) {
            /**
             * This is the real algorithm described in the doc 
             */
            
            OPAL_THREAD_LOCK(&ompi_cid_lock);
            if (comm->c_contextid != ompi_comm_lowest_cid() ) {
                /* if not lowest cid, we do not continue, but sleep and try again */
                OPAL_THREAD_UNLOCK(&ompi_cid_lock);
                continue;
            }
            OPAL_THREAD_UNLOCK(&ompi_cid_lock);
            
            
            for (i=start; i < mca_pml.pml_max_contextid ; i++) {
                flag=opal_pointer_array_test_and_set_item(&ompi_mpi_communicators, 
                                                          i, comm);
                if (true == flag) {
                    nextlocal_cid = i;
                    break;
                }
            }
            
            (allredfnct)(&nextlocal_cid, &nextcid, 1, MPI_MAX, comm, bridgecomm,
                         local_leader, remote_leader, send_first );
            if (nextcid == nextlocal_cid) {
                response = 1; /* fine with me */
            }
            else {
                opal_pointer_array_set_item(&ompi_mpi_communicators, 
                                            nextlocal_cid, NULL);
                
                flag = opal_pointer_array_test_and_set_item(&ompi_mpi_communicators, 
                                                            nextcid, comm );
                if (true == flag) {
                    response = 1; /* works as well */
                }
                else {
                    response = 0; /* nope, not acceptable */
                }
            }
            
            (allredfnct)(&response, &glresponse, 1, MPI_MIN, comm, bridgecomm,
                         local_leader, remote_leader, send_first );
            if (1 == glresponse) {
                done = 1;             /* we are done */
                break;
            }
            else if ( 0 == glresponse ) {
                if ( 1 == response ) {
                    /* we could use that, but other don't agree */
                    opal_pointer_array_set_item(&ompi_mpi_communicators, 
                                                nextcid, NULL);
                }
                start = nextcid+1; /* that's where we can start the next round */
            }
        }
        
        /* set the according values to the newcomm */
        newcomm->c_contextid = nextcid;
        newcomm->c_f_to_c_index = newcomm->c_contextid;
        opal_pointer_array_set_item (&ompi_mpi_communicators, nextcid, newcomm);
        
        OPAL_THREAD_LOCK(&ompi_cid_lock);
        ompi_comm_unregister_cid (comm->c_contextid);
        OPAL_THREAD_UNLOCK(&ompi_cid_lock);
        
        return (MPI_SUCCESS);
    }

     /**
      * In case the communication mode is INTRA_OOB or INTAR_BRIDGE, we use the 
      * highest-free algorithm
      */
    if ( OMPI_COMM_CID_INTRA_OOB == mode || OMPI_COMM_CID_INTRA_BRIDGE == mode) {
        (allredfnct)(&cid_block_start, &global_block_start, 1, 
                     MPI_MAX, comm, bridgecomm,
                     local_leader, remote_leader, send_first );
        cid_block_start = global_block_start;
        nextcid = cid_block_start;
        cid_block_start = cid_block_start + 1;
    }
    else {
        flag=false;
        block = 0;
        if( 0 == comm->c_contextid ) {
            block = OMPI_COMM_BLOCK_WORLD;
        }
        else {
            block = OMPI_COMM_BLOCK_OTHERS;
        }

        while(!flag) {
            /**
             * If the communicator has IDs available then allocate one for the child
             */
            if ( MPI_UNDEFINED != comm->c_id_available && 
                 MPI_UNDEFINED != comm->c_id_start_index &&  
                 block > comm->c_id_available - comm->c_id_start_index) {
                nextcid = comm->c_id_available;
                flag=opal_pointer_array_test_and_set_item (&ompi_mpi_communicators,
                                                           nextcid, comm);
            }
            /**
             * Otherwise the communicator needs to negotiate a new block of IDs
             */
            else {
		int start[3], gstart[3];
		/* the next function either returns exactly the same start_id as 
                   the communicator had, or the cid_block_start*/
		start[0] = ompi_comm_cid_checkforreuse ( comm->c_id_start_index, block );

		/* this is now a little tricky. By multiplying the start[0] values with -1
		   and executing the MAX operation on those as well, we will be able to
		   determine the minimum value across the provided input */ 
		start[1] = (-1) * start[0]; 
		start[2] = cid_block_start;

		(allredfnct)(start, gstart, 3, MPI_MAX, comm, bridgecomm,
                             local_leader, remote_leader, send_first );

		/* revert the minimum value back to a positive number */
		gstart[1] = (-1) * gstart[1];
		
		if  ( gstart[0] == start[0] && 
		      gstart[1] == start[0] && 
		      gstart[0] != cid_block_start ) {
		    comm->c_id_available   = gstart[0];
		    comm->c_id_start_index = gstart[0];

		    /* note: cid_block_start not modified in this section */
		}
		else {
		    /* no, one process did not agree on the reuse of the block
		       so we have to go with the higher number */
		    comm->c_id_available   = gstart[2];
		    comm->c_id_start_index = gstart[2];
		    cid_block_start = gstart[2] + block;
		}
            }
        }
        
        comm->c_id_available++;
    }
    /* set the according values to the newcomm */
    newcomm->c_contextid = nextcid;
    newcomm->c_f_to_c_index = newcomm->c_contextid;
    opal_pointer_array_set_item (&ompi_mpi_communicators, nextcid, newcomm);

    return (MPI_SUCCESS);

}

/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
static void ompi_comm_reg_constructor (ompi_comm_reg_t *regcom)
{
    regcom->cid=MPI_UNDEFINED;
}

static void ompi_comm_reg_destructor (ompi_comm_reg_t *regcom)
{
}

void ompi_comm_reg_init (void)
{
    OBJ_CONSTRUCT(&ompi_registered_comms, opal_list_t);
    OBJ_CONSTRUCT(&ompi_cid_lock, opal_mutex_t);
}

void ompi_comm_reg_finalize (void)
{
    OBJ_DESTRUCT(&ompi_registered_comms);
    OBJ_DESTRUCT(&ompi_cid_lock);
}


static int ompi_comm_register_cid (uint32_t cid )
{
    opal_list_item_t *item;
    ompi_comm_reg_t *regcom;
    ompi_comm_reg_t *newentry = OBJ_NEW(ompi_comm_reg_t);

    newentry->cid = cid;
    if ( !(opal_list_is_empty (&ompi_registered_comms)) ) {
        for (item = opal_list_get_first(&ompi_registered_comms);
             item != opal_list_get_end(&ompi_registered_comms);
             item = opal_list_get_next(item)) {
            regcom = (ompi_comm_reg_t *)item;
            if ( regcom->cid > cid ) {
                break;
            }
#if OMPI_ENABLE_MPI_THREADS
            if( regcom->cid == cid ) {
                /**
                 * The MPI standard state that is the user responsability to
                 * schedule the global communications in order to avoid any
                 * kind of troubles. As, managing communicators involve several
                 * collective communications, we should enforce a sequential
                 * execution order. This test only allow one communicator
                 * creation function based on the same communicator.
                 */
                OBJ_RELEASE(newentry);
                return OMPI_ERROR;
            }
#endif  /* OMPI_ENABLE_MPI_THREADS */
        }
        opal_list_insert_pos (&ompi_registered_comms, item, 
                              (opal_list_item_t *)newentry);
    }
    else {
        opal_list_append (&ompi_registered_comms, (opal_list_item_t *)newentry);
    }

    return OMPI_SUCCESS;
}

static int ompi_comm_unregister_cid (uint32_t cid)
{
    ompi_comm_reg_t *regcom;
    opal_list_item_t *item;

    for (item = opal_list_get_first(&ompi_registered_comms);
         item != opal_list_get_end(&ompi_registered_comms);
         item = opal_list_get_next(item)) {
        regcom = (ompi_comm_reg_t *)item;
        if(regcom->cid == cid) {
            opal_list_remove_item(&ompi_registered_comms, item);
            OBJ_RELEASE(regcom);
            break;
        }
    }
    return OMPI_SUCCESS;
}

static uint32_t ompi_comm_lowest_cid (void)
{
    ompi_comm_reg_t *regcom=NULL;
    opal_list_item_t *item=opal_list_get_first (&ompi_registered_comms);

    regcom = (ompi_comm_reg_t *)item;
    return regcom->cid;
}
/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
/* This routine serves two purposes:
 * - the allreduce acts as a kind of Barrier,
 *   which avoids, that we have incoming fragments 
 *   on the new communicator before everybody has set
 *   up the comm structure.
 * - some components (e.g. the collective MagPIe component
 *   might want to generate new communicators and communicate
 *   using the new comm. Thus, it can just be called after
 *   the 'barrier'.
 *
 * The reason that this routine is in comm_cid and not in
 * comm.c is, that this file contains the allreduce implementations
 * which are required, and thus we avoid having duplicate code...
 */
int ompi_comm_activate ( ompi_communicator_t** newcomm )
{
    int ret = 0;

    /**
     * Check to see if this process is in the new communicator.
     *
     * Specifically, this function is invoked by all proceses in the
     * old communicator, regardless of whether they are in the new
     * communicator or not.  This is because it is far simpler to use
     * MPI collective functions on the old communicator to determine
     * some data for the new communicator (e.g., remote_leader) than
     * to kludge up our own pseudo-collective routines over just the
     * processes in the new communicator.  Hence, *all* processes in
     * the old communicator need to invoke this function.
     *
     * That being said, only processes in the new communicator need to
     * select a coll module for the new communicator.  More
     * specifically, proceses who are not in the new communicator
     * should *not* select a coll module -- for example,
     * ompi_comm_rank(newcomm) returns MPI_UNDEFINED for processes who
     * are not in the new communicator.  This can cause errors in the
     * selection / initialization of a coll module.  Plus, it's
     * wasteful -- processes in the new communicator will end up
     * freeing the new communicator anyway, so we might as well leave
     * the coll selection as NULL (the coll base comm unselect code
     * handles that case properly).
     */
    if (MPI_UNDEFINED == (*newcomm)->c_local_group->grp_my_rank) {
        return OMPI_SUCCESS;
    }
    /* Initialize the PML stuff in the newcomm  */
    if ( OMPI_SUCCESS != (ret = MCA_PML_CALL(add_comm(*newcomm))) ) {
        goto bail_on_error;
    }
    OMPI_COMM_SET_PML_ADDED(*newcomm);

    /* Let the collectives components fight over who will do
       collective on this new comm.  */
    if (OMPI_SUCCESS != (ret = mca_coll_base_comm_select(*newcomm))) {
        goto bail_on_error;
    }
    return OMPI_SUCCESS;

 bail_on_error:
    OBJ_RELEASE(*newcomm);
    *newcomm = MPI_COMM_NULL;
    return ret;
}                         

/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
/* check whether all communicators registered from c_id_start_index to 
** c_id_start_index + block have been freed. For this, we rely on 
** the communicators having been properly removed from the fortran array, 
** i.e. the according request should return a NULL pointer. 
*/
static int ompi_comm_cid_checkforreuse ( int c_id_start_index, int block )
{
    int ret=cid_block_start;
    int i, count=0;
    ompi_communicator_t * tempcomm;


    if ( MPI_UNDEFINED != c_id_start_index ) {
	for ( i= c_id_start_index; i < c_id_start_index + block; i++ ) {
	    tempcomm = (ompi_communicator_t *) opal_pointer_array_get_item ( &ompi_mpi_communicators, i );
	    if ( NULL == tempcomm ) {
		count++;
	    }
	}
	
	if ( count == block ) {
	    ret = c_id_start_index;
	}
    }

    return ret;
}
/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
/* Arguments not used in this implementation:
 *  - bridgecomm
 *  - local_leader
 *  - remote_leader
 *  - send_first
 */
static int ompi_comm_allreduce_intra ( int *inbuf, int *outbuf, 
                                       int count, struct ompi_op_t *op, 
                                       ompi_communicator_t *comm,
                                       ompi_communicator_t *bridgecomm, 
                                       void* local_leader, 
                                       void* remote_leader, 
                                       int send_first )
{
    return comm->c_coll.coll_allreduce ( inbuf, outbuf, count, MPI_INT, 
                                         op,comm,
                                         comm->c_coll.coll_allreduce_module );
}

/* Arguments not used in this implementation:
 *  - bridgecomm
 *  - local_leader
 *  - remote_leader
 *  - send_first
 */
static int ompi_comm_allreduce_inter ( int *inbuf, int *outbuf, 
                                       int count, struct ompi_op_t *op, 
                                       ompi_communicator_t *intercomm,
                                       ompi_communicator_t *bridgecomm, 
                                       void* local_leader, 
                                       void* remote_leader, 
                                       int send_first )
{
    int local_rank, rsize;
    int i, rc;
    int *sbuf;
    int *tmpbuf=NULL;
    int *rcounts=NULL, scount=0;
    int *rdisps=NULL;

    if ( &ompi_mpi_op_sum.op != op && &ompi_mpi_op_prod.op != op &&
         &ompi_mpi_op_max.op != op && &ompi_mpi_op_min.op  != op ) {
        return MPI_ERR_OP;
    }

    if ( !OMPI_COMM_IS_INTER (intercomm)) {
        return MPI_ERR_COMM;
    }

    /* Allocate temporary arrays */
    rsize      = ompi_comm_remote_size (intercomm);
    local_rank = ompi_comm_rank ( intercomm );

    tmpbuf  = (int *) malloc ( count * sizeof(int));
    rdisps  = (int *) calloc ( rsize, sizeof(int));
    rcounts = (int *) calloc ( rsize, sizeof(int) );
    if ( OPAL_UNLIKELY (NULL == tmpbuf || NULL == rdisps || NULL == rcounts)) {
        rc = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }
    
    /* Execute the inter-allreduce: the result of our group will
       be in the buffer of the remote group */
    rc = intercomm->c_coll.coll_allreduce ( inbuf, tmpbuf, count, MPI_INT,
                                            op, intercomm,
                                            intercomm->c_coll.coll_allreduce_module);
    if ( OMPI_SUCCESS != rc ) {
        goto exit;
    }

    if ( 0 == local_rank ) {
        MPI_Request req;

        /* for the allgatherv later */
        scount = count;

        /* local leader exchange their data and determine the overall result
           for both groups */
        rc = MCA_PML_CALL(irecv (outbuf, count, MPI_INT, 0, 
                                OMPI_COMM_ALLREDUCE_TAG
                                , intercomm, &req));
        if ( OMPI_SUCCESS != rc ) {
            goto exit;
        }
        rc = MCA_PML_CALL(send (tmpbuf, count, MPI_INT, 0,
                               OMPI_COMM_ALLREDUCE_TAG, 
                               MCA_PML_BASE_SEND_STANDARD, intercomm));
        if ( OMPI_SUCCESS != rc ) {
            goto exit;
        }
        rc = ompi_request_wait_all ( 1, &req, MPI_STATUS_IGNORE );
        if ( OMPI_SUCCESS != rc ) {
            goto exit;
        }

        if ( &ompi_mpi_op_max.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                if (tmpbuf[i] > outbuf[i]) outbuf[i] = tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_min.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                if (tmpbuf[i] < outbuf[i]) outbuf[i] = tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_sum.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                outbuf[i] += tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_prod.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                outbuf[i] *= tmpbuf[i];
            }
        }
    }
 
    /* distribute the overall result to all processes in the other group.
       Instead of using bcast, we are using here allgatherv, to avoid the
       possible deadlock. Else, we need an algorithm to determine, 
       which group sends first in the inter-bcast and which receives 
       the result first.
    */
    rcounts[0] = count;
    sbuf       = outbuf;
    rc = intercomm->c_coll.coll_allgatherv (sbuf, scount, MPI_INT, outbuf,
                                            rcounts, rdisps, MPI_INT, 
                                            intercomm,
                                            intercomm->c_coll.coll_allgatherv_module);
    
 exit:
    if ( NULL != tmpbuf ) {
        free ( tmpbuf );
    }
    if ( NULL != rcounts ) {
        free ( rcounts );
    }
    if ( NULL != rdisps ) {
        free ( rdisps );
    }
    
    return (rc);
}

/* Arguments not used in this implementation:
 * - send_first
 */
static int ompi_comm_allreduce_intra_bridge (int *inbuf, int *outbuf, 
                                             int count, struct ompi_op_t *op, 
                                             ompi_communicator_t *comm,
                                             ompi_communicator_t *bcomm, 
                                             void* lleader, void* rleader,
                                             int send_first )
{
    int *tmpbuf=NULL;
    int local_rank;
    int i;
    int rc;
    int local_leader, remote_leader;
    
    local_leader  = (*((int*)lleader));
    remote_leader = (*((int*)rleader));

    if ( &ompi_mpi_op_sum.op != op && &ompi_mpi_op_prod.op != op &&
         &ompi_mpi_op_max.op != op && &ompi_mpi_op_min.op  != op ) {
        return MPI_ERR_OP;
    }
    
    local_rank = ompi_comm_rank ( comm );
    tmpbuf     = (int *) malloc ( count * sizeof(int));
    if ( NULL == tmpbuf ) {
        rc = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    /* Intercomm_create */
    rc = comm->c_coll.coll_allreduce ( inbuf, tmpbuf, count, MPI_INT,
                                       op, comm, comm->c_coll.coll_allreduce_module );
    if ( OMPI_SUCCESS != rc ) {
        goto exit;
    }

    if (local_rank == local_leader ) {
        MPI_Request req;
        
        rc = MCA_PML_CALL(irecv ( outbuf, count, MPI_INT, remote_leader,
                                 OMPI_COMM_ALLREDUCE_TAG, 
                                 bcomm, &req));
        if ( OMPI_SUCCESS != rc ) {
            goto exit;       
        }
        rc = MCA_PML_CALL(send (tmpbuf, count, MPI_INT, remote_leader, 
                               OMPI_COMM_ALLREDUCE_TAG,
                               MCA_PML_BASE_SEND_STANDARD,  bcomm));
        if ( OMPI_SUCCESS != rc ) {
            goto exit;
        }
        rc = ompi_request_wait_all ( 1, &req, MPI_STATUS_IGNORE);
        if ( OMPI_SUCCESS != rc ) {
            goto exit;
        }

        if ( &ompi_mpi_op_max.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                if (tmpbuf[i] > outbuf[i]) outbuf[i] = tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_min.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                if (tmpbuf[i] < outbuf[i]) outbuf[i] = tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_sum.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                outbuf[i] += tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_prod.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                outbuf[i] *= tmpbuf[i];
            }
        }
        
    }
    
    rc = comm->c_coll.coll_bcast ( outbuf, count, MPI_INT, local_leader, 
                                   comm, comm->c_coll.coll_bcast_module );

 exit:
    if (NULL != tmpbuf ) {
        free (tmpbuf);
    }

    return (rc);
}

/* Arguments not used in this implementation:
 *    - bridgecomm
 *
 * lleader is the local rank of root in comm
 * rleader is the OOB contact information of the
 * root processes in the other world.
 */
static int ompi_comm_allreduce_intra_oob (int *inbuf, int *outbuf, 
                                          int count, struct ompi_op_t *op, 
                                          ompi_communicator_t *comm,
                                          ompi_communicator_t *bridgecomm, 
                                          void* lleader, void* rleader,
                                          int send_first )
{
    int *tmpbuf=NULL;
    int i;
    int rc;
    int local_leader, local_rank;
    orte_process_name_t *remote_leader=NULL;
    orte_std_cntr_t size_count;
    
    local_leader  = (*((int*)lleader));
    remote_leader = (orte_process_name_t*)rleader;
    size_count = count;

    if ( &ompi_mpi_op_sum.op != op && &ompi_mpi_op_prod.op != op &&
         &ompi_mpi_op_max.op != op && &ompi_mpi_op_min.op  != op ) {
        return MPI_ERR_OP;
    }
    
    
    local_rank = ompi_comm_rank ( comm );
    tmpbuf     = (int *) malloc ( count * sizeof(int));
    if ( NULL == tmpbuf ) {
        rc = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    /* comm is an intra-communicator */
    rc = comm->c_coll.coll_allreduce(inbuf,tmpbuf,count,MPI_INT,op, comm,
                                     comm->c_coll.coll_allreduce_module);
    if ( OMPI_SUCCESS != rc ) {
        goto exit;
    }
    
    if (local_rank == local_leader ) {
        opal_buffer_t *sbuf;
        opal_buffer_t *rbuf;

        sbuf = OBJ_NEW(opal_buffer_t);
        rbuf = OBJ_NEW(opal_buffer_t);
        
        if (ORTE_SUCCESS != (rc = opal_dss.pack(sbuf, tmpbuf, (orte_std_cntr_t)count, OPAL_INT))) {
            goto exit;
        }

        if ( send_first ) {
            if (0 > (rc = orte_rml.send_buffer(remote_leader, sbuf, OMPI_RML_TAG_COMM_CID_INTRA, 0))) {
                goto exit;
            }
            if (0 > (rc = orte_rml.recv_buffer(remote_leader, rbuf, OMPI_RML_TAG_COMM_CID_INTRA, 0))) {
                goto exit;
            }
        }
        else {
            if (0 > (rc = orte_rml.recv_buffer(remote_leader, rbuf, OMPI_RML_TAG_COMM_CID_INTRA, 0))) {
                goto exit;
            }
            if (0 > (rc = orte_rml.send_buffer(remote_leader, sbuf, OMPI_RML_TAG_COMM_CID_INTRA, 0))) {
                goto exit;
            }
        }

        if (ORTE_SUCCESS != (rc = opal_dss.unpack(rbuf, outbuf, &size_count, OPAL_INT))) {
            goto exit;
        }
        OBJ_RELEASE(sbuf);
        OBJ_RELEASE(rbuf);
        count = (int)size_count;

        if ( &ompi_mpi_op_max.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                if (tmpbuf[i] > outbuf[i]) outbuf[i] = tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_min.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                if (tmpbuf[i] < outbuf[i]) outbuf[i] = tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_sum.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                outbuf[i] += tmpbuf[i];
            }
        }
        else if ( &ompi_mpi_op_prod.op == op ) {
            for ( i = 0 ; i < count; i++ ) {
                outbuf[i] *= tmpbuf[i];
            }
        }
        
    }
    
    rc = comm->c_coll.coll_bcast (outbuf, count, MPI_INT, 
                                  local_leader, comm,
                                  comm->c_coll.coll_bcast_module);

 exit:
    if (NULL != tmpbuf ) {
        free (tmpbuf);
    }

    return (rc);
}

END_C_DECLS

