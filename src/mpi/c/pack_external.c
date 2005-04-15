/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>

#include "mpi.h"
#include "mpi/c/bindings.h"
#include "datatype/datatype.h"
#include "errhandler/errhandler.h"
#include "communicator/communicator.h"
#include "class/ompi_object.h"

#if OMPI_HAVE_WEAK_SYMBOLS && OMPI_PROFILING_DEFINES
#pragma weak MPI_Pack_external = PMPI_Pack_external
#endif

#if OMPI_PROFILING_DEFINES
#include "mpi/c/profile/defines.h"
#endif

static const char FUNC_NAME[] = "MPI_Pack_external";


int MPI_Pack_external(char *datarep, void *inbuf, int incount,
                      MPI_Datatype datatype, void *outbuf,
                      MPI_Aint outsize, MPI_Aint *position) 
{
    int rc, freeAfter;
    ompi_convertor_t *local_convertor;
    struct iovec invec;
    unsigned int size, iov_count;

    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if ((NULL == outbuf) || (NULL == position)) {  /* inbuf can be MPI_BOTTOM */
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        } else if (incount < 0) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COUNT, FUNC_NAME);
        } else if (outsize < 0) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_ARG, FUNC_NAME);
        } else if (MPI_DATATYPE_NULL == datatype) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_TYPE, FUNC_NAME);
        }
    }

    local_convertor = ompi_convertor_get_copy( ompi_mpi_external32_convertor );
    ompi_convertor_init_for_send(local_convertor, 0, datatype, incount,
                                 inbuf, 0, NULL /*never allocate memory*/);

    /* Check for truncation */
    ompi_convertor_get_packed_size( local_convertor, &size );
    if( (*position + size) > (unsigned int)outsize ) {  /* we can cast as we already checked for < 0 */
        OBJ_RELEASE( local_convertor );
        return OMPI_ERRHANDLER_INVOKE( MPI_COMM_WORLD, MPI_ERR_TRUNCATE, FUNC_NAME );
    }

    /* Prepare the iovec with all informations */
    invec.iov_base = (char*) outbuf + (*position);
    invec.iov_len = outsize - (*position);

    /* Do the actual packing */
    iov_count = 1;
    rc = ompi_convertor_pack( local_convertor, &invec, &iov_count, &size, &freeAfter );
    *position += local_convertor->bConverted;
    OBJ_RELEASE( local_convertor );

    /* All done.  Note that the convertor returns 1 upon success, not
       OMPI_SUCCESS. */
    OMPI_ERRHANDLER_RETURN((rc == 1) ? OMPI_SUCCESS : OMPI_ERROR,
                           MPI_COMM_WORLD, MPI_ERR_UNKNOWN, FUNC_NAME);
}
