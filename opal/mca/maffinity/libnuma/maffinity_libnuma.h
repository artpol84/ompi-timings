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

/**
 * @file
 *
 * Processor affinity for libnuma.
 */


#ifndef MCA_MAFFINITY_LIBNUMA_EXPORT_H
#define MCA_MAFFINITY_LIBNUMA_EXPORT_H

#include "ompi_config.h"

#include "opal/mca/mca.h"
#include "opal/mca/maffinity/maffinity.h"


#if defined(c_plusplus) || defined(__cplusplus)
extern "C" {
#endif

    /**
     * Globally exported variable
     */
    OMPI_COMP_EXPORT extern const opal_maffinity_base_component_1_0_0_t
        mca_maffinity_libnuma_component;


    /**
     * maffinity query API function
     */
    const opal_maffinity_base_module_1_0_0_t *
        opal_maffinity_libnuma_component_query(int *query);

#if defined(c_plusplus) || defined(__cplusplus)
}
#endif
#endif /* MCA_MAFFINITY_LIBNUMA_EXPORT_H */
