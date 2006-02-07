/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
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
/** @file:
 *
 * The Open MPI general purpose registry - unit test
 *
 */

/*
 * includes
 */

#include "orte_config.h"
#include <stdio.h>
#include <string.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#include "opal/runtime/opal.h"
#include "opal/util/malloc.h"
#include "opal/util/output.h"

#include "orte/include/orte_constants.h"
#include "orte/mca/schema/schema.h"
#include "orte/mca/schema/base/base.h"
#include "orte/mca/ns/base/base.h"
#include "orte/mca/soh/base/base.h"
#include "orte/mca/rmgr/base/base.h"

#include "orte/class/orte_pointer_array.h"
#include "orte/dss/dss.h"
#include "orte/runtime/runtime.h"
#include "orte/util/proc_info.h"
#include "orte/util/sys_info.h"

#include "orte/mca/gpr/base/base.h"
#include "orte/mca/gpr/replica/api_layer/gpr_replica_api.h"
#include "orte/mca/gpr/replica/functional_layer/gpr_replica_fn.h"
#include "orte/mca/gpr/replica/communications/gpr_replica_comm.h"
#include "orte/mca/gpr/replica/transition_layer/gpr_replica_tl.h"

static void notify_callback(orte_gpr_notify_data_t *data, void *cbdata)
{
	size_t i, j, k;
	orte_gpr_value_t **values = (orte_gpr_value_t**)(data->values)->addr;
	char *tmp;

    fprintf(stderr, "Callback received - with %lu values\n", (unsigned long)data->cnt);
	for (i = 0, k=0; k < data->cnt &&
                     i < (data->values)->size; i++) {
        if (NULL != values[i]) {
            orte_gpr_value_t *value = values[i];
            k++;
            fprintf(stderr, "\tData for value %lu - with %lu keyvals\n",
                (unsigned long)i, (unsigned long)value->cnt);
            for (j = 0; j < value->cnt; j++) {
                orte_gpr_keyval_t *keyval = value->keyvals[j];
                fprintf(stderr, "\t\tKey number: %lu\tkey = %s\n", (unsigned long)j, keyval->key);
			
                orte_dss.print(&tmp, NULL, keyval->value, ORTE_DATA_VALUE);
                fprintf(stderr, "\t\t%s\n", tmp);
                free(tmp);   
			}
		}
	}
}

int main(int argc, char **argv)
{
    int i, rc;
    orte_gpr_subscription_t sub, *subs;
    orte_gpr_value_t value, *values;
    char *keys[] = {
        ORTE_NODE_STATE_KEY,
        "stupid-value-one",
        "stupid-value-two",
        "stupid-value-three",
        "stupid-value-four"};
    int32_t i32;
    int16_t i16;

    opal_init();

    /* register handler for errnum -> string converstion */
    opal_error_register("ORTE", ORTE_ERR_BASE, ORTE_ERR_MAX, orte_err2str);


    /* Ensure the process info structure is instantiated and initialized */
    if (ORTE_SUCCESS != (rc = orte_proc_info())) {
        return rc;
    }

    orte_process_info.seed = true;
    orte_process_info.my_name = (orte_process_name_t*)malloc(sizeof(orte_process_name_t));
    orte_process_info.my_name->cellid = 0;
    orte_process_info.my_name->jobid = 0;
    orte_process_info.my_name->vpid = 0;

    /* startup the MCA */
    if (OMPI_SUCCESS == mca_base_open()) {
        fprintf(stderr, "MCA started\n");
    } else {
        fprintf(stderr, "MCA could not start\n");
        exit (1);
    }

    /* open the dss */
    if (ORTE_SUCCESS == orte_dss_open()) {
        fprintf(stderr, "DSS started\n");
    } else {
        fprintf(stderr, "DSS could not start\n");
        exit (1);
    }

    /* ENSURE THE REPLICA IS ISOLATED */
    setenv("OMPI_MCA_gpr_replica_isolate", "1", 1);

    /* startup the gpr to register data types */
    if (ORTE_SUCCESS == orte_gpr_base_open()) {
        fprintf(stderr, "GPR opened\n");
    } else {
        fprintf(stderr, "GPR could not open\n");
        exit (1);
    }

    /* do a select on the registry components */
    if (OMPI_SUCCESS == orte_gpr_base_select()) {
        fprintf(stderr, "GPR selected\n");
    } else {
        fprintf(stderr, "GPR could not select\n");
        exit (1);
    }
 
	OBJ_CONSTRUCT(&sub, orte_gpr_subscription_t);
	sub.action = ORTE_GPR_NOTIFY_VALUE_CHG;

	OBJ_CONSTRUCT(&value, orte_gpr_value_t);
	values = &value;
	sub.values = &values;
	sub.cnt = 1; /* number of values */
	value.addr_mode = ORTE_GPR_TOKENS_XAND | ORTE_GPR_KEYS_OR;
	value.segment = strdup(ORTE_NODE_SEGMENT);

	value.cnt = 5; /* number of keyvals */
	value.keyvals = (orte_gpr_keyval_t**)malloc(value.cnt * sizeof(orte_gpr_keyval_t*));

	for (i=0; i < 5; i++) {
	   value.keyvals[i] = OBJ_NEW(orte_gpr_keyval_t);
	   value.keyvals[i]->key = strdup(keys[i]);
    }

	/* Any token */
	value.tokens = NULL;
	value.num_tokens = 0;

	sub.cbfunc = notify_callback;
	sub.user_tag = NULL;

	subs = &sub;
	rc = orte_gpr.subscribe(1, &subs, 0, NULL);

	if (ORTE_SUCCESS != rc) {
         return 1;
    }

    OBJ_DESTRUCT(&value);
    
    /* now let's write something into those locations */
	OBJ_CONSTRUCT(&value, orte_gpr_value_t);
    value.addr_mode = ORTE_GPR_NO_OVERWRITE |
                      ORTE_GPR_TOKENS_XAND |
                      ORTE_GPR_KEYS_OR;
    value.segment = strdup(ORTE_NODE_SEGMENT);
    value.num_tokens = 2;
    value.tokens = (char**)malloc(value.num_tokens * sizeof(char*));
    for (i=0; i < 2; i++) {
        asprintf(&(value.tokens[i]), "dummy-token-%d", i);
    }
    value.cnt = 5;
    value.keyvals = (orte_gpr_keyval_t**)malloc(5*sizeof(orte_gpr_keyval_t*));
    for (i=0; i < 5; i++) {
        value.keyvals[i] = OBJ_NEW(orte_gpr_keyval_t);
        (value.keyvals[i])->key = strdup(keys[i]);
        value.keyvals[i]->value = OBJ_NEW(orte_data_value_t);
    }
    (value.keyvals[0])->value->type = ORTE_INT32;
    i32 = 654321;
    orte_dss.copy(&(value.keyvals[0]->value->data), &i32, ORTE_INT32);
    (value.keyvals[1])->value->type = ORTE_INT16;
    i16 = 128;
     orte_dss.copy(&(value.keyvals[1]->value->data), &i16, ORTE_INT16);
    for (i=2; i < 5; i++) {
        (value.keyvals[i])->value->type = ORTE_INT32;
        i32 = i * 10;
        orte_dss.copy(&(value.keyvals[i]->value->data), &i32, ORTE_INT32);
    }
    values = &value;
    
    if (ORTE_SUCCESS != (rc = orte_gpr.put(1, &values))) {
        OBJ_DESTRUCT(&value);
        return rc;
    }
    OBJ_DESTRUCT(&value); /* clean up */

    /* now let's update a few of them */
    OBJ_CONSTRUCT(&value, orte_gpr_value_t);
    value.addr_mode = ORTE_GPR_OVERWRITE |
                      ORTE_GPR_TOKENS_XAND |
                      ORTE_GPR_KEYS_OR;
    value.segment = strdup(ORTE_NODE_SEGMENT);
    value.num_tokens = 2;
    value.tokens = (char**)malloc(value.num_tokens * sizeof(char*));
    for (i=0; i < 2; i++) {
        asprintf(&(value.tokens[i]), "dummy-token-%d", i);
    }
    value.cnt = 3;
    value.keyvals = (orte_gpr_keyval_t**)malloc(3*sizeof(orte_gpr_keyval_t*));
    for (i=0; i < 3; i++) {
        value.keyvals[i] = OBJ_NEW(orte_gpr_keyval_t);
        (value.keyvals[i])->key = strdup(keys[i]);
        value.keyvals[i]->value = OBJ_NEW(orte_data_value_t);
    }
    (value.keyvals[0])->value->type = ORTE_INT32;
    i32 = 123456;
    orte_dss.copy(&(value.keyvals[0]->value->data), &i32, ORTE_INT32);
    (value.keyvals[1])->value->type = ORTE_INT16;
    i16 = 904;
     orte_dss.copy(&(value.keyvals[1]->value->data), &i16, ORTE_INT16);
    (value.keyvals[2])->value->type = ORTE_STRING;
    (value.keyvals[2])->value->data = strdup("idiot-string");
    values = &value;
    
    if (ORTE_SUCCESS != (rc = orte_gpr.put(1, &values))) {
        fprintf(stderr, "Error in put %d\n", rc);
        OBJ_DESTRUCT(&value);
        return rc;
    }
    OBJ_DESTRUCT(&value); /* clean up */

    orte_gpr_base_close();

    orte_dss_close();
    mca_base_close();
    opal_malloc_finalize();
    opal_output_finalize();
    opal_class_finalize();

    return(0);
}
