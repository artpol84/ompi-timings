/*
 * Copyrights?
 * 
 * $HEADER$
 */

/** @file:
 *
 */

#ifndef _ORTE_CLOCK_SYNC_H_
#define _ORTE_CLOCK_SYNC_H_

#include "orte_config.h"
#include "opal/util/output.h"
#include "opal/dss/dss_types.h"


BEGIN_C_DECLS

typedef enum { cs_no = 0, cs_sock_direct, cs_sock_tree, cs_rml_direct, cs_rml_tree, cs_max } orte_util_sync_strategy_t;

typedef int (*delivery_fn)(opal_buffer_t *relay);
ORTE_DECLSPEC int orte_util_clock_sync_orted_init(opal_buffer_t *relay, delivery_fn fn);
ORTE_DECLSPEC int orte_util_clock_sync_hnp_init(opal_buffer_t *relay, delivery_fn fn);
END_C_DECLS

#endif
