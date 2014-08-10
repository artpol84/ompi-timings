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

#include "orte/runtime/orte_globals.h"
#include "opal/util/output.h"
#include "orte/util/clock_sync.h"

#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/rml/base/rml_contact.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/state/state_types.h"
#include "orte/mca/state/state.h"

BEGIN_C_DECLS

ORTE_DECLSPEC int orte_util_clock_sync_orted_init(void);
ORTE_DECLSPEC int orte_util_clock_sync_hnp_init(orte_job_t *jdata);
END_C_DECLS

#endif
