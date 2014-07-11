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

BEGIN_C_DECLS

ORTE_DECLSPEC int orte_util_clock_sync_init();
ORTE_DECLSPEC int orte_util_clock_sync_hnp_part();
END_C_DECLS

#endif
