/*
 * ? Copyrights ?
 */

#ifndef OPAL_SYS_LIMITS_H
#define OPAL_SYS_LIMITS_H

double orte_grpcomm_get_timestamp();
void orte_grpcomm_add_timestep(orte_grpcomm_collective_t *coll,
                                               char *step_name);
void orte_grpcomm_output_timings(orte_grpcomm_collective_t *coll);
void orte_grpcomm_clear_timings(orte_grpcomm_collective_t *coll);

#endif
