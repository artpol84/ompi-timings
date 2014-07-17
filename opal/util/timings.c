
double orte_grpcomm_get_timestamp(){
    struct timeval tv;
    gettimeofday(&tv,NULL);
    double ret = tv.tv_sec + tv.tv_usec*1E-6;
    return ret;
}

void orte_grpcomm_add_timestep(orte_grpcomm_collective_t *coll,
                                               char *step_name)
{
    orte_grpcomm_colltimings_t *elem;
    elem = (void*)malloc(sizeof(*elem));
    if( elem == NULL ){
        // Do some error handling
    }
    elem->step_name = strdup(step_name);
    // Remove trailing '\n'-s
    elem->timestep = orte_grpcomm_get_timestamp();
    opal_list_append (&(coll->timings), (opal_list_item_t *)elem);
    opal_output(0,"%s orte_grpcomm_add_timestep: 0x%p %s\n",
                ORTE_NAME_PRINT((&orte_process_info.my_name)), coll, step_name);
}

void orte_grpcomm_output_timings(orte_grpcomm_collective_t *coll)
{
    orte_grpcomm_colltimings_t *el, *prev, *first;
    int count = 0;
    int size = opal_list_get_size(&(coll->timings));
    char *buf = malloc(size*(60+512));
    buf[0] = '\0';
    OPAL_LIST_FOREACH(el, &(coll->timings), orte_grpcomm_colltimings_t){
        count++;
        if( count > 1){
            sprintf(buf,"%s\n%s GRPCOMM Timings %lfs[%lfs]: %s",buf,
                    ORTE_NAME_PRINT((&orte_process_info.my_name)),
                    el->timestep - first->timestep, el->timestep - prev->timestep,
                    el->step_name);
            prev = el;
        }else{
            first = el;
            prev = el;
        }
    }
    sprintf(buf,"%s\n",buf);
    opal_output(0,"%s",buf);
    free(buf);
}

void orte_grpcomm_clear_timings(orte_grpcomm_collective_t *coll)
{
    orte_grpcomm_colltimings_t *el, *prev;
    int count = 0;
    OPAL_LIST_FOREACH(el, &(coll->timings), orte_grpcomm_colltimings_t){
        if( el->step_name )
            free(el->step_name);
    }
    opal_output(0,"%s orte_grpcomm_clear_timings: 0x%p\n",
                ORTE_NAME_PRINT((&orte_process_info.my_name)), coll);
}
