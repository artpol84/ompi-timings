/*
 * $HEADER$
 */

#include "lam/atomic.h"
#include "pml_teg_proc.h"
#include "pml_ptl_array.h"

lam_class_info_t mca_pml_teg_proc_cls = { 
    "mca_pml_teg_proc_t", 
    &lam_list_item_cls,
    (class_init_t) mca_pml_teg_proc_init, 
    (class_destroy_t) mca_pml_teg_proc_destroy 
};

static lam_list_t mca_pml_teg_procs;


void mca_pml_teg_proc_init(mca_pml_proc_t* proc)
{
    static int init = 0;
    if(fetchNset(&init,1) == 0) 
        STATIC_INIT(mca_pml_teg_procs, &lam_list_cls);
    SUPER_INIT(proc, &lam_list_item_cls);
    mca_ptl_array_init(&proc->proc_ptl_first);
    mca_ptl_array_init(&proc->proc_ptl_next);
    lam_list_append(&mca_pml_teg_procs, (lam_list_item_t*)proc);
}


void mca_pml_teg_proc_destroy(mca_pml_proc_t* proc)
{
    lam_list_remove_item(&mca_pml_teg_procs, (lam_list_item_t*)proc);
    SUPER_DESTROY(proc, &lam_list_item_cls);
}

