#include "pml_teg_recvreq.h"

                                                                                                               
void mca_pml_teg_recv_request_progress(
    mca_ptl_base_recv_request_t* req,
    mca_ptl_base_recv_frag_t* frag)
{
    lam_mutex_lock(&mca_pml_teg.teg_lock);
    req->req_bytes_recvd += frag->super.frag_size;
    if (req->req_bytes_recvd >= req->super.req_status.MPI_LENGTH) {
        req->super.req_mpi_done = true;
        if(mca_pml_teg.teg_req_waiting) {
            lam_condition_broadcast(&mca_pml_teg.teg_condition);
        }
    }
    lam_mutex_unlock(&mca_pml_teg.teg_lock);
}

