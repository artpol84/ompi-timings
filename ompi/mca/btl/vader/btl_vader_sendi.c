/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2011 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2009 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2007 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2007 Voltaire. All rights reserved.
 * Copyright (c) 2009      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2011 Los Alamos National Security, LLC.  
 *                         All rights reserved. 
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "btl_vader.h"
#include "btl_vader_frag.h"
#include "btl_vader_fifo.h"

/**
 * Initiate an inline send to the peer.
 *
 * @param btl (IN)      BTL module
 * @param peer (IN)     BTL peer addressing
 */
int mca_btl_vader_sendi (struct mca_btl_base_module_t *btl,
			 struct mca_btl_base_endpoint_t *endpoint,
			 struct opal_convertor_t *convertor,
			 void *header, size_t header_size,
			 size_t payload_size, uint8_t order,
			 uint32_t flags, mca_btl_base_tag_t tag,
			 mca_btl_base_descriptor_t **descriptor)
{
    size_t length = (header_size + payload_size);
    mca_btl_vader_frag_t *frag;
    uint32_t iov_count = 1;
    struct iovec iov;
    size_t max_data;
    int rc;

    assert (length < mca_btl_vader_component.eager_limit);
    assert (0 == (flags & MCA_BTL_DES_SEND_ALWAYS_CALLBACK));

    /* allocate a fragment, giving up if we can't get one */
    /* note that frag==NULL is equivalent to rc returning an error code */
    MCA_BTL_VADER_FRAG_ALLOC_EAGER(frag, rc);
    if (OPAL_UNLIKELY(NULL == frag)) {
	*descriptor = NULL;
	return rc;
    }

    /* fill in fragment fields */
    frag->segment.seg_len = length;
    frag->hdr->len        = length;

    /* why do any flags matter here other than OWNERSHIP? */
    frag->base.des_flags = flags | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP;
    frag->hdr->tag = tag;
    frag->endpoint = endpoint;

    /* write the match header (with MPI comm/tag/etc. info) */
    memmove (frag->segment.seg_addr.pval, header, header_size);

    /* write the message data if there is any */
    /*
      We can add MEMCHECKER calls before and after the packing.
    */
    if (payload_size) {
	/* pack the data into the supplied buffer */
	iov.iov_base = (IOVBASE_TYPE *)((uintptr_t)frag->segment.seg_addr.pval + header_size);
	iov.iov_len  = max_data = payload_size;

	(void) opal_convertor_pack (convertor, &iov, &iov_count, &max_data);

	assert (max_data == payload_size);
    }

    opal_list_append (&mca_btl_vader_component.active_sends, (opal_list_item_t *) frag);

    /* write the fragment pointer to peer's the FIFO */
    vader_fifo_write ((void *) VIRTUAL2RELATIVE(frag->hdr),
		      mca_btl_vader_component.fifo[endpoint->peer_smp_rank]);

    return OMPI_SUCCESS;
}