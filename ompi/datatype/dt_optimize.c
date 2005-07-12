/* -*- Mode: C; c-basic-offset:4 ; -*- */
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

#include "ompi_config.h"

#include "datatype/datatype.h"
#include "datatype/convertor.h"
#include "datatype/datatype_internal.h"
#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif
#include <stdlib.h>

static int32_t
ompi_ddt_optimize_short( ompi_datatype_t* pData,
                         int32_t count, 
                         dt_type_desc_t* pTypeDesc )
{
    dt_elem_desc_t* pElemDesc;
    long last_disp = 0;
    dt_stack_t* pStack;            /* pointer to the position on the stack */
    int32_t pos_desc = 0;          /* actual position in the description of the derived datatype */
    int32_t stack_pos = 0, last_type = DT_BYTE;
    int32_t type = DT_BYTE, last_length = 0, nbElems = 0, changes = 0, last_extent = 1;
    uint16_t last_flags = 0xFFFF;  /* keep all for the first datatype */
    long total_disp = 0;
    int32_t optimized = 0;

    pStack = alloca( sizeof(dt_stack_t) * (pData->btypes[DT_LOOP]+2) );
    SAVE_STACK( pStack, -1, 0, count, 0, pData->desc.used );

    pTypeDesc->length = 2 * pData->desc.used + 1 /* for the fake DT_END_LOOP at the end */;
    pTypeDesc->desc = pElemDesc = (dt_elem_desc_t*)malloc( sizeof(dt_elem_desc_t) * pTypeDesc->length );
    pTypeDesc->used = 0;

    while( stack_pos >= 0 ) {
        if( DT_END_LOOP == pData->desc.desc[pos_desc].elem.common.type ) { /* end of the current loop */
            ddt_endloop_desc_t* end_loop = &(pData->desc.desc[pos_desc].end_loop);
            if( last_length != 0 ) {
                CREATE_ELEM( pElemDesc, last_type, DT_FLAG_BASIC, last_length, last_disp, last_extent );
                pElemDesc++; nbElems++;
                last_disp += last_length;
                last_length = 0;
            }
            CREATE_LOOP_END( pElemDesc, nbElems - pStack->index + 1,  /* # of elems in this loop */
                             end_loop->total_extent, end_loop->size, end_loop->common.flags );
            pElemDesc++; nbElems++;
            if( --stack_pos >= 0 ) {  /* still something to do ? */
                ddt_loop_desc_t* pStartLoop = &(pTypeDesc->desc[pStack->index - 1].loop);
                pStartLoop->items = (pElemDesc - 1)->elem.count;
                total_disp = pStack->disp;  /* update the displacement position */
            }
            pStack--;  /* go down one position on the stack */
            pos_desc++;
            continue;
        }
        if( DT_LOOP == pData->desc.desc[pos_desc].elem.common.type ) {
            ddt_loop_desc_t* loop = (ddt_loop_desc_t*)&(pData->desc.desc[pos_desc]);
            ddt_endloop_desc_t* end_loop = (ddt_endloop_desc_t*)&(pData->desc.desc[pos_desc + loop->items]);
            int index = GET_FIRST_NON_LOOP( &(pData->desc.desc[pos_desc]) );
            long loop_disp = pData->desc.desc[pos_desc + index].elem.disp;

            if( loop->common.flags & DT_FLAG_CONTIGUOUS ) {
                /* the loop is contiguous or composed by contiguous elements with a gap */
                if( loop->extent == (long)end_loop->size ) {
                    /* the whole loop is contiguous */
                    if( (last_disp + last_length) != (total_disp + loop_disp) ) {
                        CREATE_ELEM( pElemDesc, last_type, DT_FLAG_BASIC, last_length, last_disp, last_extent );
                        pElemDesc++; nbElems++;
                        last_length = 0;
                        last_disp = total_disp + loop_disp;
                    }
                    last_length += loop->loops * end_loop->size;
                    optimized++;
                } else {
                    int counter = loop->loops;
                    /* if the previous data is contiguous with this piece and it has a length not ZERO */
                    if( last_length != 0 ) {
                        if( (last_disp + last_length) == (total_disp + loop_disp) ) {
                            last_length *= ompi_ddt_basicDatatypes[last_type]->size;
                            last_length += end_loop->size;
                            last_type = DT_BYTE;
                            counter--;
                        }
                        CREATE_ELEM( pElemDesc, last_type, DT_FLAG_BASIC, last_length, last_disp, last_extent );
                        pElemDesc++; nbElems++;
                        last_disp += last_length;
                        last_length = 0;
                    }                  
                    /* we have a gap in the begining or the end of the loop but the whole
                     * loop can be merged in just one memcpy.
                     */
                    CREATE_LOOP_START( pElemDesc, counter, (long)2, loop->extent, loop->common.flags );
                    pElemDesc++; nbElems++;
                    CREATE_ELEM( pElemDesc, last_type, DT_FLAG_BASIC, end_loop->size, loop_disp, last_extent );
                    pElemDesc++; nbElems++;
                    CREATE_LOOP_END( pElemDesc, 2, end_loop->total_extent, end_loop->size,
                                     end_loop->common.flags );
                    pElemDesc++; nbElems++;
                    if( loop->items > 2 ) optimized++;
                }
                pos_desc += pData->desc.desc[pos_desc].loop.items + 1;
                changes++;
            } else {
                if( last_length != 0 ) {
                    CREATE_ELEM( pElemDesc, last_type, DT_FLAG_BASIC, last_length, last_disp, last_extent );
                    pElemDesc++; nbElems++;
                    last_disp += last_length;
                    last_length = 0;
                }
                CREATE_LOOP_START( pElemDesc, loop->loops, loop->items, loop->extent, loop->common.flags );
                pElemDesc++; nbElems++;
                PUSH_STACK( pStack, stack_pos, nbElems, DT_LOOP, loop->loops, total_disp, pos_desc + loop->extent );
                pos_desc++;
                DDT_DUMP_STACK( pStack, stack_pos, pData->desc.desc, "advance loops" );
            }
            total_disp = pStack->disp;  /* update the displacement */
            continue;
        }
        while( pData->desc.desc[pos_desc].elem.common.flags & DT_FLAG_DATA ) {  /* keep doing it until we reach a non datatype element */
            /* now here we have a basic datatype */
            type = pData->desc.desc[pos_desc].elem.common.type;
            
            if( (pData->desc.desc[pos_desc].elem.common.flags & DT_FLAG_CONTIGUOUS) && 
                (last_disp + last_length) == (total_disp + pData->desc.desc[pos_desc].elem.disp) &&
                (pData->desc.desc[pos_desc].elem.extent == (int32_t)ompi_ddt_basicDatatypes[type]->size) ) {
                if( type == last_type ) {
                    last_length += pData->desc.desc[pos_desc].elem.count;
                    last_extent = pData->desc.desc[pos_desc].elem.extent;
                } else {
                    if( last_length == 0 ) {
                        last_type = type;
                        last_length = pData->desc.desc[pos_desc].elem.count;
                        last_extent = pData->desc.desc[pos_desc].elem.extent;
                    } else {
                        last_length = last_length * ompi_ddt_basicDatatypes[last_type]->size + 
                            pData->desc.desc[pos_desc].elem.count * ompi_ddt_basicDatatypes[type]->size;
                        last_type = DT_BYTE;
                        last_extent = 1;
                        optimized++;
                    }
                }
                last_flags &= pData->desc.desc[pos_desc].elem.common.flags;
            } else {
                if( last_length != 0 ) {
                    CREATE_ELEM( pElemDesc, last_type, DT_FLAG_BASIC, last_length, last_disp, last_extent );
                    pElemDesc++; nbElems++;
                }
                last_disp = total_disp + pData->desc.desc[pos_desc].elem.disp;
                last_length = pData->desc.desc[pos_desc].elem.count;
                last_extent = pData->desc.desc[pos_desc].elem.extent;
                last_type = type;
            }
            pos_desc++;  /* advance to the next data */
        }
    }
    
    if( last_length != 0 ) {
        CREATE_ELEM( pElemDesc, DT_BYTE, DT_FLAG_BASIC, last_length, last_disp, last_extent );
        pElemDesc++; nbElems++;
    }
    /* cleanup the stack */
    pTypeDesc->used = nbElems - 1;  /* except the last fake END_LOOP */
    return OMPI_SUCCESS;
}

#define PRINT_MEMCPY( DST, SRC, LENGTH ) \
{ \
  printf( "%5d: memcpy dst = %p src %p length %ld bytes (so far %d)[%d]\n", \
          __index++, (DST), (SRC), (long)(LENGTH), __sofar, __LINE__ ); \
  __sofar += (LENGTH); \
}

#if defined(COMPILE_USELSS_CODE)
static int ompi_ddt_unroll( ompi_datatype_t* pData, int count )
{
    dt_stack_t* pStack;   /* pointer to the position on the stack */
    int pos_desc;         /* actual position in the description of the derived datatype */
    int type;             /* type at current position */
    int i;                /* index for basic elements with extent */
    int stack_pos = 0;    /* position on the stack */
    long last_disp = 0, last_length = 0;
    char* pDestBuf;
    int bConverted = 0, __index = 0, __sofar = 0;
    dt_elem_desc_t* pElems;

    pDestBuf = NULL;

    if( pData->flags & DT_FLAG_CONTIGUOUS ) {
        long extent = pData->ub - pData->lb;
        char* pSrc = (char*)pData->true_lb;

        type = count * pData->size;
        if( pData->size == extent /* true extent at this point */ ) {
            /* we can do it with just one memcpy */
            PRINT_MEMCPY( pDestBuf, pSrc, pData->size * count );
            bConverted += (pData->size * count);
        } else {
            char* pSrcBuf = (char*)pData->true_lb;
            long extent = pData->ub - pData->lb;
            for( pos_desc = 0; pos_desc < count; pos_desc++ ) {
                PRINT_MEMCPY( pDestBuf, pSrcBuf, pData->size );
                pSrcBuf += extent;
                pDestBuf += pData->size;
            }
            bConverted += type;
        }
        return (bConverted == (pData->size * count));
    }
    pStack = alloca( sizeof(dt_stack_t) * pData->btypes[DT_LOOP] );
    pStack->count = count;
    pStack->index = -1;
    pStack->disp = 0;
    pos_desc  = 0;

    if( pData->opt_desc.desc != NULL ) {
        pElems = pData->opt_desc.desc;
        pStack->end_loop = pData->opt_desc.used;
    } else {
        pElems = pData->desc.desc;
        pStack->end_loop = pData->desc.used;
    }

    DDT_DUMP_STACK( pStack, stack_pos, pElems, "starting" );
    DUMP( "remember position on stack %d last_elem at %d\n", stack_pos, pos_desc );
    DUMP( "top stack info {index = %d, count = %d}\n", pStack->index, pStack->count );

    while( pos_desc >= 0 ) {
        if( DT_END_LOOP == pElems[pos_desc].type ) { /* end of the current loop */
            if( --(pStack->count) == 0 ) { /* end of loop */
                pStack--;
                if( --stack_pos == -1 ) break;
            } else {
                pos_desc = pStack->index;
                if( pos_desc == -1 ) {
                    pStack->disp += (pData->ub - pData->lb);
                } else {
                    assert( DT_LOOP == pElems[pos_desc].elem.common.type );
                    pStack->disp += pElems[pos_desc].loop.extent;
                }
            }
            pos_desc++;
            continue;
        }
        if( DT_LOOP == pElems[pos_desc].type ) {
            if( pElems[pos_desc].flags & DT_FLAG_CONTIGUOUS ) {
                dt_elem_desc_t* pLast = &( pElems[pos_desc + pElems[pos_desc].disp]);
                if( (last_disp + last_length) == (pStack->disp + pElems[pos_desc+1].disp) ) {
                    PRINT_MEMCPY( pDestBuf, (char*)last_disp, last_length + pLast->extent );
                    last_disp = pStack->disp + pElems[pos_desc+1].disp + pLast->extent;
                    i = 1;
                } else {
                    PRINT_MEMCPY( pDestBuf, (char*)last_disp, last_length );
                    last_disp = pStack->disp + pElems[pos_desc + 1].disp;
                    i = 0;
                }
                last_length = pLast->extent;
                for( ; i < (pElems[pos_desc].count - 1); i++ ) {
                    PRINT_MEMCPY( pDestBuf, (char*)last_disp, last_length );
                    pDestBuf += pLast->extent;
                    last_disp += pElems[pos_desc].extent;
                }
                pos_desc += pElems[pos_desc].disp + 1;
                goto next_loop;
            } else {
                do {
                    PUSH_STACK( pStack, stack_pos, pos_desc, DT_LOOP, pElems[pos_desc].loop.loops,
                                pStack->disp, pos_desc + pElems[pos_desc].loop.items );
                    pos_desc++;
                } while( pElems[pos_desc].type == DT_LOOP ); /* let's start another loop */
            }
        }
        /* now here we have a basic datatype */
        type = pElems[pos_desc].type;
        if( (last_disp + last_length) == (pStack->disp + pElems[pos_desc].disp) ) {
            last_length += pElems[pos_desc].count * ompi_ddt_basicDatatypes[type]->size;
        } else {
            PRINT_MEMCPY( pDestBuf, (char*)last_disp, last_length );
            pDestBuf += last_length;
            bConverted += last_length;
            last_disp = pStack->disp + pElems[pos_desc].disp;
            last_length = pElems[pos_desc].count * ompi_ddt_basicDatatypes[type]->size;
        }
        pos_desc++;  /* advance to the next data */
    }
    PRINT_MEMCPY( pDestBuf, (char*)last_disp, last_length );
    return OMPI_SUCCESS;
}
#endif  /* COMPILE_USELSS_CODE */

int32_t ompi_ddt_commit( ompi_datatype_t** data )
{
    ompi_datatype_t* pData = *data;
    ddt_endloop_desc_t* pLast = &(pData->desc.desc[pData->desc.used].end_loop);

    if( pData->flags & DT_FLAG_COMMITED ) return OMPI_SUCCESS;
    pData->flags |= DT_FLAG_COMMITED;

    /* let's add a fake element at the end just to avoid useless comparaisons
     * in pack/unpack functions.
     */
    pLast->common.type  = DT_END_LOOP;
    pLast->common.flags = 0;
    pLast->items        = pData->desc.used;
    pLast->total_extent = pData->ub - pData->lb;
    pLast->size         = pData->size;

    /* If there is no datatype description how can we have an optimized description ? */
    if( 0 == pData->desc.used ) {
        pData->opt_desc.length = 0;
        pData->opt_desc.desc   = NULL;
        pData->opt_desc.used   = 0;
        return OMPI_SUCCESS;
    }

    /* If the data is contiguous is useless to generate an optimized version. */
    /*if( (long)pData->size == (pData->true_ub - pData->true_lb) ) return OMPI_SUCCESS; */

    (void)ompi_ddt_optimize_short( pData, 1, &(pData->opt_desc) );
    if( 0 != pData->opt_desc.used ) {
        /* let's add a fake element at the end just to avoid useless comparaisons
         * in pack/unpack functions.
         */
        pLast = &(pData->opt_desc.desc[pData->opt_desc.used].end_loop);
        pLast->common.type  = DT_END_LOOP;
        pLast->common.flags = 0;
        pLast->items        = pData->opt_desc.used;
        pLast->total_extent = pData->ub - pData->lb;
        pLast->size         = pData->size;
    }
    return OMPI_SUCCESS;
}
