/*
 * $HEADERS$
 */

#include "lam_config.h"
#include "mpi.h"
#include "mpi/c/bindings.h"
#include "group/group.h"

#if LAM_HAVE_WEAK_SYMBOLS && LAM_PROFILING_DEFINES
#pragma weak MPI_Group_f2c = PMPI_Group_f2c
#endif


MPI_Group MPI_Group_f2c(MPI_Fint group_f)
{
    /* local variables */
    lam_group_t *group_c;
    int group_index;

    group_index = (int) group_f;

    /* error checks */
    if (MPI_PARAM_CHECK) {
        if (0 > group_index) {
            return MPI_GROUP_NULL;
        }
        if (group_index >= lam_pointer_array_get_size(lam_group_f_to_c_table)) {
            return MPI_GROUP_NULL;
        }
    }

    group_c = lam_group_f_to_c_table->addr[group_index];

    return (MPI_Group) group_c;
}
