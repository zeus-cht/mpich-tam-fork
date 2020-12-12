#include "ad_bv.h"

#include <bv.h>
/* open and close are no-ops as benvolio operates on file names only
 */
void ADIOI_BV_Close(ADIO_File fd, int *error_code)
{
    int rank;
    bv_client_t client_info = fd->fs_ptr;
    MPI_Comm_rank(fd->comm, &rank);
    if (!rank&&getenv("BV_SHOW_STATS"))
        bv_statistics(client_info, !rank);
    if (!rank)
        bv_flush(client_info, fd->filename);
    MPI_Barrier(fd->comm);
    *error_code = MPI_SUCCESS;
}
