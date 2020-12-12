/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*-
 *   vim: ts=8 sts=4 sw=4 noexpandtab
 *
 * This is adapted from Avery Ching's "new List I/O" code -- one third the
 * number of lines as the "classic" list i/o
 */

#include <assert.h>
#include "adio.h"
#include "adio_extern.h"
#include "ad_bv.h"
#include "ad_bv_common.h"

#if 0
#define DEBUG_LIST 1
#define DEBUG_LIST2 1
#endif

#define COALESCE_REGIONS        /* TODO: would we ever want to *not* coalesce? */
#define MAX_OL_COUNT 1024

/* To do: Fix the code to coalesce the offset-length pairs for memory
 * and file. */

/* gen_listio_arr - fills in offset-length pairs for memory and file
 * for list I/O */
static int gen_listio_arr(ADIOI_Flatlist_node * flat_buf_p,     /* IN: memory description */
                          int *flat_buf_index_p,        /* IN: partial-progress cursor */
                          int64_t * cur_flat_buf_reg_off_p,     /* ??? */
                          int flat_buf_size,    /* IN: total bytes described in memory */
                          int flat_buf_extent,  /* IN: extent of memory description */
                          ADIOI_Flatlist_node * flat_file_p,    /* IN: file description */
                          int *flat_file_index_p,       /* IN: partial-progress cursor */
                          int64_t * cur_flat_file_reg_off_p,    /* ??? */
                          int flat_file_size,   /* IN: total bytes of file description */
                          int flat_file_extent, /* IN: extent of file description */
                          int max_ol_count,     /* IN: largest size offset-length arrays to make */
                          ADIO_Offset disp,     /* IN: displacement in file */
                          int bytes_into_filetype,      /* IN: partial progress: how far into file type to resume */
                          int64_t * bytes_completed,    /* OUT: bytes described in this collection of offset-length arrays */
                          int64_t total_io_size,        /* ??? */
                          void *buffer, /* IN: starting address for relative offsets */
                          char *buf_addr_arr[], /* OUT: array of memory offsets */
                          uint64_t buf_len_arr[],       /* OUT: array of memory lengths */
                          int64_t * buf_ol_count_p,     /* OUT: number of elements of memory offset/length arrays */
                          off_t file_off_arr[], uint64_t file_len_arr[], int64_t * file_ol_count_p)
{       /* OUT: same but for file arrays */
    int region_size = -1;

    /* parameters for flattened memory and file datatypes */
    int64_t cur_flat_buf_reg_left = 0;
    int64_t cur_flat_file_reg_left = 0;

#ifdef DEBUG_LIST2
    fprintf(stderr, "gen_list_arr:\n");
#endif

    if ((*buf_ol_count_p) != 0 || (*file_ol_count_p) != 0) {
        fprintf(stderr, "buf_ol_count != 0 || file_ol_count != 0\n");
        return -1;
    }

    /* Start on a non-zero memory and file region
     * Note this does not affect the bytes_completed
     * since no data is in these regions.  Initialize the
     * first memory and file offsets. */
    while (flat_buf_p->blocklens[(*flat_buf_index_p)] == 0) {
        (*flat_buf_index_p) = ((*flat_buf_index_p) + 1) % flat_buf_p->count;
    }
    buf_addr_arr[*buf_ol_count_p] = (char *) buffer +
        ((*bytes_completed / flat_buf_size) *
         flat_buf_extent + flat_buf_p->indices[*flat_buf_index_p] + *cur_flat_buf_reg_off_p);
    buf_len_arr[*buf_ol_count_p] = 0;

    while (flat_file_p->blocklens[(*flat_file_index_p)] == 0) {
        (*flat_file_index_p) = ((*flat_file_index_p) + 1) % flat_file_p->count;
    }
    file_off_arr[*file_ol_count_p] = disp +
        (((bytes_into_filetype + *bytes_completed) / flat_file_size) *
         flat_file_extent) + flat_file_p->indices[*flat_file_index_p] + *cur_flat_file_reg_off_p;
    file_len_arr[*file_ol_count_p] = 0;

#ifdef DEBUG_LIST2
    fprintf(stderr, "initial buf_addr_arr[%ld] = %p\n", *buf_ol_count_p,
            buf_addr_arr[*buf_ol_count_p]);
    fprintf(stderr, "initial file_off_arr[%ld] = %ld\n", *file_ol_count_p,
            file_off_arr[*file_ol_count_p]);
#endif

    while (*bytes_completed != total_io_size
           && (*buf_ol_count_p) < max_ol_count && (*file_ol_count_p) < max_ol_count) {
        /* How much data is left in the current piece in
         * the flattened datatypes */
        cur_flat_buf_reg_left = flat_buf_p->blocklens[*flat_buf_index_p]
            - *cur_flat_buf_reg_off_p;
        cur_flat_file_reg_left = flat_file_p->blocklens[*flat_file_index_p]
            - *cur_flat_file_reg_off_p;

#ifdef DEBUG_LIST2
        fprintf(stderr,
                "flat_buf_index=%d flat_buf->blocklens[%d]=%lld\n"
                "cur_flat_buf_reg_left=%ld "
                "*cur_flat_buf_reg_off_p=%ld\n"
                "flat_file_index=%d flat_file->blocklens[%d]=%lld\n"
                "cur_flat_file_reg_left=%ld "
                "*cur_flat_file_reg_off_p=%ld\n"
                "bytes_completed=%ld\n"
                "buf_ol_count=%ld file_ol_count=%ld\n"
                "buf_addr_arr[%ld]=%p file_len_arr[%ld]=%ld\n\n",
                *flat_buf_index_p, *flat_buf_index_p,
                flat_buf_p->blocklens[*flat_buf_index_p],
                cur_flat_buf_reg_left,
                *cur_flat_buf_reg_off_p,
                *flat_file_index_p, *flat_file_index_p,
                flat_file_p->blocklens[*flat_file_index_p],
                cur_flat_file_reg_left,
                *cur_flat_file_reg_off_p,
                *bytes_completed,
                *buf_ol_count_p, *file_ol_count_p,
                *buf_ol_count_p,
                buf_addr_arr[*buf_ol_count_p], *file_ol_count_p, file_len_arr[*file_ol_count_p]);
#endif

        /* What is the size of the next contiguous region agreed
         * upon by both memory and file regions that does not
         * surpass the file size */
        if (cur_flat_buf_reg_left > cur_flat_file_reg_left)
            region_size = cur_flat_file_reg_left;
        else
            region_size = cur_flat_buf_reg_left;

        if (region_size > total_io_size - *bytes_completed)
            region_size = total_io_size - *bytes_completed;

        /* Add this piece to both the mem and file arrays
         * coalescing offset-length pairs if possible and advance
         * the pointers through the flatten mem and file datatypes
         * as well Note: no more than a single piece can be done
         * since we take the smallest one possible */

        if (cur_flat_buf_reg_left == region_size) {
#ifdef DEBUG_LIST2
            fprintf(stderr, "reached end of memory block...\n");
#endif
            (*flat_buf_index_p) = ((*flat_buf_index_p) + 1) % flat_buf_p->count;
            while (flat_buf_p->blocklens[(*flat_buf_index_p)] == 0) {
                (*flat_buf_index_p) = ((*flat_buf_index_p) + 1) % flat_buf_p->count;
            }
            *cur_flat_buf_reg_off_p = 0;

#ifdef COALESCE_REGIONS
            if (*buf_ol_count_p != 0) {
                if (buf_addr_arr[(*buf_ol_count_p) - 1] +
                    buf_len_arr[(*buf_ol_count_p) - 1] == buf_addr_arr[*buf_ol_count_p]) {
                    buf_len_arr[(*buf_ol_count_p) - 1] += region_size;
                } else {
                    buf_len_arr[*buf_ol_count_p] += region_size;
                    (*buf_ol_count_p)++;
                }
            } else {
#endif
                buf_len_arr[*buf_ol_count_p] += region_size;
                (*buf_ol_count_p)++;
#ifdef COALESCE_REGIONS
            }
#endif

            /* Don't prepare for the next piece if we have reached
             * the limit or else it will segment fault. */
            if ((*buf_ol_count_p) != max_ol_count) {
                buf_addr_arr[*buf_ol_count_p] = (char *) buffer +
                    (((*bytes_completed + region_size) / flat_buf_size) *
                     flat_buf_extent +
                     flat_buf_p->indices[*flat_buf_index_p] + (*cur_flat_buf_reg_off_p));
                buf_len_arr[*buf_ol_count_p] = 0;
            }
        } else if (cur_flat_buf_reg_left > region_size) {
#ifdef DEBUG_LIST2
            fprintf(stderr, "advanced %d in memory block...\n", region_size);
#endif
            (*cur_flat_buf_reg_off_p) += region_size;
            buf_len_arr[*buf_ol_count_p] += region_size;
        } else {
            fprintf(stderr, "gen_listio_arr: Error\n");
        }

        /* To calculate the absolute file offset we need to
         * add the disp, how many filetypes we have gone through,
         * the relative block offset in the filetype and how far
         * into the block we have gone. */
        if (cur_flat_file_reg_left == region_size) {
#ifdef DEBUG_LIST2
            fprintf(stderr, "reached end of file block...\n");
#endif
            (*flat_file_index_p) = ((*flat_file_index_p) + 1) % flat_file_p->count;
            while (flat_file_p->blocklens[(*flat_file_index_p)] == 0) {
                (*flat_file_index_p) = ((*flat_file_index_p) + 1) % flat_file_p->count;
            }
            (*cur_flat_file_reg_off_p) = 0;

#ifdef COALESCE_REGIONS
            if (*file_ol_count_p != 0) {
                if (file_off_arr[(*file_ol_count_p) - 1] +
                    file_len_arr[(*file_ol_count_p) - 1] == file_off_arr[*file_ol_count_p]) {
                    file_len_arr[(*file_ol_count_p) - 1] += region_size;
                } else {
                    file_len_arr[*file_ol_count_p] += region_size;
                    (*file_ol_count_p)++;
                }
            } else {
#endif
                file_len_arr[*file_ol_count_p] += region_size;
                (*file_ol_count_p)++;
#ifdef COALESCE_REGIONS
            }
#endif

            /* Don't prepare for the next piece if we have reached
             * the limit or else it will segment fault. */
            if ((*file_ol_count_p) != max_ol_count) {
                file_off_arr[*file_ol_count_p] = disp +
                    (((bytes_into_filetype + *bytes_completed + region_size)
                      / flat_file_size) *
                     flat_file_extent) +
                    flat_file_p->indices[*flat_file_index_p] + (*cur_flat_file_reg_off_p);
                file_len_arr[*file_ol_count_p] = 0;
            }
        } else if (cur_flat_file_reg_left > region_size) {
#ifdef DEBUG_LIST2
            fprintf(stderr, "advanced %d in file block...\n", region_size);
#endif
            (*cur_flat_file_reg_off_p) += region_size;
            file_len_arr[*file_ol_count_p] += region_size;
        } else {
            fprintf(stderr, "gen_listio_arr: Error\n");
        }
#ifdef DEBUG_LIST2
        fprintf(stderr, "------------------------------\n\n");
#endif
        *bytes_completed += region_size;
    }
    /* Increment the count if we stopped in the middle of a
     * memory or file region */
    if (*cur_flat_buf_reg_off_p != 0)
        (*buf_ol_count_p)++;
    if (*cur_flat_file_reg_off_p != 0)
        (*file_ol_count_p)++;

    return 0;
}

#ifdef DEBUG_LIST2
static void print_buf_file_ol_pairs(char *buf_off_arr[],
                                    uint64_t buf_len_arr[],
                                    uint64_t buf_ol_count,
                                    int64_t file_off_arr[],
                                    uint64_t file_len_arr[], int64_t file_ol_count, void *buf,
                                    int rw_type)
{
    int i = -1;

    fprintf(stderr, "buf_ol_pairs(offset,length) count = %ld\n", buf_ol_count);
    for (i = 0; i < buf_ol_count; i++) {
        fprintf(stderr, "(%lld, %ld) ", (long long) buf_off_arr[i], buf_len_arr[i]);
    }
    fprintf(stderr, "\n");

    fprintf(stderr, "file_ol_pairs(offset,length) count = %ld\n", file_ol_count);
    for (i = 0; i < file_ol_count; i++) {
        fprintf(stderr, "(%lld, %ld) ", (long long) file_off_arr[i], file_len_arr[i]);
    }
    fprintf(stderr, "\n\n");

}
#endif

static void ADIOI_BV_TAM_write(ADIO_File fd, const int64_t mem_count, const char **mem_addresses, const uint64_t *mem_sizes, const int64_t file_count, const off_t *file_starts, const uint64_t *file_sizes, off_t **file_offset_ptr, uint64_t **offset_length_ptr, int64_t *number_of_requests, int64_t *total_mem_size) {
    int i, j, k, myrank;
    uint64_t total_memory = 0;
    /* First one is the total number of file offsets to be accessed, the second one is the total memory size. */
    uint64_t bv_meta_data[2];
    uint64_t local_data_size, local_contig_req;
    size_t temp;
    off_t *file_offset, *off_ptr;
    uint64_t *offset_length, *uint64_ptr;
    char *buf_ptr, *tmp_ptr;
    MPI_Datatype new_type, *new_types;
    int array_of_blocklengths[3];
    MPI_Count *array_of_blocklengths_64 = (MPI_Count *) ADIOI_Malloc( (mem_count + 2) * sizeof(MPI_Count) );
    MPI_Aint *array_of_displacements = (MPI_Aint *) ADIOI_Malloc( (mem_count + 3) * sizeof(MPI_Aint) );

    MPI_Request *req = fd->req;
    MPI_Status *sts = fd->sts;
    MPI_Comm_rank(fd->comm, &myrank);
    /* First one is the number of I/O requests. The second one is the size of data (total I/O accesses size). */
    bv_meta_data[0] = (uint64_t) file_count;
    bv_meta_data[1] = 0;
    for ( i = 0; i < mem_count; ++i ) {
        bv_meta_data[1] += mem_sizes[i];
    }

    j = 0;
    if (fd->is_local_aggregator) {
        for ( i = 0; i < fd->nprocs_aggregator; ++i ) {
            if ( fd->aggregator_local_ranks[i] != myrank ){
                MPI_Irecv(fd->bv_meta_data + 2 * i, 2, MPI_UINT64_T, fd->aggregator_local_ranks[i], fd->aggregator_local_ranks[i] + myrank, fd->comm, &req[j++]);
            } else {
                fd->bv_meta_data[2 * i] = bv_meta_data[0];
                fd->bv_meta_data[2 * i + 1] = bv_meta_data[1];
            }
        }
    }
    /* Send message size to local aggregators*/
    if ( fd->my_local_aggregator != myrank ){
        MPI_Issend(bv_meta_data, 2, MPI_UINT64_T, fd->my_local_aggregator, myrank + fd->my_local_aggregator, fd->comm, &req[j++]);
    }
    if (j) {
#ifdef MPI_STATUSES_IGNORE
        MPI_Waitall(j, req, MPI_STATUSES_IGNORE);
#else
        MPI_Waitall(j, req, sts);
#endif
    }
    j = 0;
    if (fd->is_local_aggregator) {
        local_data_size = 0;
        local_contig_req = 0;
        for ( i = 0; i < fd->nprocs_aggregator; ++i ) {
            local_contig_req += fd->bv_meta_data[2 * i];
            local_data_size += fd->bv_meta_data[2 * i + 1];
        }
        /* One buffer for both data and metadata
         * Data buffer comes first, then offsets, finally lengths of the offsets */
        temp = (size_t) ( local_data_size * sizeof(char) + local_contig_req * (sizeof(uint64_t) + sizeof(off_t)) );
        if ( fd->local_buf_size < temp ){
            if (fd->local_buf_size) {
                ADIOI_Free(fd->local_buf);
            }
            fd->local_buf_size = temp;
            fd->local_buf = (char *) ADIOI_Malloc( temp );
        }
        file_offset = (off_t*) (fd->local_buf + local_data_size);
        offset_length = (uint64_t*) (file_offset + local_contig_req);

        *file_offset_ptr = file_offset;
        *offset_length_ptr = offset_length;
        *number_of_requests = (int64_t) local_contig_req;
        *total_mem_size = (uint64_t) local_data_size;

        buf_ptr = fd->local_buf;
        off_ptr = file_offset;
        uint64_ptr = offset_length;
        new_types = (MPI_Datatype *) ADIOI_Malloc((fd->nprocs_aggregator+1) * sizeof(MPI_Datatype));
        for ( i = 0; i < fd->nprocs_aggregator; i++) {
            if ( myrank != fd->aggregator_local_ranks[i] ) {
                array_of_blocklengths[0] = sizeof(char) * fd->bv_meta_data[2 * i + 1];
                array_of_blocklengths[1] = sizeof(off_t) * fd->bv_meta_data[2 * i];
                array_of_blocklengths[2] = sizeof(uint64_t) * fd->bv_meta_data[2 * i];
                array_of_displacements[0] = (MPI_Aint) buf_ptr;
                array_of_displacements[1] = (MPI_Aint) off_ptr;
                array_of_displacements[2] = (MPI_Aint) uint64_ptr;
                MPI_Type_create_hindexed(3, array_of_blocklengths, array_of_displacements, MPI_BYTE, new_types + i);
                MPI_Type_commit(new_types + i);

                MPI_Irecv(MPI_BOTTOM, 1, 
                          new_types[i], fd->aggregator_local_ranks[i], fd->aggregator_local_ranks[i] + myrank, fd->comm, &req[j++]);
                MPI_Type_free(new_types+i);
            } else {
                tmp_ptr = buf_ptr;
                for ( k = 0; k < mem_count; ++k ) {
                    memcpy((void*)tmp_ptr, (void*)mem_addresses[k], sizeof(char) * mem_sizes[k]);
                    tmp_ptr += mem_sizes[k];
                }
            }
            buf_ptr += fd->bv_meta_data[2 * i + 1];
            off_ptr += fd->bv_meta_data[2 * i];
            uint64_ptr += fd->bv_meta_data[2 * i];
        }
        ADIOI_Free(new_types);
    }
    if ( fd->my_local_aggregator != myrank ){
        /* file offsets can have larger than int number of elements. We need to use an extension of native h_indexed_x.
         * Warning: I do not believe file_count can be larger than 32 bit integer because it is parsed from MPI_Datatype. List I/O has some restrictions on this size. Hence I will ignore this 64 bit problem here.*/
/*
        array_of_blocklengths_64[0] = (MPI_Count) sizeof(off_t) * file_count;
        array_of_blocklengths_64[1] = (MPI_Count) sizeof(uint64_t) * file_count;
        array_of_displacements[0] = (MPI_Aint) file_starts;
        array_of_displacements[1] = (MPI_Aint) file_sizes;

        ADIOI_Type_create_hindexed_x(2,
                                     array_of_blocklengths_64,
                                     array_of_displacements,
                                     MPI_BYTE, &new_type);
        MPI_Type_commit(&new_type);
*/
        /* Data comes first, then offsets, finally lengths at the offsets*/
        for ( i = 0; i < mem_count; ++i ) {
            array_of_blocklengths_64[i] = (MPI_Count) mem_sizes[i];
            array_of_displacements[i] = (MPI_Aint) mem_addresses[i];
        }

        array_of_blocklengths_64[mem_count] = (MPI_Count) sizeof(off_t) * file_count;
        array_of_blocklengths_64[mem_count + 1] = (MPI_Count) sizeof(uint64_t) * file_count;
        array_of_displacements[mem_count] = (MPI_Aint) file_starts;
        array_of_displacements[mem_count + 1] = (MPI_Aint) file_sizes;

        ADIOI_Type_create_hindexed_x(mem_count + 2,
                                     array_of_blocklengths_64,
                                     array_of_displacements,
                                     MPI_BYTE, &new_type);

        MPI_Type_commit(&new_type);
        MPI_Isend(MPI_BOTTOM, 1, new_type, fd->my_local_aggregator, myrank + fd->my_local_aggregator, fd->comm, &req[j++]);
    }
    if (j) {
#ifdef MPI_STATUSES_IGNORE
        MPI_Waitall(j, req, MPI_STATUSES_IGNORE);
#else
        MPI_Waitall(j, req, sts);
#endif
    }
    ADIOI_Free(array_of_blocklengths_64);
    ADIOI_Free(array_of_displacements);
}

static void ADIOI_BV_TAM_pre_read(ADIO_File fd, const int64_t mem_count, const uint64_t *mem_sizes, const int64_t file_count, const off_t *file_starts, const uint64_t *file_sizes, off_t **file_offset_ptr, uint64_t **offset_length_ptr, int64_t *number_of_requests, int64_t *total_mem_size) {
    int i, j, k, myrank;
    uint64_t total_memory = 0;
    /* First one is the total number of file offsets to be accessed, the second one is the total memory size. */
    uint64_t bv_meta_data[2];
    uint64_t local_data_size, local_contig_req;
    size_t temp;
    off_t *file_offset, *off_ptr;
    uint64_t *offset_length, *uint64_ptr;
    char *buf_ptr, *tmp_ptr;
    MPI_Datatype new_types2[2];
    MPI_Datatype new_type, new_type2, *new_types;
    int array_of_blocklengths[2];
    MPI_Count array_of_blocklengths_64[2];
    MPI_Aint array_of_displacements[2];

    MPI_Request *req = fd->req;
    MPI_Status *sts = fd->sts;
    MPI_Comm_rank(fd->comm, &myrank);
    /* First one is the number of I/O requests. The second one is the size of data (total I/O accesses size). */
    bv_meta_data[0] = (uint64_t) file_count;
    bv_meta_data[1] = 0;
    for ( i = 0; i < mem_count; ++i ) {
        bv_meta_data[1] += mem_sizes[i];
    }

    j = 0;
    if (fd->is_local_aggregator) {
        for ( i = 0; i < fd->nprocs_aggregator; ++i ) {
            if ( fd->aggregator_local_ranks[i] != myrank ){
                MPI_Irecv(fd->bv_meta_data + 2 * i, 2, MPI_UINT64_T, fd->aggregator_local_ranks[i], fd->aggregator_local_ranks[i] + myrank, fd->comm, &req[j++]);
            } else {
                fd->bv_meta_data[2 * i] = bv_meta_data[0];
                fd->bv_meta_data[2 * i + 1] = bv_meta_data[1];
            }
        }
    }
    /* Send message size to local aggregators*/
    if ( fd->my_local_aggregator != myrank ){
        MPI_Issend(bv_meta_data, 2, MPI_UINT64_T, fd->my_local_aggregator, myrank + fd->my_local_aggregator, fd->comm, &req[j++]);
    }
    if (j) {
#ifdef MPI_STATUSES_IGNORE
        MPI_Waitall(j, req, MPI_STATUSES_IGNORE);
#else
        MPI_Waitall(j, req, sts);
#endif
    }
    j = 0;
    if (fd->is_local_aggregator) {
        local_data_size = 0;
        local_contig_req = 0;
        for ( i = 0; i < fd->nprocs_aggregator; ++i ) {
            local_contig_req += fd->bv_meta_data[2 * i];
            local_data_size += fd->bv_meta_data[2 * i + 1];
        }
        /* One buffer for both data and metadata
         * Data buffer comes first, then offsets, finally lengths of the offsets */
        temp = (size_t) ( local_data_size * sizeof(char) + local_contig_req * (sizeof(uint64_t) + sizeof(off_t)) );
        if ( fd->local_buf_size < temp ){
            if (fd->local_buf_size) {
                ADIOI_Free(fd->local_buf);
            }
            fd->local_buf_size = temp;
            fd->local_buf = (char *) ADIOI_Malloc( temp );
        }
        file_offset = (off_t*) (fd->local_buf + local_data_size);
        offset_length = (uint64_t*) (file_offset + local_contig_req);

        *file_offset_ptr = file_offset;
        *offset_length_ptr = offset_length;
        *number_of_requests = (int64_t) local_contig_req;
        *total_mem_size = (uint64_t) local_data_size;

        buf_ptr = fd->local_buf;
        off_ptr = file_offset;
        uint64_ptr = offset_length;
        new_types = (MPI_Datatype *) ADIOI_Malloc((fd->nprocs_aggregator+1) * sizeof(MPI_Datatype));
        for ( i = 0; i < fd->nprocs_aggregator; i++) {
            if ( myrank != fd->aggregator_local_ranks[i] ) {
                array_of_blocklengths[0] = sizeof(off_t) * fd->bv_meta_data[2 * i];
                array_of_blocklengths[1] = sizeof(uint64_t) * fd->bv_meta_data[2 * i];
                array_of_displacements[0] = (MPI_Aint) off_ptr;
                array_of_displacements[1] = (MPI_Aint) uint64_ptr;
                MPI_Type_create_hindexed(2, array_of_blocklengths, array_of_displacements, MPI_BYTE, new_types + i);
                MPI_Type_commit(new_types + i);

                MPI_Irecv(MPI_BOTTOM, 1, 
                          new_types[i], fd->aggregator_local_ranks[i], fd->aggregator_local_ranks[i] + myrank, fd->comm, &req[j++]);
                MPI_Type_free(new_types+i);
            }
            buf_ptr += fd->bv_meta_data[2 * i + 1];
            off_ptr += fd->bv_meta_data[2 * i];
            uint64_ptr += fd->bv_meta_data[2 * i];
        }
        ADIOI_Free(new_types);
    }
    if ( fd->my_local_aggregator != myrank ){
        /* Data comes first, then offsets, finally lengths at the offsets*/
        array_of_blocklengths[0] = (MPI_Count) sizeof(off_t) * file_count;
        array_of_blocklengths[1] = (MPI_Count) sizeof(uint64_t) * file_count;
        array_of_displacements[0] = (MPI_Aint) file_starts;
        array_of_displacements[1] = (MPI_Aint) file_sizes;

        MPI_Type_create_hindexed(2, array_of_blocklengths, array_of_displacements, MPI_BYTE, &new_type);

        MPI_Type_commit(&new_type);
        MPI_Isend(MPI_BOTTOM, 1, new_type, fd->my_local_aggregator, myrank + fd->my_local_aggregator, fd->comm, &req[j++]);
    }

    if (j) {
#ifdef MPI_STATUSES_IGNORE
        MPI_Waitall(j, req, MPI_STATUSES_IGNORE);
#else
        MPI_Waitall(j, req, sts);
#endif
    }
}

static void ADIOI_BV_TAM_post_read(ADIO_File fd, const int64_t mem_count, const char **mem_addresses, const uint64_t *mem_sizes) {
    int i, j, k, myrank;
    char *buf_ptr, *tmp_ptr;
    MPI_Count *array_of_blocklengths_64 = (MPI_Count *) ADIOI_Malloc( (mem_count + 1) * sizeof(MPI_Count) );
    MPI_Aint *array_of_displacements = (MPI_Aint *) ADIOI_Malloc( (mem_count + 1) * sizeof(MPI_Aint) );

    MPI_Request *req = fd->req;
    MPI_Status *sts = fd->sts;
    MPI_Comm_rank(fd->comm, &myrank);

    j = 0;
    if ( fd->my_local_aggregator != myrank ){
        for ( i = 0; i < mem_count; ++i ) {
            array_of_blocklengths_64[i] = (MPI_Count) mem_sizes[i];
            array_of_displacements[i] = (MPI_Aint) mem_addresses[i];
        }
        ADIOI_Type_create_hindexed_x(mem_count,
                                     array_of_blocklengths_64,
                                     array_of_displacements,
                                     MPI_BYTE, &new_type);
        MPI_Irecv(MPI_BOTTOM, 1, new_type, fd->my_local_aggregator, myrank + fd->my_local_aggregator, fd->comm, &req[j++]);
    }
    if (fd->is_local_aggregator) {
        buf_ptr = fd->local_buf;
        for ( i = 0; i < fd->nprocs_aggregator; i++) {
            if ( myrank != fd->aggregator_local_ranks[i] ) {
                MPI_Issend(buf_ptr, fd->bv_meta_data[2 * i + 1], 
                          MPI_BYTE, fd->aggregator_local_ranks[i], fd->aggregator_local_ranks[i] + myrank, fd->comm, &req[j++]);
            } else {
                tmp_ptr = buf_ptr;
                for ( k = 0; k < mem_count; ++k ) {
                    memcpy((void*)mem_addresses[k], (void*)tmp_ptr, sizeof(char) * mem_sizes[k]);
                    tmp_ptr += mem_sizes[k];
                }
            }
            buf_ptr += fd->bv_meta_data[2 * i + 1];
        }
    }

    if (j) {
#ifdef MPI_STATUSES_IGNORE
        MPI_Waitall(j, req, MPI_STATUSES_IGNORE);
#else
        MPI_Waitall(j, req, sts);
#endif
    }
    ADIOI_Free(array_of_blocklengths_64);
    ADIOI_Free(array_of_displacements);
}

int ADIOI_BV_StridedListIO(ADIO_File fd, void *buf, int count,
                               MPI_Datatype datatype, int file_ptr_type,
                               ADIO_Offset offset, ADIO_Status * status,
                               int *error_code, int rw_type)
{
    /* list I/O parameters */
    int i = -1;
    int tmp_filetype_size = -1;
    int64_t cur_io_size = 0, io_size = 0;
    int etype_size = -1;
    int num_etypes_in_filetype = -1, num_filetypes = -1;
    int etypes_in_filetype = -1, size_into_filetype = -1;
    int bytes_into_filetype = 0;
    MPI_Offset response = 0, total_bytes_accessed = 0;

    /* parameters for offset-length pairs arrays */
    char *buf_off_arr[MAX_OL_COUNT];
    uint64_t buf_len_arr[MAX_OL_COUNT];
    off_t file_off_arr[MAX_OL_COUNT];
    uint64_t file_len_arr[MAX_OL_COUNT];
    int64_t buf_ol_count = 0;
    int64_t file_ol_count = 0;

    /* parameters for flattened memory and file datatypes */
    int flat_buf_index = 0;
    int flat_file_index = 0;
    int64_t cur_flat_buf_reg_off = 0;
    int64_t cur_flat_file_reg_off = 0;
    ADIOI_Flatlist_node *flat_buf_p, *flat_file_p;
    MPI_Count buftype_size = -1, filetype_size = -1;
    MPI_Aint filetype_extent = -1, buftype_extent = -1;;

    /* parameters for TAM */
    off_t *local_file_offset;
    uint64_t *local_offset_length;
    int64_t number_of_requests, local_data_size;

    static char myname[] = "ADIOI_BV_STRIDED_LISTIO";

    if (fd->atomicity) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                           MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__,
                                           MPI_ERR_ARG,
                                           "Atomic noncontiguous writes"
                                           " are not supported by BV", 0);
        return -1;
    }

    MPI_Type_size_x(fd->filetype, &filetype_size);
    if (filetype_size == 0) {
        *error_code = MPI_SUCCESS;
        return -1;
    }
    MPI_Type_extent(fd->filetype, &filetype_extent);
    MPI_Type_size_x(datatype, &buftype_size);
    MPI_Type_extent(datatype, &buftype_extent);
    io_size = buftype_size * count;


    flat_buf_p = ADIOI_Flatten_and_find(datatype);
    flat_file_p = ADIOI_Flatten_and_find(fd->filetype);

    /* Find out where we are in the flattened filetype (the block index,
     * how far into the block, and how many bytes_into_filetype)
     * If the file_ptr_type == ADIO_INDIVIDUAL we will use disp, fp_ind
     * to figure this out (offset should always be zero)
     * If file_ptr_type == ADIO_EXPLICIT, we will use disp and offset
     * to figure this out. */

    etype_size = fd->etype_size;
    num_etypes_in_filetype = filetype_size / etype_size;
    if (file_ptr_type == ADIO_INDIVIDUAL) {
        int flag = 0;
        /* Should have already been flattened in ADIO_Open */
        num_filetypes = -1;
        while (!flag) {
            num_filetypes++;
            for (i = 0; i < flat_file_p->count; i++) {
                /* Start on a non zero-length region */
                if (flat_file_p->blocklens[i]) {
                    if (fd->disp + flat_file_p->indices[i] +
                        (num_filetypes * filetype_extent) +
                        flat_file_p->blocklens[i] > fd->fp_ind &&
                        fd->disp + flat_file_p->indices[i] <= fd->fp_ind) {
                        flat_file_index = i;
                        cur_flat_file_reg_off = fd->fp_ind -
                            (fd->disp + flat_file_p->indices[i] +
                             (num_filetypes * filetype_extent));
                        flag = 1;
                        break;
                    } else
                        bytes_into_filetype += flat_file_p->blocklens[i];
                }
            }
        }
        /* Impossible that we don't find it in this datatype */
        assert(i != flat_file_p->count);
    } else {
        num_filetypes = (int) (offset / num_etypes_in_filetype);
        etypes_in_filetype = (int) (offset % num_etypes_in_filetype);
        size_into_filetype = etypes_in_filetype * etype_size;

        tmp_filetype_size = 0;
        for (i = 0; i < flat_file_p->count; i++) {
            tmp_filetype_size += flat_file_p->blocklens[i];
            if (tmp_filetype_size > size_into_filetype) {
                flat_file_index = i;
                cur_flat_file_reg_off = flat_file_p->blocklens[i] -
                    (tmp_filetype_size - size_into_filetype);
                //bytes_into_filetype = offset * filetype_size - flat_file_p->blocklens[i];
                bytes_into_filetype = offset * filetype_size;
                break;
            }
        }
    }
#ifdef DEBUG_LIST
    fprintf(stderr, "ADIOI_BV_StridedListIO: (fd->fp_ind=%lld,fd->disp=%lld,"
            " offset=%Ld)\n(flat_file_index=%d,cur_flat_file_reg_off=%ld,"
            "bytes_into_filetype=%d)\n",
            fd->fp_ind, fd->disp, offset, flat_file_index,
            cur_flat_file_reg_off, bytes_into_filetype);
#endif
#ifdef DEBUG_LIST2
    fprintf(stderr, "flat_buf:\n");
    for (i = 0; i < flat_buf_p->count; i++)
        fprintf(stderr, "(offset, length) = (%Ld, %lld)\n",
                flat_buf_p->indices[i], flat_buf_p->blocklens[i]);
    fprintf(stderr, "flat_file:\n");
    for (i = 0; i < flat_file_p->count; i++)
        fprintf(stderr, "(offset, length) = (%Ld, %lld)\n",
                flat_file_p->indices[i], flat_file_p->blocklens[i]);
#endif

    /* total data written */
    cur_io_size = 0;
    while (cur_io_size != io_size) {
        /* Initialize the temporarily unrolling lists and
         * and associated variables */
        buf_ol_count = 0;
        file_ol_count = 0;
        for (i = 0; i < MAX_OL_COUNT; i++) {
            buf_off_arr[i] = 0;
            buf_len_arr[i] = 0;
            file_off_arr[i] = 0;
            file_len_arr[i] = 0;
        }

        /* Generate the offset-length pairs for a
         * list I/O operation */
        gen_listio_arr(flat_buf_p,
                       &flat_buf_index,
                       &cur_flat_buf_reg_off,
                       buftype_size,
                       buftype_extent,
                       flat_file_p,
                       &flat_file_index,
                       &cur_flat_file_reg_off,
                       filetype_size,
                       filetype_extent,
                       MAX_OL_COUNT,
                       fd->disp,
                       bytes_into_filetype,
                       &cur_io_size,
                       io_size,
                       buf,
                       buf_off_arr,
                       buf_len_arr, &buf_ol_count, file_off_arr, file_len_arr, &file_ol_count);

        assert(buf_ol_count <= MAX_OL_COUNT);
        assert(file_ol_count <= MAX_OL_COUNT);
#ifdef DEBUG_LIST2
        print_buf_file_ol_pairs(buf_off_arr,
                                buf_len_arr,
                                buf_ol_count,
                                file_off_arr, file_len_arr, file_ol_count, buf, rw_type);
#endif
#ifdef DEBUG_LIST2
#if 0
        do {
            int y, z;
            fprintf(stderr, "ad_pvfs2_io_list.c::\n");
            for (y = 0; y < buf_ol_count; y++) {
                for (z = 0; z < buf_len_arr[y]; z++) {
                    fprintf(stderr, "buf[%d][%d]=%c\n", y, z, ((char *) buf + buf_off_arr[y])[z]);
                }
            }
        } while (0);
#endif
#endif

        /* Run list I/O operation */
        if (rw_type == READ_OP) {
/*
            response =
                bv_read(fd->fs_ptr, fd->filename, buf_ol_count, (const char **) buf_off_arr,
                            buf_len_arr, file_ol_count, file_off_arr, file_len_arr);
*/
            /* Local aggregators gather I/O requests from the processes they are responsible for. */
            ADIOI_BV_TAM_pre_read(fd, buf_ol_count, buf_len_arr, file_ol_count, file_off_arr, file_len_arr, &local_file_offset, &local_offset_length, &number_of_requests, &local_data_size);
            if (fd->is_local_aggregator) {
                response =
                    bv_read(fd->fs_ptr, fd->filename, 1, (const char **) &(fd->local_buf),
                                &local_data_size, number_of_requests, local_file_offset, local_offset_length);
            }
            /* Local aggregators distribute data fetched from BV providers to the processes they are responsible for.*/
            ADIOI_BV_TAM_post_read(fd, buf_ol_count, (const char **) buf_off_arr, buf_len_arr);

#if 0
            char **mem_buf = (char **) ADIOI_Malloc( (buf_ol_count + 1) * sizeof(char*) );
            uint64_t *mem_size2 = (uint64_t *) ADIOI_Malloc( (buf_ol_count + 1) * sizeof(uint64_t) );
            uint64_t j;
            for( i = 0; i < buf_ol_count; ++i ) {
                mem_buf[i] = (char *) ADIOI_Malloc( buf_len_arr[i] * sizeof(char) );
                mem_size2[i] = buf_len_arr[i];
            }

            response =
                bv_read(fd->fs_ptr, fd->filename, buf_ol_count, (const char **) mem_buf,
                            mem_size2, file_ol_count, file_off_arr, file_len_arr);

            for ( i = 0; i < buf_ol_count; ++i ) {
                for ( j = 0; j < mem_size2[i]; ++j ) {
                    if ( mem_buf[i][j] != buf_off_arr[i][j] ) {
                        printf("result is wrong !!!!!!!!!!!!!!!!!!!!!!!!!!!!\n");
                        break;
                    }
                }
                ADIOI_Free(mem_buf[i]);
            }
            ADIOI_Free(mem_buf);
            ADIOI_Free(mem_size2);
#endif

        } else {
            /* Local aggregators gather I/O requests from the processes they are responsible for. */
            ADIOI_BV_TAM_write(fd, buf_ol_count, (const char **) buf_off_arr, buf_len_arr, file_ol_count, file_off_arr, file_len_arr, &local_file_offset, &local_offset_length, &number_of_requests, &local_data_size);
/*
            response =
                bv_write(fd->fs_ptr, fd->filename, buf_ol_count, (const char **) buf_off_arr,
                             buf_len_arr, file_ol_count, file_off_arr, file_len_arr);
*/
            if (fd->is_local_aggregator) {
                response =
                    bv_write(fd->fs_ptr, fd->filename, 1, (const char **) &(fd->local_buf),
                             &local_data_size, number_of_requests, local_file_offset, local_offset_length);
            }
        }
        if (response == -1) {
            fprintf(stderr, "ADIOI_BV_StridedListIO: Warning - bv_"
                    "read/write completed %lld bytes.\n", (long long) response);
            *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                               MPIR_ERR_RECOVERABLE,
                                               myname, __LINE__, -1, "Error in bv i/o\n", 0);
            goto error_state;
        }
        total_bytes_accessed += response;

    }

#ifdef DEBUG_LIST
    fprintf(stderr, "ADIOI_BV_StridedListIO: "
            "total_bytes_accessed=%Ld,ret=%lld\n", total_bytes_accessed, response);
#endif

    if (file_ptr_type == ADIO_INDIVIDUAL)
        fd->fp_ind += total_bytes_accessed;
    *error_code = MPI_SUCCESS;

  error_state:
#ifdef HAVE_STATUS_SET_BYTES
    /* TODO: why the cast? */
    MPIR_Status_set_bytes(status, datatype, total_bytes_accessed);
/* This is a temporary way of filling in status. The right way is to
   keep track of how much data was actually written by ADIOI_BUFFERED_WRITE. */
#endif

    return 0;
}
