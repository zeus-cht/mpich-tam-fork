/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*-
 *
 *
 *   Copyright (C) 1997 University of Chicago.
 *   Copyright (C) 2017 DataDirect Networks.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "adio.h"
#include "adio_extern.h"
#include "ad_bv.h"
#include "ad_bv_common.h"

#include <stdint.h>

#include <bv.h>

#define BV_READ 0
#define BV_WRITE 1

static void BV_IOContig(ADIO_File fd,
                            void *buf,
                            int count,
                            MPI_Datatype datatype,
                            int file_ptr_type,
                            ADIO_Offset offset, ADIO_Status * status, int io_flag, int *error_code)
{
    ssize_t ret;
    MPI_Count datatype_size;
    size_t mem_len;
    off_t file_offset = offset;
    static char myname[] = "ADIOI_BV_IOCONTIG";
    const char *mem_addr;
    uint64_t file_size;

    MPI_Type_size_x(datatype, &datatype_size);
    mem_len = datatype_size * count;

    if (file_ptr_type == ADIO_INDIVIDUAL)
        file_offset = fd->fp_ind;

    mem_addr = buf;
    file_size = mem_len;

    switch (io_flag) {
        case BV_READ:
            ret =
                bv_read(fd->fs_ptr, fd->filename, 1, &mem_addr, &mem_len, 1, &file_offset,
                            &file_size);
            break;
        case BV_WRITE:
            ret =
                bv_write(fd->fs_ptr, fd->filename, 1, &mem_addr, &mem_len, 1, &file_offset,
                             &file_size);
            break;
        default:
            *error_code = MPIO_Err_create_code(MPI_SUCCESS,
                                               MPIR_ERR_RECOVERABLE,
                                               myname, __LINE__, MPI_ERR_IO, "Unknown flag", 0);
            goto exit;

            break;
    };

    /* Let the application decide how to fail */
    if (ret < 0) {
        *error_code = MPI_SUCCESS;
        goto exit;
    }

    if (file_ptr_type == ADIO_INDIVIDUAL)
        fd->fp_ind += ret;
    fd->fp_sys_posn = file_offset + ret;

#ifdef HAVE_STATUS_SET_BYTES
    MPIR_Status_set_bytes(status, datatype, ret);
#endif

    *error_code = MPI_SUCCESS;

  exit:
    return;
}

void ADIOI_BV_ReadContig(ADIO_File fd,
                             void *buf,
                             int count,
                             MPI_Datatype datatype,
                             int file_ptr_type,
                             ADIO_Offset offset, ADIO_Status * status, int *error_code)
{
    BV_IOContig(fd, buf, count, datatype, file_ptr_type, offset, status, BV_READ,
                    error_code);
}

void ADIOI_BV_WriteContig(ADIO_File fd,
                              const void *buf,
                              int count,
                              MPI_Datatype datatype,
                              int file_ptr_type,
                              ADIO_Offset offset, ADIO_Status * status, int *error_code)
{
    BV_IOContig(fd,
                    (void *) buf,
                    count, datatype, file_ptr_type, offset, status, BV_WRITE, error_code);
}

void ADIOI_BV_ReadStrided(ADIO_File fd,
                              void *buf,
                              int count,
                              MPI_Datatype datatype,
                              int file_ptr_type,
                              ADIO_Offset offset, ADIO_Status * status, int *error_code)
{
    ADIOI_BV_OldStridedListIO(fd, buf, count, datatype, file_ptr_type, offset, status,
                                  error_code, READ_OP);
}

void ADIOI_BV_WriteStrided(ADIO_File fd,
                               const void *buf,
                               int count,
                               MPI_Datatype datatype,
                               int file_ptr_type,
                               ADIO_Offset offset, ADIO_Status * status, int *error_code)
{
    ADIOI_BV_OldStridedListIO(fd, (void *) buf, count, datatype, file_ptr_type, offset, status,
                                  error_code, WRITE_OP);
}


void ADIOI_BV_TAM_write(ADIO_File fd,const void *buf, int count, MPI_Datatype datatype, const int64_t mem_count, const char **mem_addresses, const uint64_t *mem_sizes, const int64_t file_count, const off_t *file_starts, const uint64_t *file_sizes, off_t **file_offset_ptr, uint64_t **offset_length_ptr, int64_t *number_of_requests, int64_t *total_mem_size) {
    int i, j, k, myrank;
    uint64_t total_memory = 0;
    /* First one is the total number of file offsets to be accessed, the second one is the total memory size. */
    uint64_t bv_meta_data[2];
    uint64_t local_data_size, local_contig_req;
    size_t temp;
    off_t *file_offset, *off_ptr;
    uint64_t *offset_length, *uint64_ptr;
    char *buf_ptr, *tmp_ptr;
    MPI_Datatype new_type, *new_types, new_types2[3];
    int array_of_blocklengths[3];
    MPI_Aint array_of_displacements[3];
    //MPI_Count *array_of_blocklengths_64 = (MPI_Count *) ADIOI_Malloc( (mem_count + 2) * sizeof(MPI_Count) );
    //MPI_Aint *array_of_displacements = (MPI_Aint *) ADIOI_Malloc( (mem_count + 3) * sizeof(MPI_Aint) );

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
                MPI_Irecv(&(fd->bv_meta_data[2 * i]), 2, MPI_UINT64_T, fd->aggregator_local_ranks[i], fd->aggregator_local_ranks[i] + myrank, fd->comm, &req[j++]);
                //printf("post receive at rank %d from %d\n", myrank, fd->aggregator_local_ranks[i]);
            } else {
                fd->bv_meta_data[2 * i] = bv_meta_data[0];
                fd->bv_meta_data[2 * i + 1] = bv_meta_data[1];
            }
        }
    }
    /* Send message size to local aggregators*/
    if ( fd->my_local_aggregator != myrank ){
        //printf("post send at rank %d to %d\n", myrank, fd->my_local_aggregator);
        MPI_Issend(bv_meta_data, 2, MPI_UINT64_T, fd->my_local_aggregator, myrank + fd->my_local_aggregator, fd->comm, &req[j++]);
    }
    if (j) {
#ifdef MPI_STATUSES_IGNORE
        MPI_Waitall(j, req, MPI_STATUSES_IGNORE);
#else
        MPI_Waitall(j, req, sts);
#endif
    }
    //printf("rank %d-----------------\n", myrank);
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
/*
                tmp_ptr = buf_ptr;
                for ( k = 0; k < mem_count; ++k ) {
                    memcpy((void*)tmp_ptr, (void*)mem_addresses[k], sizeof(char) * mem_sizes[k]);
                    tmp_ptr += mem_sizes[k];
                }
*/
                
                memcpy(off_ptr, file_starts, fd->bv_meta_data[2 * i] * sizeof(off_t));
                memcpy(uint64_ptr, file_sizes, fd->bv_meta_data[2 * i] * sizeof(off_t));
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

/*
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
*/

        array_of_blocklengths[0] = count;
        array_of_blocklengths[1] = sizeof(ADIO_Offset) * file_count;
        array_of_blocklengths[2] = sizeof(ADIO_Offset) * file_count;
        array_of_displacements[0] = (MPI_Aint) buf;
        array_of_displacements[1] = (MPI_Aint) file_starts;
        array_of_displacements[2] = (MPI_Aint) file_sizes;

        new_types2[0] = datatype;
        new_types2[1] = MPI_BYTE;
        new_types2[2] = MPI_BYTE;
        MPI_Type_struct(3,
                       array_of_blocklengths,
                       array_of_displacements,
                       new_types2,
                       &new_type);


        MPI_Type_commit(&new_type);
        MPI_Issend(MPI_BOTTOM, 1, new_type, fd->my_local_aggregator, myrank + fd->my_local_aggregator, fd->comm, &req[j++]);
        MPI_Type_free(&new_type);
    }
    if (j) {
#ifdef MPI_STATUSES_IGNORE
        MPI_Waitall(j, req, MPI_STATUSES_IGNORE);
#else
        MPI_Waitall(j, req, sts);
#endif
    }
    //ADIOI_Free(array_of_blocklengths_64);
    //ADIOI_Free(array_of_displacements);
}

void ADIOI_BV_TAM_pre_read(ADIO_File fd, const int64_t mem_count, const uint64_t *mem_sizes, const int64_t file_count, const off_t *file_starts, const uint64_t *file_sizes, off_t **file_offset_ptr, uint64_t **offset_length_ptr, int64_t *number_of_requests, int64_t *total_mem_size) {
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

        //buf_ptr = fd->local_buf;
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
            } else {
                memcpy(off_ptr, file_starts, sizeof(off_t) * fd->bv_meta_data[2 * i]);
                memcpy(uint64_ptr, file_sizes, sizeof(off_t) * fd->bv_meta_data[2 * i]);
            }
            //buf_ptr += fd->bv_meta_data[2 * i + 1];
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
        MPI_Type_free(&new_type);
    }

    if (j) {
#ifdef MPI_STATUSES_IGNORE
        MPI_Waitall(j, req, MPI_STATUSES_IGNORE);
#else
        MPI_Waitall(j, req, sts);
#endif
    }
}

void ADIOI_BV_TAM_post_read(ADIO_File fd, const int64_t mem_count, const char **mem_addresses, const uint64_t *mem_sizes) {
    int i, j, k, myrank;
    char *buf_ptr, *tmp_ptr;
    MPI_Count *array_of_blocklengths_64 = (MPI_Count *) ADIOI_Malloc( (mem_count + 1) * sizeof(MPI_Count) );
    MPI_Aint *array_of_displacements = (MPI_Aint *) ADIOI_Malloc( (mem_count + 1) * sizeof(MPI_Aint) );
    MPI_Datatype new_type;

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

#define BV_MAX_REQUEST 4096

void ADIOI_BV_WriteStridedColl(ADIO_File fd,
                               const void *buf,
                               int count,
                               MPI_Datatype datatype,
                               int file_ptr_type,
                               ADIO_Offset offset, ADIO_Status * status, int *error_code)
{
    int i, j, contig_access_count = 0;
    ADIO_Offset *offset_list = NULL, start_offset, end_offset;
    ADIO_Offset *len_list = NULL;
    MPI_Count buftype_size;
    MPI_Type_size_x(datatype, &buftype_size);

    MPI_Count contig_buf_size;
    char *contig_buf, *tmp_ptr;
    int position = 0;
    MPI_Offset response = 0;
    MPI_Request req[2];
    MPI_Status sts[2];
    int myrank;
    int ntimes, request_processed;
    MPI_Count mem_processed;

    /* parameters for TAM */
    off_t *local_file_offset;
    uint64_t *local_offset_length;
    int64_t number_of_requests, local_data_size;

    MPI_Comm_rank(fd->comm, &myrank);

    ADIOI_Calc_my_off_len(fd, count, datatype, file_ptr_type, offset,
                          &offset_list, &len_list, &start_offset,
                          &end_offset, &contig_access_count);

    contig_buf_size = buftype_size * count;
    contig_buf = (char *) ADIOI_Malloc( sizeof(char) * contig_buf_size );
/*
    MPI_Irecv(contig_buf, contig_buf_size, MPI_BYTE, myrank, myrank, fd->comm, &req[0]);
    MPI_Isend(buf, count, datatype, myrank, myrank, fd->comm, &req[1]);
    MPI_Waitall(2, req, sts);
*/
    //MPI_Pack(buf, count, datatype, contig_buf, contig_buf_size, &position, fd->comm);
 
    off_t *bv_file_offset = (off_t *) ADIOI_Malloc( sizeof(off_t) * contig_access_count );
    uint64_t *bv_file_sizes = (uint64_t *) ADIOI_Malloc( sizeof(uint64_t) * contig_access_count );
    for ( i = 0; i < contig_access_count; ++i ) {
        bv_file_offset[i] = (off_t) offset_list[i];
        bv_file_sizes[i] = (uint64_t) len_list[i];
    }
    //printf("rank 0 before bv_write, data size = %llu, contig access account = %d\n", (long long unsigned)contig_buf_size, contig_access_count);
    //ADIOI_BV_TAM_write(fd, buf, count, datatype, 1, (const char **) &(contig_buf), (uint64_t*) (&contig_buf_size), (int64_t) contig_access_count, bv_file_offset, bv_file_sizes, &local_file_offset, &local_offset_length, &number_of_requests, &local_data_size);
/*
    ADIOI_Free(contig_buf);
    ADIOI_Free(offset_list);
    //ADIOI_Free(len_list);
    ADIOI_Free(bv_file_offset);
    ADIOI_Free(bv_file_sizes);

    if ( !fd->is_local_aggregator ) {
        // Local aggregators are going to proxy the rest of work.
        return;
    }

    contig_buf = fd->local_buf;
    contig_buf_size = local_data_size;
    bv_file_offset = local_file_offset;
    bv_file_sizes = local_offset_length;
    contig_access_count = number_of_requests;
*/  

    ntimes = (contig_access_count + BV_MAX_REQUEST - 1) / BV_MAX_REQUEST;
    tmp_ptr = contig_buf;
    for ( i = 0 ; i < ntimes; ++i ) {
        if (contig_access_count - i * BV_MAX_REQUEST < BV_MAX_REQUEST) {
            request_processed = contig_access_count - i * BV_MAX_REQUEST;
        } else {
            request_processed = BV_MAX_REQUEST;
        }
        mem_processed = 0;
        for ( j = 0; j < request_processed; ++j ) {
            mem_processed += bv_file_sizes[i * BV_MAX_REQUEST + j];
        }

        response = bv_write(fd->fs_ptr, fd->filename, 1, (const char **) &(tmp_ptr), (uint64_t*) (&mem_processed), (int64_t) request_processed, bv_file_offset + i * BV_MAX_REQUEST, bv_file_sizes + i * BV_MAX_REQUEST);
        tmp_ptr += mem_processed;
    }

    ADIOI_Free(contig_buf);
    ADIOI_Free(offset_list);
    //ADIOI_Free(len_list);
    ADIOI_Free(bv_file_offset);
    ADIOI_Free(bv_file_sizes);
    //response = bv_write(fd->fs_ptr, fd->filename, 1, (const char **) &contig_buf, (uint64_t*) (&contig_buf_size), (int64_t) contig_access_count, bv_file_offset, bv_file_sizes);
}
