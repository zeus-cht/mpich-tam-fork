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

#define BV_MAX_REQUEST 1000

void ADIOI_BV_WriteStrided(ADIO_File fd,
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
    char *contig_buf;
    int position = 0;
    MPI_Offset response = 0;
    MPI_Request req[2];
    MPI_Status sts[2];
    int myrank;
    int ntimes, request_processed;
    MPI_Count mem_processed, temp;

    MPI_Comm_rank(fd->comm, &myrank);

    ADIOI_Calc_my_off_len(fd, count, datatype, file_ptr_type, offset,
                          &offset_list, &len_list, &start_offset,
                          &end_offset, &contig_access_count);

    contig_buf_size = buftype_size * count;
    contig_buf = (char *) ADIOI_Malloc( sizeof(char) * contig_buf_size );

    MPI_Irecv(contig_buf, contig_buf_size, MPI_BYTE, myrank, myrank, fd->comm, &req[0]);
    MPI_Isend(buf, count, datatype, myrank, myrank, fd->comm, &req[1]);
    //MPI_Pack(buf, count, datatype, contig_buf, contig_buf_size, &position, fd->comm);
    MPI_Waitall(2, req, sts);
 
    off_t *bv_file_offset = (off_t *) ADIOI_Malloc( sizeof(off_t) * contig_access_count );
    uint64_t *bv_file_sizes = (uint64_t *) ADIOI_Malloc( sizeof(uint64_t) * contig_access_count );
    for ( i = 0; i < contig_access_count; ++i ) {
        bv_file_offset[i] = (off_t) offset_list[i];
        bv_file_sizes[i] = (uint64_t) len_list[i];
    }
    if (!myrank) {
        printf("rank 0 before bv_write, data size = %llu, contig access account = %d\n", (long long unsigned)contig_buf_size, contig_access_count);
    }
    
    ntimes = (contig_access_count + BV_MAX_REQUEST - 1) / BV_MAX_REQUEST;
    mem_processed = 0;
    for ( i = 0 ; i < ntimes; ++i ) {
        if (contig_access_count - i * BV_MAX_REQUEST < BV_MAX_REQUEST) {
            request_processed = contig_access_count - i * BV_MAX_REQUEST;
        } else {
            request_processed = BV_MAX_REQUEST;
        }
        temp = 0;
        for ( j = 0; j < request_processed; ++j ) {
            temp += len_list[i * BV_MAX_REQUEST + j];
        }
        if (!myrank) {
            printf("rank 0 before bv_write, data size = %llu, contig access account = %d, round = %d\n", (long long unsigned)temp, request_processed, i);
        }
        response = bv_write(fd->fs_ptr, fd->filename, 1, (const char **) &(contig_buf + mem_processed), (uint64_t*) (&temp), (int64_t) request_processed, bv_file_offset + i * BV_MAX_REQUEST, bv_file_sizes + i * BV_MAX_REQUEST);
        mem_processed += temp;
    }
    //response = bv_write(fd->fs_ptr, fd->filename, 1, (const char **) &contig_buf, (uint64_t*) (&contig_buf_size), (int64_t) contig_access_count, bv_file_offset, bv_file_sizes);

    if (!myrank) {
        printf("rank 0 after bv_write\n");
    }
    ADIOI_Free(contig_buf);
    ADIOI_Free(offset_list);
    //ADIOI_Free(len_list);
    ADIOI_Free(bv_file_offset);
    ADIOI_Free(bv_file_sizes);
/*
    ADIOI_BV_OldStridedListIO(fd, (void *) buf, count, datatype, file_ptr_type, offset, status,
                                  error_code, WRITE_OP);
*/
}
