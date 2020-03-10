/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *
 *   Copyright (C) 1997 University of Chicago.
 *   See COPYRIGHT notice in top-level directory.
 */

#include "adio.h"
#include "adio_extern.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#define TIMER_SIZE 18

int write_logs(ADIO_File fd){
    int nprocs;
    MPI_Comm_size(fd->comm, &nprocs);
    char filename[1024];
    sprintf(filename,"collective_write_results_%d.csv",nprocs);
    FILE* stream = fopen(filename,"r");
    if (stream){
        fclose(stream);
        stream = fopen(filename,"a");
    } else {
        stream = fopen(filename,"w");
        fprintf(stream,"# of processes,");
        fprintf(stream,"# of global aggregators,");
        fprintf(stream,"# of local aggregators,");
        fprintf(stream,"# of collective loops,");
        // patched
        fprintf(stream,"# of metadata send,");
        fprintf(stream,"# of metadata recv,");
        fprintf(stream,"# of data send,");
        fprintf(stream,"# of data recv,");
        fprintf(stream,"comm type meta,");
        fprintf(stream,"comm type write,");
        fprintf(stream,"comm limit,");
        fprintf(stream,"barrier,");
        // end of patch
        fprintf(stream,"calculate offsets,");

        fprintf(stream,"intra type,");
        fprintf(stream,"intra heap sort,");
        fprintf(stream,"intra memcpy,");
        fprintf(stream,"intra wait meta,");
        fprintf(stream,"intra wait data,");
        fprintf(stream,"intra total,");

        fprintf(stream,"calc my request,");
        fprintf(stream,"calc other request,");

        fprintf(stream,"inter type,");
        fprintf(stream,"inter heap sort,");
        fprintf(stream,"inter unpack,");
        fprintf(stream,"inter data sieving,");
        fprintf(stream,"inter wait data,");
        fprintf(stream,"inter total,");

        fprintf(stream,"I/O,");

        fprintf(stream,"other,");

        fprintf(stream,"first phase,");
        fprintf(stream,"second phase,");
        fprintf(stream,"third phase,");
        fprintf(stream,"total\n");
    }
    fprintf(stream,"%d,",nprocs);

    fprintf(stream,"%d,",fd->hints->cb_nodes);
    fprintf(stream,"%d,",fd->local_aggregator_size);
    fprintf(stream,"%d,",fd->ntimes);
    //patched
    fprintf(stream,"%llu,",fd->meta_send_count);
    fprintf(stream,"%llu,",fd->meta_recv_count);
    fprintf(stream,"%llu,",fd->data_send_count);
    fprintf(stream,"%llu,",fd->data_recv_count);
    fprintf(stream,"%d,",fd->alltoall_type_meta);
    fprintf(stream,"%d,",fd->alltoall_type_write);
    fprintf(stream,"%d,",fd->comm_limit);
    fprintf(stream,"%d,",fd->try_barrier);

    fprintf(stream,"%lf,",fd->calc_offset_time);

    fprintf(stream,"%lf,",fd->intra_type_time);
    fprintf(stream,"%lf,",fd->intra_heap_time);
    fprintf(stream,"%lf,",fd->intra_memcpy_time);
    fprintf(stream,"%lf,",fd->intra_wait_offset_time);
    fprintf(stream,"%lf,",fd->intra_wait_data_time);
    fprintf(stream,"%lf,",fd->total_intra_time);

    fprintf(stream,"%lf,",fd->calc_my_request_time);
    fprintf(stream,"%lf,",fd->calc_other_request_time);

    fprintf(stream,"%lf,",fd->inter_type_time);
    fprintf(stream,"%lf,",fd->inter_heap_time);
    fprintf(stream,"%lf,",fd->inter_unpack_time);
    fprintf(stream,"%lf,",fd->inter_ds_time);
    fprintf(stream,"%lf,",fd->inter_wait_time);
    fprintf(stream,"%lf,",fd->total_inter_time);

    fprintf(stream,"%lf,",fd->io_time);

    fprintf(stream,"%lf,",fd->total_time - fd->io_time - fd->total_intra_time - fd->total_inter_time - fd->calc_my_request_time - fd->calc_other_request_time - fd->calc_offset_time);

    fprintf(stream,"%lf,",fd->calc_offset_time + fd->total_intra_time);
    fprintf(stream,"%lf,",fd->total_inter_time + fd->calc_my_request_time + fd->calc_other_request_time);
    fprintf(stream,"%lf,",fd->io_time);

    fprintf(stream,"%lf\n",fd->total_time);
    fclose(stream);
    return 0;
}

int read_logs(ADIO_File fd){
    int nprocs;
    MPI_Comm_size(fd->comm, &nprocs);
    char filename[1024];
    sprintf(filename,"collective_read_results_%d.csv",nprocs);

    FILE* stream = fopen(filename,"r");
    if (stream){
        fclose(stream);
        stream = fopen(filename,"a");
    } else {
        stream = fopen(filename,"w");
        fprintf(stream,"# of processes,");
        fprintf(stream,"# of global aggregators,");
        fprintf(stream,"# of local aggregators,");
        fprintf(stream,"# of collective loops,");
        // patched
        fprintf(stream,"# of metadata send,");
        fprintf(stream,"# of metadata recv,");
        fprintf(stream,"# of data send,");
        fprintf(stream,"# of data recv,");
        fprintf(stream,"comm meta type,");
        fprintf(stream,"comm write type,");
        fprintf(stream,"comm limit,");
        fprintf(stream,"barrier,");
        // end of patch
        fprintf(stream,"calculate offsets,");

        fprintf(stream,"intra type,");
        fprintf(stream,"intra heap sort,");
        fprintf(stream,"intra memcpy,");
        fprintf(stream,"intra wait meta,");
        fprintf(stream,"intra wait data,");
        fprintf(stream,"intra total,");

        fprintf(stream,"calc my request,");
        fprintf(stream,"calc other request,");
        fprintf(stream,"calc file domain,");

        fprintf(stream,"inter type,");
        fprintf(stream,"inter wait data,");
        fprintf(stream,"inter total,");

        fprintf(stream,"I/O,");

        fprintf(stream,"other,");

        fprintf(stream,"first phase,");
        fprintf(stream,"second phase,");
        fprintf(stream,"third phase,");
        fprintf(stream,"total\n");
    }
    fprintf(stream,"%d,",nprocs);

    fprintf(stream,"%d,",fd->hints->cb_nodes);
    fprintf(stream,"%d,",fd->local_aggregator_size);
    fprintf(stream,"%d,",fd->read_ntimes);

    //patched
    fprintf(stream,"%llu,",fd->meta_send_count);
    fprintf(stream,"%llu,",fd->meta_recv_count);
    fprintf(stream,"%llu,",fd->read_data_send_count);
    fprintf(stream,"%llu,",fd->read_data_recv_count);
    fprintf(stream,"%d,",fd->alltoall_type_meta);
    fprintf(stream,"%d,",fd->alltoall_type_write);
    fprintf(stream,"%d,",fd->comm_limit);
    fprintf(stream,"%d,",fd->try_barrier);

    fprintf(stream,"%lf,",fd->read_calc_offset_time);

    fprintf(stream,"%lf,",fd->read_intra_type_time);
    fprintf(stream,"%lf,",fd->read_intra_heap_time);
    fprintf(stream,"%lf,",fd->read_intra_memcpy_time);
    fprintf(stream,"%lf,",fd->read_intra_wait_offset_time);
    fprintf(stream,"%lf,",fd->read_intra_wait_data_time);
    fprintf(stream,"%lf,",fd->read_total_intra_time);

    fprintf(stream,"%lf,",fd->read_calc_my_request_time);
    fprintf(stream,"%lf,",fd->read_calc_other_request_time);
    fprintf(stream,"%lf,",fd->read_calc_file_domain_time);

    fprintf(stream,"%lf,",fd->read_inter_type_time);
    fprintf(stream,"%lf,",fd->read_inter_wait_time);
    fprintf(stream,"%lf,",fd->read_total_inter_time);

    fprintf(stream,"%lf,",fd->read_io_time);

    fprintf(stream,"%lf,",fd->read_total_time - fd->read_io_time - fd->read_total_intra_time - fd->read_total_inter_time - fd->read_calc_my_request_time - fd->read_calc_other_request_time - fd->read_calc_offset_time - fd->read_calc_file_domain_time);

    fprintf(stream,"%lf,",fd->read_calc_offset_time + fd->read_total_intra_time);
    fprintf(stream,"%lf,",fd->read_total_inter_time + fd->read_calc_my_request_time + fd->read_calc_other_request_time + fd->read_calc_file_domain_time);
    fprintf(stream,"%lf,",fd->read_io_time);

    fprintf(stream,"%lf\n",fd->read_total_time);
    fclose(stream);
    return 0;
}

int comm_logs(ADIO_File fd, MPI_Count **request_sum_ptr, MPI_Count **request_sum2_ptr){
    int nprocs;
    MPI_Comm_size(fd->comm, &nprocs);
    char filename[1024];
    sprintf(filename,"collective_comm_size_%d.csv",nprocs);

    FILE* stream = fopen(filename,"r");
    if (stream){
        fclose(stream);
        stream = fopen(filename,"a");
    } else {
        stream = fopen(filename,"w");
        fprintf(stream,"# of processes,");
        fprintf(stream,"# of global aggregators,");
        fprintf(stream,"# of local aggregators,");
        fprintf(stream,"# of write collective loops,");
        fprintf(stream,"# of read collective loops,");
        // all processes
        fprintf(stream,"# of non-contiguous requests,");
        fprintf(stream,"# of metadata send,");
        fprintf(stream,"# of metadata recv,");
        fprintf(stream,"# of write data send,");
        fprintf(stream,"# of write data recv,");
        fprintf(stream,"# of read data send,");
        fprintf(stream,"# of read data recv,");
        fprintf(stream,"max # of local requests,");
        fprintf(stream,"max # of metadata send,");
        fprintf(stream,"max # of metadata recv,");
        fprintf(stream,"max # of write data send,");
        fprintf(stream,"max # of write data recv,");
        fprintf(stream,"max # of read data send,");
        fprintf(stream,"max # of read data recv,");
        fprintf(stream,"min # of local requests,");
        fprintf(stream,"min # of metadata send,");
        fprintf(stream,"min # of metadata recv,");
        fprintf(stream,"min # of write data send,");
        fprintf(stream,"min # of write data recv,");
        fprintf(stream,"min # of read data send,");
        fprintf(stream,"min # of read data recv,");
        // all global aggregators
        fprintf(stream,"max # of requests gathered at GA,");
        fprintf(stream,"min # of requests gathered at GA,");
        fprintf(stream,"# of metadata send at GA,");
        fprintf(stream,"# of metadata recv at GA,");
        fprintf(stream,"# of write data send at GA,");
        fprintf(stream,"# of write data recv at GA,");
        fprintf(stream,"# of read data send at GA,");
        fprintf(stream,"# of read data recv at GA,");
        fprintf(stream,"max # of metadata send at GA,");
        fprintf(stream,"max # of metadata recv at GA,");
        fprintf(stream,"max # of write data send at GA,");
        fprintf(stream,"max # of write data recv at GA,");
        fprintf(stream,"max # of read data send at GA,");
        fprintf(stream,"max # of read data recv at GA,");
        fprintf(stream,"min # of metadata send at GA,");
        fprintf(stream,"min # of metadata recv at GA,");
        fprintf(stream,"min # of write data send at GA,");
        fprintf(stream,"min # of write data recv at GA,");
        fprintf(stream,"min # of read data send at GA,");
        fprintf(stream,"min # of read data recv at GA\n");
    }
    fprintf(stream,"%d,",nprocs);
    fprintf(stream,"%d,",fd->hints->cb_nodes);
    fprintf(stream,"%d,",fd->local_aggregator_size);
    fprintf(stream,"%d,",fd->ntimes);
    fprintf(stream,"%d,",fd->read_ntimes);

    fprintf(stream,"%llu,",request_sum_ptr[0][0]);
    fprintf(stream,"%llu,",request_sum_ptr[0][1]);
    fprintf(stream,"%llu,",request_sum_ptr[0][2]);
    fprintf(stream,"%llu,",request_sum_ptr[0][3]);
    fprintf(stream,"%llu,",request_sum_ptr[0][4]);
    fprintf(stream,"%llu,",request_sum_ptr[0][5]);
    fprintf(stream,"%llu,",request_sum_ptr[0][6]);
    fprintf(stream,"%llu,",request_sum_ptr[1][0]);
    fprintf(stream,"%llu,",request_sum_ptr[1][1]);
    fprintf(stream,"%llu,",request_sum_ptr[1][2]);
    fprintf(stream,"%llu,",request_sum_ptr[1][3]);
    fprintf(stream,"%llu,",request_sum_ptr[1][4]);
    fprintf(stream,"%llu,",request_sum_ptr[1][5]);
    fprintf(stream,"%llu,",request_sum_ptr[1][6]);
    fprintf(stream,"%llu,",request_sum_ptr[2][0]);
    fprintf(stream,"%llu,",request_sum_ptr[2][1]);
    fprintf(stream,"%llu,",request_sum_ptr[2][2]);
    fprintf(stream,"%llu,",request_sum_ptr[2][3]);
    fprintf(stream,"%llu,",request_sum_ptr[2][4]);
    fprintf(stream,"%llu,",request_sum_ptr[2][5]);
    fprintf(stream,"%llu,",request_sum_ptr[2][6]);

    fprintf(stream,"%llu,",request_sum2_ptr[1][7]);
    fprintf(stream,"%llu,",request_sum2_ptr[2][7]);
    fprintf(stream,"%llu,",request_sum2_ptr[0][1]);
    fprintf(stream,"%llu,",request_sum2_ptr[0][2]);
    fprintf(stream,"%llu,",request_sum2_ptr[0][3]);
    fprintf(stream,"%llu,",request_sum2_ptr[0][4]);
    fprintf(stream,"%llu,",request_sum2_ptr[0][5]);
    fprintf(stream,"%llu,",request_sum2_ptr[0][6]);
    fprintf(stream,"%llu,",request_sum2_ptr[1][1]);
    fprintf(stream,"%llu,",request_sum2_ptr[1][2]);
    fprintf(stream,"%llu,",request_sum2_ptr[1][3]);
    fprintf(stream,"%llu,",request_sum2_ptr[1][4]);
    fprintf(stream,"%llu,",request_sum2_ptr[1][5]);
    fprintf(stream,"%llu,",request_sum2_ptr[1][6]);
    fprintf(stream,"%llu,",request_sum2_ptr[2][1]);
    fprintf(stream,"%llu,",request_sum2_ptr[2][2]);
    fprintf(stream,"%llu,",request_sum2_ptr[2][3]);
    fprintf(stream,"%llu,",request_sum2_ptr[2][4]);
    fprintf(stream,"%llu,",request_sum2_ptr[2][5]);
    fprintf(stream,"%llu\n",request_sum2_ptr[2][6]);
    fclose(stream);
    return 0;
}

int write_timings(ADIO_File fd, int myrank, MPI_Count **request_sum, MPI_Count **request_sum2, int is_write){
    if (myrank == 0){
        if ( is_write ){
            write_logs(fd);
        }else{
            read_logs(fd);
        }
        comm_logs(fd, request_sum, request_sum2);
    }
    return 0;
}

int print_timing_results(ADIO_File fd, int myrank, int is_write){
    MPI_Comm new_comm;
    int color;
    MPI_Count request_sum[2];
    MPI_Count request_count[2];
    MPI_Count **request_sum_ptr = (MPI_Count **) ADIOI_Malloc(3 * sizeof(MPI_Count*));
    MPI_Count **request_sum2_ptr = (MPI_Count **) ADIOI_Malloc(3 * sizeof(MPI_Count*));
    MPI_Count **request_count_ptr = (MPI_Count **) ADIOI_Malloc(3 * sizeof(MPI_Count*));

    if (fd->is_agg){
        color = 1;
    }else{
        color = 0;
    }
    MPI_Comm_split(fd->comm, color, myrank, &new_comm);

    request_sum_ptr[0] = (MPI_Count *) ADIOI_Malloc(7 * sizeof(MPI_Count));
    request_sum_ptr[1] = (MPI_Count *) ADIOI_Malloc(8 * sizeof(MPI_Count));
    request_sum_ptr[2] = (MPI_Count *) ADIOI_Malloc(8 * sizeof(MPI_Count));
    request_sum2_ptr[0] = (MPI_Count *) ADIOI_Malloc(7 * sizeof(MPI_Count));
    request_sum2_ptr[1] = (MPI_Count *) ADIOI_Malloc(8 * sizeof(MPI_Count));
    request_sum2_ptr[2] = (MPI_Count *) ADIOI_Malloc(8 * sizeof(MPI_Count));
    request_count_ptr[0] = (MPI_Count *) ADIOI_Malloc(7 * sizeof(MPI_Count));
    request_count_ptr[1] = (MPI_Count *) ADIOI_Malloc(8 * sizeof(MPI_Count));
    request_count_ptr[2] = (MPI_Count *) ADIOI_Malloc(8 * sizeof(MPI_Count));


    request_count[0] = 0;
    request_count[1] = 0;
    MPI_Reduce( request_count, request_sum, 2, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, fd->comm);

    request_count_ptr[0][0] = fd->local_request_count;
    request_count_ptr[0][1] = fd->meta_send_count;
    request_count_ptr[0][2] = fd->meta_recv_count;
    request_count_ptr[0][3] = fd->data_send_count;
    request_count_ptr[0][4] = fd->data_recv_count;
    request_count_ptr[0][5] = fd->read_data_send_count;
    request_count_ptr[0][6] = fd->read_data_recv_count;

    request_count_ptr[1][0] = fd->local_request_count;
    request_count_ptr[1][1] = fd->meta_send_count;
    request_count_ptr[1][2] = fd->meta_recv_count; 
    request_count_ptr[1][3] = fd->data_send_count;
    request_count_ptr[1][4] = fd->data_recv_count;
    request_count_ptr[1][5] = fd->read_data_send_count;
    request_count_ptr[1][6] = fd->read_data_recv_count;
    request_count_ptr[1][7] = fd->gathered_request_count;

    request_count_ptr[2][0] = fd->local_request_count;
    request_count_ptr[2][1] = fd->meta_send_count;
    request_count_ptr[2][2] = fd->meta_recv_count; 
    request_count_ptr[2][3] = fd->data_send_count;
    request_count_ptr[2][4] = fd->data_recv_count;
    request_count_ptr[2][5] = fd->read_data_send_count;
    request_count_ptr[2][6] = fd->read_data_recv_count;
    request_count_ptr[2][7] = fd->gathered_request_count;

    MPI_Reduce( request_count_ptr[0], request_sum_ptr[0], 7, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, fd->comm);
    MPI_Reduce( request_count_ptr[1], request_sum_ptr[1], 8, MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, fd->comm);
    MPI_Reduce( request_count_ptr[2], request_sum_ptr[2], 8, MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, fd->comm);
    if (fd->is_agg){
        MPI_Reduce( request_count_ptr[0], request_sum2_ptr[0], 7, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, new_comm);
        MPI_Reduce( request_count_ptr[1], request_sum2_ptr[1], 8, MPI_UNSIGNED_LONG_LONG, MPI_MAX, 0, new_comm);
        MPI_Reduce( request_count_ptr[2], request_sum2_ptr[2], 8, MPI_UNSIGNED_LONG_LONG, MPI_MIN, 0, new_comm);
    }
    MPI_Comm_free(&new_comm);

    if (myrank==0){
        if (is_write){
            printf("rank 0 write timing+--------------------------------------------------------------------------+\n");
            printf("| write collective write cycle breakdown for ntimes = %d\n", fd->ntimes);
            printf("|\n");
            printf("| write collective local requests = %llu\n",fd->local_request_count);
            printf("| write collective max local requests = %llu, min local requests = %llu, total_request=%llu\n",request_sum_ptr[1][0],request_sum_ptr[2][0],request_sum_ptr[0][0]);
            printf("| write collective max gathered requests = %llu, min gathered requests = %llu\n",request_sum_ptr[1][5],request_sum_ptr[2][5]);
            printf("| write collective max meta send count = %llu, min meta send count = %llu, total meta send count=%llu\n",request_sum_ptr[1][1],request_sum_ptr[2][1],request_sum_ptr[0][1]);
            printf("| write collective max meta recv count = %llu, min meta recv count = %llu, total meta recv count=%llu\n",request_sum_ptr[1][2],request_sum_ptr[2][2],request_sum_ptr[0][2]);
            printf("| write collective max data send count = %llu, min data send count = %llu, total data send count=%llu\n",request_sum_ptr[1][3],request_sum_ptr[2][3],request_sum_ptr[0][3]);
            printf("| write collective max data recv count = %llu, min data recv count = %llu, total data recv count=%llu\n",request_sum_ptr[1][4],request_sum_ptr[2][4],request_sum_ptr[0][4]);

            printf("| write collective write cycle breakdown for ntimes = %d\n", fd->ntimes);
            printf("|\n");
            printf("| write total number of request = %lld, merged request = %lld, ratio = %lf\n", request_sum[0], request_sum[1], (request_sum[1] + .001)/ (request_sum[0] + .001));
            printf("|\n");
            printf("| write 1 max exchange_write time = %lf\n", fd->exchange_write_time);
            printf("|\n");
            printf("| write 1 max calculate file offset time = %lf\n", fd->calc_offset_time);
            printf("|\n");
            printf("| write 1 max intra datatype create = %lf\n", fd->intra_type_time);
            printf("| write 1 max intra heap = %lf\n", fd->intra_heap_time);
            printf("| write 1 max intra memcpy = %lf\n", fd->intra_memcpy_time);
            printf("| write 1 max intra offset comm = %lf\n", fd->intra_wait_offset_time);
            printf("| write 1 max intra data comm = %lf\n", fd->intra_wait_data_time);
            printf("| write 1 max intra total = %lf\n", fd->total_intra_time);
            printf("|\n");
            printf("| write 2 max calculate my request = %lf\n", fd->calc_my_request_time);
            printf("| write 2 max calc other request all to all = %lf\n", fd->calc_other_request_all_to_all_time);
            printf("| write 2 max calc other request wait = %lf\n", fd->calc_other_request_wait_time);
            printf("| write 2 max calc other request total = %lf\n", fd->calc_other_request_time);
            printf("|\n");
            printf("| write 2 max inter datatype create = %lf\n", fd->inter_type_time);
            printf("| write 2 max inter heap = %lf\n", fd->inter_heap_time);
            printf("| write 2 max inter unpack = %lf\n", fd->inter_unpack_time);
            printf("| write 2 max inter data sieving = %lf\n", fd->inter_ds_time);
            printf("| write 2 max inter waitall = %lf\n", fd->inter_wait_time);
            printf("| write 2 max inter total = %lf\n", fd->total_inter_time);
            printf("|\n");
            printf("| write 3 max I/O time = %lf\n", fd->io_time);
            printf("|\n");
            printf("| write max other time = %lf\n", fd->total_time - fd->io_time - fd->total_intra_time - fd->total_inter_time - fd->calc_my_request_time - fd->calc_other_request_time - fd->calc_offset_time);
            printf("|\n");
            printf("| write 1 max first phase time = %lf\n", fd->calc_offset_time + fd->total_intra_time);
            printf("| write 2 max second phase time = %lf\n", fd->total_inter_time + fd->calc_my_request_time + fd->calc_other_request_time);
            printf("| write 3 max third phase time = %lf\n", fd->io_time);
            printf("| write max total time = %lf\n", fd->total_time);
            printf("+--------------------------------------------------------------------------+\n");
        }else {
            printf("rank 0 read timing+--------------------------------------------------------------------------+\n");
            printf("| read collective write cycle breakdown for ntimes = %d\n", fd->read_ntimes);
            printf("|\n");
            printf("| read collective local requests = %llu\n",fd->local_request_count);
            printf("| read collective max local requests = %llu, min local requests = %llu, total_request=%llu\n",request_sum_ptr[1][0],request_sum_ptr[2][0],request_sum_ptr[0][0]);
            printf("| read collective max gathered requests = %llu, min gathered requests = %llu\n",request_sum_ptr[1][5],request_sum_ptr[2][5]);
            printf("| read collective max meta send count = %llu, min meta send count = %llu, total meta send count=%llu\n",request_sum_ptr[1][1],request_sum_ptr[2][1],request_sum_ptr[0][1]);
            printf("| read collective max meta recv count = %llu, min meta recv count = %llu, total meta recv count=%llu\n",request_sum_ptr[1][2],request_sum_ptr[2][2],request_sum_ptr[0][2]);
            printf("| read collective max data send count = %llu, min data send count = %llu, total data send count=%llu\n",request_sum_ptr[1][5],request_sum_ptr[2][5],request_sum_ptr[0][5]);
            printf("| read collective max data recv count = %llu, min data recv count = %llu, total data recv count=%llu\n",request_sum_ptr[1][6],request_sum_ptr[2][6],request_sum_ptr[0][6]);
            printf("|\n");
            printf("|\n");
            printf("| read 1 max exchange_write time = %lf\n", fd->read_exchange_write_time);
            printf("|\n");
            printf("| read 1 max calculate file offset time = %lf\n", fd->read_calc_offset_time);
            printf("|\n");
            printf("| read 1 max intra datatype create = %lf\n", fd->read_intra_type_time);
            printf("| read 1 max intra heap = %lf\n", fd->read_intra_heap_time);
            printf("| read 1 max intra memcpy = %lf\n", fd->read_intra_memcpy_time);
            printf("| read 1 max intra offset comm = %lf\n", fd->read_intra_wait_offset_time);
            printf("| read 1 max intra data comm = %lf\n", fd->read_intra_wait_data_time);
            printf("| read 1 max intra total = %lf\n", fd->read_total_intra_time);
            printf("|\n");
            printf("| read 2 max calculate my request = %lf\n", fd->read_calc_my_request_time);
            printf("| read 2 max calc other request = %lf\n", fd->read_calc_other_request_time);
            printf("| read 2 max calc other request all-to-all = %lf\n", fd->calc_other_request_all_to_all_time);
            printf("| read 2 max calc other request post send= %lf\n", fd->calc_other_request_post_send_time);
            printf("| read 2 max calc other request waitall = %lf\n", fd->calc_other_request_wait_time);
            printf("| read 2 max calc file domain = %lf\n", fd->read_calc_file_domain_time);
            printf("|\n");
            printf("| read 2 max inter datatype create = %lf\n", fd->read_inter_type_time);
            printf("| read 2 max inter waitall = %lf\n", fd->read_inter_wait_time);
            printf("| read 2 max inter total = %lf\n", fd->read_total_inter_time);
            printf("|\n");
            printf("| read 3 max I/O time = %lf\n", fd->read_io_time);
            printf("|\n");
            printf("| read max other time = %lf\n", fd->read_total_time - fd->read_io_time - fd->read_total_intra_time - fd->read_total_inter_time - fd->read_calc_my_request_time - fd->read_calc_other_request_time - fd->read_calc_offset_time - fd->read_calc_file_domain_time);
            printf("|\n");
            printf("| read 1 max first phase time = %lf\n", fd->read_calc_offset_time + fd->read_total_intra_time);
            printf("| read 2 max second phase time = %lf\n", fd->read_total_inter_time + fd->read_calc_my_request_time + fd->read_calc_other_request_time + fd->read_calc_file_domain_time);
            printf("| read 3 max third phase time = %lf\n", fd->read_io_time);
            printf("| read max total time = %lf\n", fd->read_total_time);
            printf("+--------------------------------------------------------------------------+\n");
        }
        write_timings(fd, myrank, request_sum_ptr, request_sum2_ptr,is_write);
    }
    ADIOI_Free(request_sum_ptr[0]);
    ADIOI_Free(request_sum_ptr[1]);
    ADIOI_Free(request_sum_ptr[2]);
    ADIOI_Free(request_sum2_ptr[0]);
    ADIOI_Free(request_sum2_ptr[1]);
    ADIOI_Free(request_sum2_ptr[2]);
    ADIOI_Free(request_count_ptr[0]);
    ADIOI_Free(request_count_ptr[1]);
    ADIOI_Free(request_count_ptr[2]);
    ADIOI_Free(request_sum_ptr);
    ADIOI_Free(request_sum2_ptr);
    ADIOI_Free(request_count_ptr);

    return 0;
}


void ADIO_Close(ADIO_File fd, int *error_code)
{
    int i, j, k, combiner, myrank, err;
    static char myname[] = "ADIO_CLOSE";

    ADIOI_Free(fd->global_aggregators);

    if (fd->async_count) {
        *error_code = MPIO_Err_create_code(MPI_SUCCESS, MPIR_ERR_RECOVERABLE,
                                           myname, __LINE__, MPI_ERR_IO, "**io",
                                           "**io %s", strerror(errno));
        return;
    }

    /* because of deferred open, this warants a bit of explaining.  First, if
     * we've done aggregation,
     * then close the file.  Then, if any process left has done independent
     * i/o, close the file.  Otherwise, we'll skip the fs-specific close and
     * just say everything is a-ok.
     *
     * XXX: is it ok for those processes with a "real" communicator and those
     * with "MPI_COMM_SELF" to both call ADIOI_xxx_Close at the same time ?
     * everyone who ever opened the file will close it. Is order important? Is
     * timing important?
     */
    if (fd->hints->deferred_open && fd->is_agg) {
        (*(fd->fns->ADIOI_xxx_Close)) (fd, error_code);
    } else {
        if (fd->is_open) {
            (*(fd->fns->ADIOI_xxx_Close)) (fd, error_code);
        } else {
            *error_code = MPI_SUCCESS;
        }

    }

    if (fd->access_mode & ADIO_DELETE_ON_CLOSE) {
        /* if we are doing aggregation and deferred open, then it's possible
         * that rank 0 does not have access to the file. make sure only an
         * aggregator deletes the file.*/
        MPI_Comm_rank(fd->comm, &myrank);
        if (myrank == fd->hints->ranklist[0]) {
            ADIO_Delete(fd->filename, &err);
        }
        MPI_Barrier(fd->comm);
    }

    if (fd->fortran_handle != -1) {
        ADIOI_Ftable[fd->fortran_handle] = ADIO_FILE_NULL;
    }

    if (fd->hints)
        ADIOI_Free(fd->hints->ranklist);
    if (fd->hints && fd->hints->cb_config_list)
        ADIOI_Free(fd->hints->cb_config_list);

    /* This BlueGene platform-specific free must be done in the common code
     * because the malloc's for these hint data structures are done at the
     * scope of ADIO_Open within the SetInfo call (ADIOI_GPFS_SetInfo which
     * calls ADIOI_BG_gen_agg_ranklist).  They cannot be done in the
     * ADIOI_GPFS_Close because of the file creation case where the
     * ADIOI_GPFS_Close and re-open via ADIOI_GPFS_Open are done which results
     * in a double-free - ADIOI_GPFS_Open does not redo the SetInfo...  */
#ifdef BGQPLATFORM
    if (fd->hints && fd->hints->fs_hints.bg.bridgelist)
        ADIOI_Free(fd->hints->fs_hints.bg.bridgelist);
    if (fd->hints && fd->hints->fs_hints.bg.bridgelistnum)
        ADIOI_Free(fd->hints->fs_hints.bg.bridgelistnum);
#endif

    /* Persistent File Realms */
    if (fd->hints->cb_pfr == ADIOI_HINT_ENABLE) {
        /* AAR, FSIZE, and User provided uniform File realms */
        if (1) {
            MPI_Type_free(&fd->file_realm_types[0]);
        } else {
            for (i = 0; i < fd->hints->cb_nodes; i++) {
                MPI_Type_free(&fd->file_realm_types[i]);
            }
        }
        ADIOI_Free(fd->file_realm_st_offs);
        ADIOI_Free(fd->file_realm_types);
    }
    ADIOI_Free(fd->hints);



    MPI_Comm_free(&(fd->comm));
    ADIOI_Free(fd->filename);

    MPI_Type_get_envelope(fd->etype, &i, &j, &k, &combiner);
    if (combiner != MPI_COMBINER_NAMED)
        MPI_Type_free(&(fd->etype));

    MPI_Type_get_envelope(fd->filetype, &i, &j, &k, &combiner);
    if (combiner != MPI_COMBINER_NAMED)
        MPI_Type_free(&(fd->filetype));

    MPI_Info_free(&(fd->info));

    ADIOI_Free(fd->io_buf);
    ADIOI_OneSidedCleanup(fd);

    /* memory for fd is freed in MPI_File_close */
}
