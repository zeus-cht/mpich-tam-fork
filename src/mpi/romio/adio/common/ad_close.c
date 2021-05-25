/*
 * Copyright (C) by Argonne National Laboratory
 *     See COPYRIGHT in top-level directory
 */

#include "adio.h"
#include "adio_extern.h"
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif

#if TIME_PROFILING==1
int write_logs(ADIO_File fd, int myrank){
    if ( fd->n_coll_write == 0 ) {
        return 0;
    }
    int nprocs;
    MPI_Comm_size(fd->comm, &nprocs);
    char filename[1024];
    double write_two_phase_max, write_total_max;
    int total_recv_comms;

    MPI_Reduce(&(fd->total_recv_op), &total_recv_comms, 1, MPI_INT, MPI_MAX, 0, fd->comm);
    MPI_Reduce(&(fd->write_two_phase), &write_two_phase_max, 1, MPI_DOUBLE, MPI_MAX, 0, fd->comm);
    MPI_Reduce(&(fd->total_write_time), &write_total_max, 1, MPI_DOUBLE, MPI_MAX, 0, fd->comm);
    if (myrank) {
        return 0;
    }
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
        fprintf(stream,"# of collective write,");
        fprintf(stream,"# of write collective loops,");
        fprintf(stream,"# of write comms,");

        fprintf(stream,"calculate offsets,");
        fprintf(stream,"calc my request,");
        fprintf(stream,"calc other request,");
        fprintf(stream,"write exchange,");

        fprintf(stream,"intra wait meta,");
        fprintf(stream,"intra wait data,");
        fprintf(stream,"total intra time,");

        fprintf(stream,"inter metadata,");
        fprintf(stream,"inter heap merge,");
        fprintf(stream,"inter unpack,");
        fprintf(stream,"inter ds,");
        fprintf(stream,"inter wait,");
        fprintf(stream,"total inter time,");

        fprintf(stream,"I/O,");
        fprintf(stream,"write_two_phase,");
        fprintf(stream,"write_two_phase_max,");
        fprintf(stream,"write total,");
        fprintf(stream,"write total max\n");
    }
    fprintf(stream,"%d,", nprocs);
    fprintf(stream,"%d,", fd->hints->cb_nodes);
    fprintf(stream,"%d,", fd->local_aggregator_size);
    fprintf(stream,"%d,", fd->n_coll_write);
    fprintf(stream,"%d,", fd->ntimes);
    fprintf(stream,"%d,", total_recv_comms);

    fprintf(stream,"%lf,", fd->calc_offset_time);
    fprintf(stream,"%lf,", fd->calc_my_request_time);
    fprintf(stream,"%lf,", fd->calc_other_request_time);
    fprintf(stream,"%lf,", fd->exchange_write);

    fprintf(stream,"%lf,", fd->intra_wait_offset_time);
    fprintf(stream,"%lf,", fd->intra_wait_data_time);
    fprintf(stream,"%lf,", fd->total_intra_time);

    fprintf(stream,"%lf,", fd->metadata_exchange_time);
    fprintf(stream,"%lf,", fd->inter_heap_time);
    fprintf(stream,"%lf,", fd->inter_unpack_time);
    fprintf(stream,"%lf,", fd->inter_ds_time);
    fprintf(stream,"%lf,", fd->inter_wait_time);
    fprintf(stream,"%lf,", fd->total_inter_time);

    fprintf(stream,"%lf,", fd->io_time);
    fprintf(stream,"%lf,", fd->write_two_phase);
    fprintf(stream,"%lf,", write_two_phase_max);
    fprintf(stream,"%lf,", fd->total_write_time);
    fprintf(stream,"%lf\n", write_total_max);


    fclose(stream);
    return 0;
}

int read_logs(ADIO_File fd, int myrank){
    if ( fd->n_coll_read == 0 ) {
        return 0;
    }
    int nprocs;
    MPI_Comm_size(fd->comm, &nprocs);
    char filename[1024];
    double read_two_phase_max, read_total_max;
    int total_recv_comms;

    MPI_Reduce(&(fd->read_two_phase), &read_two_phase_max, 1, MPI_DOUBLE, MPI_MAX, 0, fd->comm);
    MPI_Reduce(&(fd->total_read_time), &read_total_max, 1, MPI_DOUBLE, MPI_MAX, 0, fd->comm);
    MPI_Reduce(&(fd->total_read_recv_op), &total_recv_comms, 1, MPI_INT, MPI_MAX, 0, fd->comm);
    if (myrank) {
        return 0;
    }
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
        fprintf(stream,"# of collective read,");
        fprintf(stream,"# of read collective loops,");
        fprintf(stream,"# of read comms,");

        fprintf(stream,"calculate offsets,");
        fprintf(stream,"calc my request,");
        fprintf(stream,"calc other request,");
        fprintf(stream,"read exchange,");

        fprintf(stream,"intra wait meta,");
        fprintf(stream,"intra wait data,");
        fprintf(stream,"total intra time,");

        fprintf(stream,"inter metadata,");
        fprintf(stream,"inter unpack,");
        fprintf(stream,"inter wait,");
        fprintf(stream,"total inter time,");

        fprintf(stream,"I/O,");
        fprintf(stream,"read_two_phase,");
        fprintf(stream,"read_two_phase_max,");
        fprintf(stream,"read total,");
        fprintf(stream,"read total max\n");
    }
    fprintf(stream,"%d,", nprocs);
    fprintf(stream,"%d,", fd->hints->cb_nodes);
    fprintf(stream,"%d,", fd->local_aggregator_size);
    fprintf(stream,"%d,", fd->n_coll_read);
    fprintf(stream,"%d,", fd->read_ntimes);
    fprintf(stream,"%d,", total_recv_comms);

    fprintf(stream,"%lf,", fd->read_calc_offset_time);
    fprintf(stream,"%lf,", fd->read_calc_my_request_time);
    fprintf(stream,"%lf,", fd->read_calc_other_request_time);
    fprintf(stream,"%lf,", fd->exchange_read);

    fprintf(stream,"%lf,", fd->read_intra_wait_offset_time);
    fprintf(stream,"%lf,", fd->read_intra_wait_data_time);
    fprintf(stream,"%lf,", fd->read_total_intra_time);

    fprintf(stream,"%lf,", fd->read_metadata_exchange_time);
    fprintf(stream,"%lf,", fd->read_inter_unpack_time);
    fprintf(stream,"%lf,", fd->read_inter_wait_time);
    fprintf(stream,"%lf,", fd->read_total_inter_time);

    fprintf(stream,"%lf,", fd->read_io_time);
    fprintf(stream,"%lf,", fd->read_two_phase);
    fprintf(stream,"%lf,", read_two_phase_max);
    fprintf(stream,"%lf,", fd->total_read_time);
    fprintf(stream,"%lf\n", read_total_max);


    fclose(stream);
    return 0;
}
#endif

void ADIO_Close(ADIO_File fd, int *error_code)
{
    int i, j, k, combiner, myrank, err;
    static char myname[] = "ADIO_CLOSE";
    MPI_Comm_rank(fd->comm, &myrank);
    #if TIME_PROFILING==1
    write_logs(fd, myrank);
    read_logs(fd, myrank);
    #endif
    /*TAM cleanup*/
    if (fd->is_agg){
        ADIOI_Free(fd->local_aggregator_domain[0]);
        ADIOI_Free(fd->local_aggregator_domain);
        ADIOI_Free(fd->global_recv_size);
    }
    if (fd->is_local_aggregator) {
        ADIOI_Free(fd->aggregator_local_ranks);
        ADIOI_Free(fd->local_send_size);
        ADIOI_Free(fd->local_lens);
        ADIOI_Free(fd->new_types);
        ADIOI_Free(fd->array_of_displacements);
        ADIOI_Free(fd->array_of_blocklengths);
    }
    ADIOI_Free(fd->cb_send_size);
    if (fd->local_buf_size) {
        ADIOI_Free(fd->local_buf);
    }
    ADIOI_Free(fd->req);
    ADIOI_Free(fd->sts);
    ADIOI_Free(fd->local_aggregators);

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
