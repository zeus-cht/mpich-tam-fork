/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil ; -*- */
/*
 *
 *  (C) 2003 by Argonne National Laboratory.
 *      See COPYRIGHT in top-level directory.
 */

#ifndef MPIU_GREQ_H_INCLUDED
#define MPIU_GREQ_H_INCLUDED

#if !defined(ROMIO_INSIDE_MPICH) && !defined(HAVE_MPIX_GREQUEST_CLASS)
typedef int MPIX_Grequest_class;
#endif

int MPIU_Greq_query_fn(void *extra_state, MPI_Status * status);
int MPIU_Greq_free_fn(void *extra_state);
int MPIU_Greq_cancel_fn(void *extra_state, int complete);

#endif /* MPIU_GREQ_H_INCLUDED */
