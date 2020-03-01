#ifndef CH4_COLL_CONTAINERS_H_INCLUDED
#define CH4_COLL_CONTAINERS_H_INCLUDED

#include "ch4_coll_params.h"

/* Empty container */
extern const MPIDI_coll_algo_container_t MPIDI_empty_cnt;

/* Barrier CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Barrier_intra_composition_alpha_cnt;
extern const MPIDI_coll_algo_container_t MPIDI_Barrier_intra_composition_beta_cnt;

/* Bcast  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Bcast_intra_composition_alpha_cnt;
extern const MPIDI_coll_algo_container_t MPIDI_Bcast_intra_composition_beta_cnt;
extern const MPIDI_coll_algo_container_t MPIDI_Bcast_intra_composition_gamma_cnt;

/* Reduce  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Reduce_intra_composition_alpha_cnt;
extern const MPIDI_coll_algo_container_t MPIDI_Reduce_intra_composition_beta_cnt;
extern const MPIDI_coll_algo_container_t MPIDI_Reduce_intra_composition_gamma_cnt;

/* Allreduce  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Allreduce_intra_composition_alpha_cnt;
extern const MPIDI_coll_algo_container_t MPIDI_Allreduce_intra_composition_beta_cnt;
extern const MPIDI_coll_algo_container_t MPIDI_Allreduce_intra_composition_gamma_cnt;

/* Alltoall  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Alltoall_intra_composition_alpha_cnt;

/* Alltoallv  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Alltoallv_intra_composition_alpha_cnt;

/* Alltoallw  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Alltoallw_intra_composition_alpha_cnt;

/* Allgather  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Allgather_intra_composition_alpha_cnt;

/* Allgatherv  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Allgatherv_intra_composition_alpha_cnt;

/* Gather  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Gather_intra_composition_alpha_cnt;

/* Gatherv  CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Gatherv_intra_composition_alpha_cnt;

/* Scatter CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Scatter_intra_composition_alpha_cnt;

/* Scatterv CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Scatterv_intra_composition_alpha_cnt;

/* Reduce_scatter CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Reduce_scatter_intra_composition_alpha_cnt;

/* Reduce_scatter_block CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Reduce_scatter_block_intra_composition_alpha_cnt;

/* Scan CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Scan_intra_composition_alpha_cnt;
extern const MPIDI_coll_algo_container_t MPIDI_Scan_intra_composition_beta_cnt;

/* Exscan CH4 level containers declaration */
extern const MPIDI_coll_algo_container_t MPIDI_Exscan_intra_composition_alpha_cnt;

#endif /* CH4_COLL_CONTAINERS_H_INCLUDED */
