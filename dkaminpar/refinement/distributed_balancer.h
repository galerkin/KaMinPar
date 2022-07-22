/*******************************************************************************
 * @file:   distributed_balancer.h
 *
 * @author: Daniel Seemaier
 * @date:   12.04.2022
 * @brief:  Distributed balancing refinement algorithm.
 ******************************************************************************/
#pragma once

#include "common/datastructures/binary_heap.h"
#include "common/datastructures/marker.h"
#include "definitions.h"
#include "dkaminpar/context.h"
#include "dkaminpar/datastructure/distributed_graph.h"
#include "kaminpar/datastructure/rating_map.h"

namespace dkaminpar {
class DistributedBalancer {
    SET_STATISTICS_FROM_GLOBAL();
    SET_DEBUG(false);
    constexpr static std::size_t kPrintStatsEveryNRounds = 100'000;

    struct Statistics {
        bool         initial_feasible              = false;
        bool         final_feasible                = false;
        BlockID      initial_num_imbalanced_blocks = 0;
        BlockID      final_num_imbalanced_blocks   = 0;
        double       initial_imbalance             = 0;
        double       final_imbalance               = 0;
        BlockWeight  initial_total_overload        = 0;
        BlockWeight  final_total_overload          = 0;
        GlobalNodeID num_adjacent_moves            = 0;
        GlobalNodeID num_nonadjacent_moves         = 0;
        GlobalNodeID local_num_conflicts           = 0;
        GlobalNodeID local_num_nonconflicts        = 0;
        int          num_reduction_rounds          = 0;

        GlobalEdgeWeight initial_cut = 0;
        GlobalEdgeWeight final_cut   = 0;
    };

public:
    DistributedBalancer(const Context& ctx);

    DistributedBalancer(const DistributedBalancer&)            = delete;
    DistributedBalancer& operator=(const DistributedBalancer&) = delete;

    DistributedBalancer(DistributedBalancer&&) noexcept   = default;
    DistributedBalancer& operator=(DistributedBalancer&&) = delete;

    void initialize(const DistributedPartitionedGraph& p_graph);
    void balance(DistributedPartitionedGraph& p_graph, const PartitionContext& p_ctx);

private:
    struct MoveCandidate {
        GlobalNodeID node;
        BlockID      from;
        BlockID      to;
        NodeWeight   weight;
        double       rel_gain;
    };

    std::vector<MoveCandidate> pick_move_candidates();
    std::vector<MoveCandidate> reduce_move_candidates(std::vector<MoveCandidate>&& candidates);
    std::vector<MoveCandidate> reduce_move_candidates(std::vector<MoveCandidate>&& a, std::vector<MoveCandidate>&& b);
    void                       perform_moves(const std::vector<MoveCandidate>& moves);
    void                       perform_move(const MoveCandidate& move);

    void print_candidates(const std::vector<MoveCandidate>& moves, const std::string& desc = "") const;
    void print_overloads() const;

    void                       init_pq();
    std::pair<BlockID, double> compute_gain(NodeID u, BlockID u_block) const;

    BlockWeight block_overload(BlockID b) const;
    double      compute_relative_gain(EdgeWeight absolute_gain, NodeWeight weight) const;

    bool add_to_pq(BlockID b, NodeID u);
    bool add_to_pq(BlockID b, NodeID u, NodeWeight u_weight, double rel_gain);

    void reset_statistics();
    void print_statistics() const;

    const Context& _ctx;

    DistributedPartitionedGraph* _p_graph;
    const PartitionContext*      _p_ctx;

    shm::DynamicBinaryMinMaxForest<NodeID, double>                      _pq;
    mutable tbb::enumerable_thread_specific<shm::RatingMap<EdgeWeight>> _rating_map{[&] {
        return shm::RatingMap<EdgeWeight>{_ctx.partition.k};
    }};
    std::vector<BlockWeight>                                            _pq_weight;
    shm::Marker<>                                                       _marker;

    Statistics _stats;
};
}; // namespace dkaminpar
