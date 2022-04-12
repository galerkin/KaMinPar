/*******************************************************************************
 * @file:   distributed_balancer.h
 *
 * @author: Daniel Seemaier
 * @date:   12.04.2022
 * @brief:  Distributed balancing refinement algorithm.
 ******************************************************************************/
#pragma once

#include "dkaminpar/context.h"
#include "dkaminpar/datastructure/distributed_graph.h"
#include "kaminpar/datastructure/binary_heap.h"
#include "kaminpar/datastructure/marker.h"
#include "kaminpar/datastructure/rating_map.h"

namespace dkaminpar {
class DistributedBalancer {
public:
    DistributedBalancer(const Context& ctx);

    DistributedBalancer(const DistributedBalancer&) = delete;
    DistributedBalancer& operator=(const DistributedBalancer&) = delete;

    DistributedBalancer(DistributedBalancer&&) noexcept = default;
    DistributedBalancer& operator=(DistributedBalancer&&) noexcept = default;

    void initialize(const DistributedPartitionedGraph& p_graph);
    void balance(DistributedPartitionedGraph& p_graph, const PartitionContext& p_ctx);

private:
    void                       init_pq();
    std::pair<BlockID, double> compute_gain(NodeID u, BlockID u_block) const;

    BlockWeight block_overload(BlockID b) const;
    double      compute_relative_gain(EdgeWeight absolute_gain, NodeWeight weight) const;

    bool add_to_pq(BlockID b, NodeID u);
    bool add_to_pq(BlockID b, NodeID u, NodeWeight u_weight, double rel_gain);

    const Context& _ctx;

    DistributedPartitionedGraph* _p_graph;
    const PartitionContext*      _p_ctx;

    shm::DynamicBinaryMinMaxForest<NodeID, double>                      _pq;
    mutable tbb::enumerable_thread_specific<shm::RatingMap<EdgeWeight>> _rating_map{[&] {
        return shm::RatingMap<EdgeWeight>{_ctx.partition.k};
    }};
    std::vector<BlockWeight>                                            _pq_weight;
    shm::Marker<>                                                       _marker;
};
}; // namespace dkaminpar

