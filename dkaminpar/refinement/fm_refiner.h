/*******************************************************************************
 * @file:   fm_refiner.h
 * @author: Daniel Seemaier
 * @date:   02.08.2022
 * @brief:  Distributed FM refiner.
 ******************************************************************************/
#pragma once

#include <oneapi/tbb/concurrent_vector.h>
#include <tbb/concurrent_vector.h>

#include "logger.h"

#include "dkaminpar/context.h"
#include "dkaminpar/datastructure/distributed_graph.h"
#include "dkaminpar/refinement/i_distributed_refiner.h"

#include "common/parallel/atomic.h"

namespace kaminpar::dist {
class FMRefiner : public IDistributedRefiner {
    SET_STATISTICS(true);

    struct Statistics {
        // Sizes of search graphs
        tbb::concurrent_vector<NodeID> graphs_n{};
        tbb::concurrent_vector<EdgeID> graphs_m{};
        tbb::concurrent_vector<NodeID> graphs_border_n{};

        // Number of move conflicts when applying moves from search graphs to the global partition
        parallel::Atomic<NodeID> num_conflicts{0};

        // Improvement statistics
        parallel::Atomic<NodeID> num_searches_with_improvement{0};
        EdgeWeight               initial_cut{kInvalidEdgeWeight};
        EdgeWeight               final_cut{kInvalidEdgeWeight};

        void print() const;
    };

public:
    FMRefiner(const Context& ctx);

    FMRefiner(const FMRefiner&)            = delete;
    FMRefiner(FMRefiner&&)                 = default;
    FMRefiner& operator=(const FMRefiner&) = delete;
    FMRefiner& operator=(FMRefiner&&)      = delete;

    void initialize(const DistributedGraph& graph, const PartitionContext& p_ctx);
    void refine(DistributedPartitionedGraph& p_graph);

private:
    void                           refinement_round();
    tbb::concurrent_vector<NodeID> find_seed_nodes();

    void init_external_degrees();

    EdgeWeight& external_degree(const NodeID u, const BlockID b) {
        KASSERT(_external_degrees.size() >= _p_graph->n() * _p_graph->k());
        return _external_degrees[u * _p_graph->k() + b];
    }

    // initialized by ctor
    const FMRefinementContext& _fm_ctx;

    // initalized by refine()
    const PartitionContext*      _p_ctx;
    DistributedPartitionedGraph* _p_graph;
    std::vector<EdgeWeight>      _external_degrees;

    // initialized here
    std::size_t                                 _round{0};
    std::vector<parallel::Atomic<std::uint8_t>> _locked;

    Statistics _stats;
};
} // namespace kaminpar::dist
