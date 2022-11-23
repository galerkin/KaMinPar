/*******************************************************************************
 * @file:   graph_rearrangement.h
 * @author: Daniel Seemaier
 * @date:   18.11.2021
 * @brief:  Sort and rearrange a graph by degree buckets.
 ******************************************************************************/
#pragma once

#include "dkaminpar/context.h"
#include "dkaminpar/datastructures/distributed_graph.h"

#include "common/scalable_vector.h"

namespace kaminpar::dist::graph {
DistributedGraph rearrange_by_degree_buckets(DistributedGraph graph);
DistributedGraph rearrange_by_coloring(DistributedGraph graph, const Context& ctx);
DistributedGraph rearrange_by_permutation(
    DistributedGraph graph, scalable_vector<NodeID> old_to_new, scalable_vector<NodeID> new_to_old
);
} // namespace kaminpar::dist::graph
