/*******************************************************************************
 * @file:   partitioning.cc
 * @author: Daniel Seemaier
 * @date:   27.10.2021
 * @brief:  Interface for partitioning schemes.
 ******************************************************************************/
#include "dkaminpar/partitioning/partitioning.h"

#include "dkaminpar/context.h"
#include "dkaminpar/partitioning/deep_partitioning_scheme.h"
#include "dkaminpar/partitioning/deeper_partitioning_scheme.h"
#include "dkaminpar/partitioning/kway_partitioning_scheme.h"

namespace kaminpar::dist {
DistributedPartitionedGraph partition(const DistributedGraph& graph, const Context& ctx) {
    switch (ctx.partition.mode) {
        case PartitioningMode::KWAY:
            return KWayPartitioningScheme(graph, ctx).partition();

        case PartitioningMode::DEEP:
            return DeepPartitioningScheme(graph, ctx).partition();

        case PartitioningMode::DEEPER:
            return DeeperPartitioningScheme(graph, ctx).partition();
    }

    __builtin_unreachable();
}
} // namespace kaminpar::dist
