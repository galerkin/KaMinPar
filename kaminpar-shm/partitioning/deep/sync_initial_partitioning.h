/*******************************************************************************
 * Implementation of the n / 2C < P phase of deep multilevel graph partitioning
 * scheduling the PE groups synchronously.
 *
 * @file:   sync_initial_partitioning.h
 * @author: Daniel Seemaier
 * @date:   21.09.2021
 ******************************************************************************/
#pragma once

#include <tbb/enumerable_thread_specific.h>
#include <tbb/parallel_invoke.h>
#include <tbb/task_arena.h>
#include <tbb/task_group.h>
#include <tbb/task_scheduler_observer.h>

#include "kaminpar-shm/partitioning/helper.h"

namespace kaminpar::shm::partitioning {
class SyncInitialPartitioner {
public:
  SyncInitialPartitioner(
      const Context &input_ctx,
      InitialBipartitionerPoolEts &bipartitioner_pool_ets,
      TemporarySubgraphMemoryEts &tmp_extraction_mem_pool_ets
  );

  PartitionedGraph partition(const Coarsener *coarsener, const PartitionContext &p_ctx);

private:
  std::unique_ptr<Coarsener> duplicate_coarsener(const Coarsener *coarsener);

  const Context &_input_ctx;
  InitialBipartitionerPoolEts &_bipartitioner_pool_ets;
  TemporarySubgraphMemoryEts &_tmp_extraction_mem_pool_ets;
};
} // namespace kaminpar::shm::partitioning
