/*******************************************************************************
 * Distributed JET refiner due to: "Jet: Multilevel Graph Partitioning on GPUs"
 * by Gilbert et al.
 *
 * @file:   jet_refiner.h
 * @author: Daniel Seemaier
 * @date:   02.05.2023
 ******************************************************************************/
#pragma once

#include <tbb/concurrent_vector.h>

#include "kaminpar-dist/context.h"
#include "kaminpar-dist/datastructures/distributed_graph.h"
#include "kaminpar-dist/datastructures/distributed_partitioned_graph.h"
#include "kaminpar-dist/refinement/refiner.h"

namespace kaminpar::dist {
class JetRefinerFactory : public GlobalRefinerFactory {
public:
  JetRefinerFactory(const Context &ctx);

  JetRefinerFactory(const JetRefinerFactory &) = delete;
  JetRefinerFactory &operator=(const JetRefinerFactory &) = delete;

  JetRefinerFactory(JetRefinerFactory &&) noexcept = default;
  JetRefinerFactory &operator=(JetRefinerFactory &&) = delete;

  std::unique_ptr<GlobalRefiner>
  create(DistributedPartitionedGraph &p_graph, const PartitionContext &p_ctx) final;

private:
  const Context &_ctx;
};

class JetRefiner : public GlobalRefiner {
  SET_STATISTICS_FROM_GLOBAL();
  SET_DEBUG(true);

public:
  JetRefiner(
      const Context &ctx, DistributedPartitionedGraph &p_graph, const PartitionContext &p_ctx
  );

  JetRefiner(const JetRefiner &) = delete;
  JetRefiner &operator=(const JetRefiner &) = delete;

  JetRefiner(JetRefiner &&) noexcept = default;
  JetRefiner &operator=(JetRefiner &&) = delete;

  void initialize() final;
  bool refine() final;

private:
  void reset_locks();

  double compute_penalty_factor() const;

  const Context &_ctx;
  const JetRefinementContext &_jet_ctx;
  DistributedPartitionedGraph &_p_graph;
  const PartitionContext &_p_ctx;

  std::unique_ptr<GlobalRefiner> _balancer;

  NoinitVector<std::uint8_t> _locks;
};
} // namespace kaminpar::dist
