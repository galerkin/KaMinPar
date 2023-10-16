/*******************************************************************************
 * Distributed JET refiner due to: "Jet: Multilevel Graph Partitioning on GPUs"
 * by Gilbert et al.
 *
 * @file:   jet_refiner.cc
 * @author: Daniel Seemaier
 * @date:   02.05.2023
 ******************************************************************************/
#include "kaminpar-dist/refinement/jet/jet_refiner.h"

#include <tbb/parallel_invoke.h>

#include "kaminpar-dist/context.h"
#include "kaminpar-dist/datastructures/distributed_graph.h"
#include "kaminpar-dist/datastructures/distributed_partitioned_graph.h"
#include "kaminpar-dist/factories.h"
#include "kaminpar-dist/graphutils/synchronization.h"
#include "kaminpar-dist/metrics.h"
#include "kaminpar-dist/refinement/gain_calculator.h"
#include "kaminpar-dist/refinement/snapshooter.h"
#include "kaminpar-dist/timer.h"

#include "kaminpar-common/random.h"

#define HEAVY assert::heavy

namespace kaminpar::dist {
JetRefinerFactory::JetRefinerFactory(const Context &ctx) : _ctx(ctx) {}

std::unique_ptr<GlobalRefiner>
JetRefinerFactory::create(DistributedPartitionedGraph &p_graph, const PartitionContext &p_ctx) {
  return std::make_unique<JetRefiner>(_ctx, p_graph, p_ctx);
}

JetRefiner::JetRefiner(
    const Context &ctx, DistributedPartitionedGraph &p_graph, const PartitionContext &p_ctx
)
    : _ctx(ctx),
      _jet_ctx(ctx.refinement.jet),
      _p_graph(p_graph),
      _p_ctx(p_ctx),
      _snapshooter(ctx.partition.graph->total_n, ctx.partition.k),
      _gain_calculator(ctx.partition.k),
      _gains_and_targets(ctx.partition.graph->total_n),
      _block_weight_deltas(ctx.partition.k),
      _locked(p_ctx.graph->n),
      _balancer(factory::create_refiner(_ctx, _ctx.refinement.jet.balancing_algorithm)
                    ->create(_p_graph, _p_ctx)) {}

void JetRefiner::initialize() {
  SCOPED_TIMER("Jet initialization");

  _snapshooter.init(_p_graph, _p_ctx);
  _gain_calculator.init(_p_graph);

  tbb::parallel_invoke(
      [&] { _p_graph.pfor_nodes([&](const NodeID u) { _locked[u] = 0; }); },
      [&] {
        _p_graph.pfor_all_nodes([&](const NodeID u) {
          _gains_and_targets[u] = {0, _p_graph.block(u)};
        });
      },
      [&] { _p_graph.pfor_blocks([&](const BlockID b) { _block_weight_deltas[b] = 0; }); }
  );

  _penalty_factor = (_p_graph.n() <= 2 * _p_ctx.k * _ctx.coarsening.contraction_limit)
                        ? _jet_ctx.coarse_penalty_factor
                        : _jet_ctx.fine_penalty_factor;

  TIMER_BARRIER(_p_graph.communicator());
}

bool JetRefiner::refine() {
  TIMER_BARRIER(_p_graph.communicator());
  SCOPED_TIMER("Jet Refinement");

  KASSERT(
      [&] {
        for (const NodeID u : _p_graph.nodes()) {
          if (_locked[u]) {
            LOG_WARNING << "node " << u << " already locked: refiner was not properly initialized";
            return false;
          }
        }

        for (const BlockID block : _p_graph.blocks()) {
          if (_block_weight_deltas[block] != 0) {
            LOG_WARNING << "block " << block << " has nonzero initial block weight delta";
            return false;
          }
        }
        return true;
      }(),
      "refiner was not properly initialized",
      HEAVY
  );

  const int max_num_fruitless_iterations = (_ctx.refinement.jet.num_fruitless_iterations == 0)
                                               ? std::numeric_limits<int>::max()
                                               : _ctx.refinement.jet.num_fruitless_iterations;
  const int max_num_iterations = (_ctx.refinement.jet.num_iterations == 0)
                                     ? std::numeric_limits<int>::max()
                                     : _ctx.refinement.jet.num_iterations;
  int cur_fruitless_iteration = 0;
  int cur_iteration = 0;

  const EdgeWeight initial_cut = metrics::edge_cut(_p_graph);
  EdgeWeight best_cut = initial_cut;

  do {
    TIMER_BARRIER(_p_graph.communicator());

    find_moves();
    synchronize_ghost_node_move_candidates();
    filter_bad_moves();
    move_locked_nodes();
    synchronize_ghost_node_labels();
    apply_block_weight_deltas();

    KASSERT(
        graph::debug::validate_partition(_p_graph),
        "graph partition is in an inconsistent state after JET iterations " << cur_iteration,
        HEAVY
    );

    TIMED_SCOPE("Rebalance") {
      _balancer->initialize();
      _balancer->refine();
    };

    TIMED_SCOPE("Update best partition") {
      _snapshooter.update(_p_graph, _p_ctx);
    };

    ++cur_iteration;
    ++cur_fruitless_iteration;

    const EdgeWeight final_cut = metrics::edge_cut(_p_graph);
    if (best_cut - final_cut > (1.0 - _ctx.refinement.jet.fruitless_threshold) * best_cut) {
      DBG0 << "Improved cut from " << initial_cut << " to " << best_cut << " to " << final_cut
           << ": resetting number of fruitless iterations (threshold: "
           << _ctx.refinement.jet.fruitless_threshold << ")";
      best_cut = final_cut;
      cur_fruitless_iteration = 0;
    } else {
      DBG0 << "Fruitless edge cut change from " << initial_cut << " to " << best_cut << " to "
           << final_cut << " (threshold: " << _ctx.refinement.jet.fruitless_threshold
           << "): incrementing fruitless iterations counter to " << cur_fruitless_iteration;
    }
  } while (cur_iteration < max_num_iterations &&
           cur_fruitless_iteration < max_num_fruitless_iterations);

  TIMED_SCOPE("Rollback") {
    _snapshooter.rollback(_p_graph);
  };

  KASSERT(
      graph::debug::validate_partition(_p_graph),
      "graph partition is in an inconsistent state after JET refinement",
      HEAVY
  );

  TIMER_BARRIER(_p_graph.communicator());
  return initial_cut > best_cut;
}

void JetRefiner::find_moves() {
  SCOPED_TIMER("Find moves");

  _p_graph.pfor_nodes([&](const NodeID u) {
    const BlockID b_u = _p_graph.block(u);
    const NodeWeight w_u = _p_graph.node_weight(u);

    if (_locked[u]) {
      _gains_and_targets[u] = {0, b_u};
      return;
    }

    const auto max_gainer = _gain_calculator.compute_max_gainer(u);

    if (max_gainer.block != b_u &&
        (max_gainer.ext_degree > max_gainer.int_degree ||
         max_gainer.absolute_gain() >= -std::floor(_penalty_factor * max_gainer.int_degree))) {
      _gains_and_targets[u] = {max_gainer.absolute_gain(), max_gainer.block};
    } else {
      _gains_and_targets[u] = {0, b_u};
    }
  });

  TIMER_BARRIER(_p_graph.communicator());
}

void JetRefiner::synchronize_ghost_node_move_candidates() {
  SCOPED_TIMER("Exchange moves");

  tbb::parallel_for<NodeID>(_p_graph.n(), _p_graph.total_n(), [&](const NodeID ghost) {
    _gains_and_targets[ghost] = {0, _p_graph.block(ghost)};
  });

  struct Message {
    NodeID node;
    EdgeWeight gain;
    BlockID target;
  };

  mpi::graph::sparse_alltoall_interface_to_pe<Message>(
      _p_graph.graph(),
      [&](const NodeID u) { return _gains_and_targets[u].second != _p_graph.block(u); },
      [&](const NodeID u) -> Message {
        return {
            .node = u,
            .gain = _gains_and_targets[u].first,
            .target = _gains_and_targets[u].second,
        };
      },
      [&](const auto &recv_buffer, const PEID pe) {
        tbb::parallel_for<std::size_t>(0, recv_buffer.size(), [&](const std::size_t i) {
          const auto [their_lnode, gain, target] = recv_buffer[i];
          const NodeID lnode = _p_graph.map_foreign_node(their_lnode, pe);
          _gains_and_targets[lnode] = {gain, target};
        });
      }
  );

  TIMER_BARRIER(_p_graph.communicator());
}

void JetRefiner::filter_bad_moves() {
  SCOPED_TIMER("Filter moves");

  _p_graph.pfor_nodes([&](const NodeID u) {
    _locked[u] = 0;

    const BlockID from_u = _p_graph.block(u);
    const auto [gain_u, to_u] = _gains_and_targets[u];

    if (from_u == to_u) {
      return;
    }

    EdgeWeight projected_gain = 0;

    for (const auto &[e, v] : _p_graph.neighbors(u)) {
      const EdgeWeight w_e = _p_graph.edge_weight(e);

      const auto [gain_v, to_v] = _gains_and_targets[v];
      const BlockID projected_b_v =
          (gain_v > gain_u || (gain_v == gain_u && v < u)) ? to_v : _p_graph.block(v);

      if (projected_b_v == to_u) {
        projected_gain += w_e;
      } else if (projected_b_v == from_u) {
        projected_gain -= w_e;
      }
    }

    // Locking the node here means that the move
    // will be executed by move_locked_nodes()
    if (projected_gain >= 0) {
      _locked[u] = 1;
    }
  });

  TIMER_BARRIER(_p_graph.communicator());
}

void JetRefiner::move_locked_nodes() {
  SCOPED_TIMER("Execute moves");

  _p_graph.pfor_nodes([&](const NodeID u) {
    if (_locked[u]) {
      const BlockID from = _p_graph.block(u);
      const BlockID to = _gains_and_targets[u].second;
      _p_graph.set_block<false>(u, to);

      const NodeWeight w_u = _p_graph.node_weight(u);
      __atomic_fetch_sub(&_block_weight_deltas[from], w_u, __ATOMIC_RELAXED);
      __atomic_fetch_add(&_block_weight_deltas[to], w_u, __ATOMIC_RELAXED);
    }
  });

  TIMER_BARRIER(_p_graph.communicator());
}

void JetRefiner::synchronize_ghost_node_labels() {
  SCOPED_TIMER("Synchronize ghost node labels");

  struct Message {
    NodeID node;
    BlockID block;
  };

  mpi::graph::sparse_alltoall_interface_to_pe<Message>(
      _p_graph.graph(),

      // Only exchange messages for nodes that were moved during the last round
      [&](const NodeID u) { return _locked[u]; },

      [&](const NodeID u) -> Message { return {.node = u, .block = _p_graph.block(u)}; },
      [&](const auto &recv_buffer, const PEID pe) {
        tbb::parallel_for<std::size_t>(0, recv_buffer.size(), [&](const std::size_t i) {
          const auto [their_lnode, block] = recv_buffer[i];
          const NodeID lnode = _p_graph.map_foreign_node(their_lnode, pe);
          _p_graph.set_block<false>(lnode, block);
        });
      }
  );

  TIMER_BARRIER(_p_graph.communicator());
}

void JetRefiner::apply_block_weight_deltas() {
  SCOPED_TIMER("Apply block weight deltas");

  MPI_Allreduce(
      MPI_IN_PLACE,
      _block_weight_deltas.data(),
      _p_graph.k(),
      mpi::type::get<NodeWeight>(),
      MPI_SUM,
      _p_graph.communicator()
  );

  _p_graph.pfor_blocks([&](const BlockID b) {
    _p_graph.set_block_weight(b, _p_graph.block_weight(b) + _block_weight_deltas[b]);
    _block_weight_deltas[b] = 0;
  });

  TIMER_BARRIER(_p_graph.communicator());
}
} // namespace kaminpar::dist
