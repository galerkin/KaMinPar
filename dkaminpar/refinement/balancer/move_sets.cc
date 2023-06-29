#include "dkaminpar/refinement/balancer/move_sets.h"

#include <tbb/parallel_for.h>
#include <tbb/parallel_invoke.h>

#include "dkaminpar/context.h"

#include "common/assertion_levels.h"
#include "common/datastructures/binary_heap.h"
#include "common/datastructures/fast_reset_array.h"
#include "common/datastructures/marker.h"
#include "common/datastructures/rating_map.h"
#include "common/noinit_vector.h"

namespace kaminpar::dist {
MoveSets::MoveSets(
    const DistributedPartitionedGraph &p_graph,
    NoinitVector<NodeID> node_to_move_set,
    NoinitVector<NodeID> move_sets,
    NoinitVector<NodeID> move_set_indices
)
    : _p_graph(p_graph),
      _node_to_move_set(std::move(node_to_move_set)),
      _move_sets(std::move(move_sets)),
      _move_set_indices(std::move(move_set_indices)) {
  KASSERT(_move_set_indices.front() == 0u);
  KASSERT(_move_set_indices.back() == _move_sets.size());
}

NodeID MoveSets::size(const NodeID set) const {
  KASSERT(set + 1 < _move_set_indices.size());
  return _move_set_indices[set + 1] - _move_set_indices[set];
}

namespace {
class MoveSetBuilder {
public:
  MoveSetBuilder(const DistributedPartitionedGraph &p_graph, const PartitionContext &p_ctx)
      : _p_graph(p_graph),
        _p_ctx(p_ctx),
        _node_to_move_set(p_graph.n()),
        _move_sets(p_graph.n()),
        _move_set_indices(p_graph.n() + 1),
        _conns(p_graph.n() * p_graph.k()),
        _frontier(p_graph.n()),
        _cur_conns(p_graph.k()) {
    _p_graph.pfor_nodes([&](const NodeID u) {
      _node_to_move_set[u] = kInvalidNodeID;
      _move_sets[u] = kInvalidNodeID;
    });
    _move_set_indices.front() = 0;
  }

  void build(const NodeWeight max_move_set_weight) {
    for (const NodeID u : _p_graph.nodes()) {
      const BlockID bu = _p_graph.block(u);

      if (_p_graph.block_weight(bu) > _p_ctx.graph->max_block_weight(bu) &&
          _node_to_move_set[u] == kInvalidNodeID) {
        grow_move_set(u, max_move_set_weight);
      }
    }
  }

  void grow_move_set(const NodeID seed, const NodeWeight max_weight) {
    KASSERT(_node_to_move_set[seed] == kInvalidNodeID);

    _frontier.push(seed, 0);
    while (!_frontier.empty() && _cur_weight < max_weight) {
      const NodeID u = _frontier.peek_id();
      const BlockID bu = _p_graph.block(u);
      _frontier.pop();

      add_to_move_set(u);

      for (const auto [e, v] : _p_graph.neighbors(u)) {
        const BlockID bv = _p_graph.block(v);
        if (bv == bu) {
          if (_frontier.contains(v)) {
            _frontier.decrease_priority(v, _frontier.key(v) + _p_graph.edge_weight(e));
          } else {
            _frontier.push(v, _p_graph.edge_weight(e));
          }
        }
      }
    }

    finish_move_set();

    KASSERT(_node_to_move_set[seed] != kInvalidBlockID);
  }

  void add_to_move_set(const NodeID u) {
    KASSERT(_cur_block == kInvalidBlockID || _cur_block == _p_graph.block(u));

    if (_cur_block == kInvalidBlockID) {
      _cur_block = _p_graph.block(u);
    }

    _cur_weight += _p_graph.node_weight(u);
    _node_to_move_set[u] = _cur_move_set;
    _move_sets[_cur_pos] = u;
    ++_cur_pos;

    for (const auto [e, v] : _p_graph.neighbors(u)) {
      if (_p_graph.is_owned_node(v) && _node_to_move_set[v] == _cur_move_set) {
        _cur_block_conn -= _p_graph.edge_weight(e);
      } else {
        const BlockID bv = _p_graph.block(v);
        if (bv == _cur_block) {
          _cur_block_conn += _p_graph.edge_weight(e);
        } else {
          _cur_conns.decrease_priority_by(bv, _p_graph.edge_weight(e));
        }
      }
    }

    if (_cur_conns.peek_key() >= _best_prefix_conn) {
      _best_prefix_block = _cur_conns.peek_id();
      _best_prefix_conn = _cur_conns.peek_key();
      _best_prefix_pos = _cur_pos;
    }
  }

  void finish_move_set() {
    for (NodeID pos = _best_prefix_pos; pos < _cur_pos; ++pos) {
      _node_to_move_set[_move_sets[pos]] = kInvalidNodeID;
    }

    _move_set_indices[_cur_move_set + 1] = _move_set_indices[_cur_move_set] + _best_prefix_pos;
    ++_cur_move_set;

    reset_cur_conns();
    _cur_block = kInvalidBlockID;
    _cur_block_conn = 0;
    _cur_pos = _best_prefix_pos;
  }

  MoveSets finalize() {
    _move_set_indices.resize(_cur_move_set + 1);
    _move_set_indices.back() = _p_graph.n();

    return {
        _p_graph,
        std::move(_node_to_move_set),
        std::move(_move_sets),
        std::move(_move_set_indices),
    };
  }

  NodeWeight current_weight() {
    return _cur_weight;
  }

private:
  void reset_cur_conns() {
    _cur_conns.clear();
    for (const BlockID b : _p_graph.blocks()) {
      _cur_conns.push(b, 0);
    }
  }

  const DistributedPartitionedGraph &_p_graph;
  const PartitionContext &_p_ctx;

  NoinitVector<NodeID> _node_to_move_set;
  NoinitVector<NodeID> _move_sets;
  NoinitVector<NodeID> _move_set_indices;

  NoinitVector<EdgeWeight> _conns;

  BinaryMaxHeap<EdgeWeight> _frontier;

  NodeID _cur_pos = 0;
  NodeID _cur_move_set = 0;
  EdgeWeight _cur_block_conn = 0;
  BinaryMaxHeap<EdgeWeight> _cur_conns;
  BlockID _cur_block = kInvalidBlockID;
  NodeWeight _cur_weight = 0;

  NodeID _best_prefix_pos = 0;
  BlockID _best_prefix_block = kInvalidBlockID;
  EdgeWeight _best_prefix_conn = 0;
};
} // namespace

MoveSets build_greedy_move_sets(
    const DistributedPartitionedGraph &p_graph,
    const PartitionContext &p_ctx,
    const NodeWeight max_move_set_weight
) {
  MoveSetBuilder builder(p_graph, p_ctx);
  builder.build(max_move_set_weight);
  return builder.finalize();
}
} // namespace kaminpar::dist

