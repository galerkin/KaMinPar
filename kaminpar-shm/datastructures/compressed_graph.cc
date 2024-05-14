/*******************************************************************************
 * Compressed static graph representation.
 *
 * @file:   compressed_graph.cc
 * @author: Daniel Salwasser
 * @date:   01.12.2023
 ******************************************************************************/
#include "compressed_graph.h"

#include "kaminpar-common/assert.h"

namespace kaminpar::shm {

CompressedGraph::CompressedGraph(
    CompactStaticArray<EdgeID> nodes,
    StaticArray<std::uint8_t> compressed_edges,
    StaticArray<NodeWeight> node_weights,
    StaticArray<EdgeWeight> edge_weights,
    EdgeID edge_count,
    NodeID max_degree,
    bool sorted,
    std::size_t num_high_degree_nodes,
    std::size_t num_high_degree_parts,
    std::size_t num_interval_nodes,
    std::size_t num_intervals
)
    : _nodes(std::move(nodes)),
      _compressed_edges(std::move(compressed_edges)),
      _node_weights(std::move(node_weights)),
      _edge_weights(std::move(edge_weights)),
      _edge_count(edge_count),
      _max_degree(max_degree),
      _sorted(sorted),
      _num_high_degree_nodes(num_high_degree_nodes),
      _num_high_degree_parts(num_high_degree_parts),
      _num_interval_nodes(num_interval_nodes),
      _num_intervals(num_intervals) {
  KASSERT(kHighDegreeEncoding || _num_high_degree_nodes == 0);
  KASSERT(kHighDegreeEncoding || _num_high_degree_parts == 0);
  KASSERT(kIntervalEncoding || _num_interval_nodes == 0);
  KASSERT(kIntervalEncoding || _num_intervals == 0);

  if (_node_weights.empty()) {
    _total_node_weight = static_cast<NodeWeight>(n());
    _max_node_weight = 1;
  } else {
    _total_node_weight = parallel::accumulate(_node_weights, static_cast<NodeWeight>(0));
    _max_node_weight = parallel::max_element(_node_weights);
  }

  if (_edge_weights.empty()) {
    _total_edge_weight = static_cast<EdgeWeight>(m());
  } else {
    _total_edge_weight = parallel::accumulate(_edge_weights, static_cast<EdgeWeight>(0));
  }

  init_degree_buckets();
};

void CompressedGraph::init_degree_buckets() {
  KASSERT(std::all_of(_buckets.begin(), _buckets.end(), [](const auto n) { return n == 0; }));

  if (sorted()) {
    constexpr std::size_t kNumBuckets = kNumberOfDegreeBuckets<NodeID> + 1;
    tbb::enumerable_thread_specific<std::array<NodeID, kNumBuckets>> buckets_ets([&] {
      return std::array<NodeID, kNumBuckets>{};
    });

    tbb::parallel_for(tbb::blocked_range<NodeID>(0, n()), [&](const auto &r) {
      auto &buckets = buckets_ets.local();
      for (NodeID u = r.begin(); u != r.end(); ++u) {
        ++buckets[degree_bucket(degree(u)) + 1];
      }
    });

    std::fill(_buckets.begin(), _buckets.end(), 0);
    for (auto &local_buckets : buckets_ets) {
      for (std::size_t i = 0; i < kNumBuckets; ++i) {
        _buckets[i] += local_buckets[i];
      }
    }

    auto last_nonempty_bucket =
        std::find_if(_buckets.rbegin(), _buckets.rend(), [](const auto n) { return n > 0; });
    _number_of_buckets = std::distance(_buckets.begin(), (last_nonempty_bucket + 1).base());
  } else {
    _buckets[1] = n();
    _number_of_buckets = 1;
  }

  std::partial_sum(_buckets.begin(), _buckets.end(), _buckets.begin());
}

void CompressedGraph::update_total_node_weight() {
  if (_node_weights.empty()) {
    _total_node_weight = n();
    _max_node_weight = 1;
  } else {
    _total_node_weight = parallel::accumulate(_node_weights, static_cast<NodeWeight>(0));
    _max_node_weight = parallel::max_element(_node_weights);
  }
}

void CompressedGraph::remove_isolated_nodes(const NodeID isolated_nodes) {
  KASSERT(sorted());

  if (isolated_nodes == 0) {
    return;
  }

  const NodeID new_n = n() - isolated_nodes;
  _nodes.restrict(new_n + 1);
  if (!_node_weights.empty()) {
    _node_weights.restrict(new_n);
  }

  update_total_node_weight();

  // Update degree buckets
  for (std::size_t i = 0; i < _buckets.size() - 1; ++i) {
    _buckets[1 + i] -= isolated_nodes;
  }

  // If the graph has only isolated nodes then there are no buckets afterwards
  if (_number_of_buckets == 1) {
    _number_of_buckets = 0;
  }
}

void CompressedGraph::integrate_isolated_nodes() {
  KASSERT(sorted());

  const NodeID nonisolated_nodes = n();
  _nodes.unrestrict();
  _node_weights.unrestrict();

  const NodeID isolated_nodes = n() - nonisolated_nodes;
  update_total_node_weight();

  // Update degree buckets
  for (std::size_t i = 0; i < _buckets.size() - 1; ++i) {
    _buckets[1 + i] += isolated_nodes;
  }

  // If the graph has only isolated nodes then there is one bucket afterwards
  if (_number_of_buckets == 0) {
    _number_of_buckets = 1;
  }
}

} // namespace kaminpar::shm
