/*******************************************************************************
 * Input benchmark for the shared-memory algorithm.
 *
 * @file:   shm_input_benchmark.cc
 * @author: Daniel Salwasser
 * @date:   25.05.2024
 ******************************************************************************/
// clang-format off
#include <kaminpar-cli/kaminpar_arguments.h>
// clang-format on

#include <tbb/global_control.h>

#include "kaminpar-shm/context_io.h"
#include "kaminpar-shm/graphutils/compressed_graph_builder.h"
#include "kaminpar-shm/graphutils/parallel_compressed_graph_builder.h"

#include "kaminpar-common/console_io.h"
#include "kaminpar-common/logger.h"
#include "kaminpar-common/random.h"
#include "kaminpar-common/timer.h"

#include "apps/io/shm_io.h"

using namespace kaminpar;
using namespace kaminpar::shm;

int main(int argc, char *argv[]) {
  // Create context
  Context ctx = create_default_context();
  ctx.partition.k = 2;

  // Parse CLI arguments
  std::string graph_filename;
  io::GraphFileFormat graph_file_format = io::GraphFileFormat::METIS;
  bool compress_in_memory = false;
  int seed = 0;

  CLI::App app("Shared-memory input benchmark");
  app.add_option("-G,--graph", graph_filename, "Graph file")->required();
  app.add_option("-f,--graph-file-format", graph_file_format)
      ->transform(CLI::CheckedTransformer(io::get_graph_file_formats()).description(""))
      ->description(R"(Graph file formats:
  - metis
  - parhip)")
      ->capture_default_str();
  app.add_option("--node-order", ctx.node_ordering)
      ->transform(CLI::CheckedTransformer(get_node_orderings()).description(""))
      ->description(R"(Criteria by which the nodes of the graph are sorted and rearranged:
  - natural:     keep node order of the graph (do not rearrange)
  - deg-buckets: sort nodes by degree bucket and rearrange accordingly
  - implicit-deg-buckets: nodes of the input graph are sorted by deg-buckets order)")
      ->capture_default_str();
  app.add_flag(
         "--compress-in-memory",
         compress_in_memory,
         "Whether to compress the input graph in memory when graph compression is enabled"
  )
      ->capture_default_str();
  app.add_option("-t,--threads", ctx.parallel.num_threads, "Number of threads")
      ->capture_default_str();
  app.add_option("-s,--seed", seed, "Seed for random number generation")->capture_default_str();
  app.add_option("-k,--k", ctx.partition.k);
  app.add_option("-e,--epsilon", ctx.partition.epsilon);
  create_graph_compression_options(&app, ctx);
  CLI11_PARSE(app, argc, argv);

  tbb::global_control gc(tbb::global_control::max_allowed_parallelism, ctx.parallel.num_threads);
  Random::reseed(seed);

  GLOBAL_TIMER.reset();
  ENABLE_HEAP_PROFILER();

  TIMED_SCOPE("Read Input Graph") {
    SCOPED_HEAP_PROFILER("Read Input Graph");

    if (ctx.compression.enabled && compress_in_memory) {
      CSRGraph csr_graph = TIMED_SCOPE("Read CSR Graph") {
        SCOPED_HEAP_PROFILER("Read CSR Graph");
        return io::csr_read(graph_filename, graph_file_format, ctx.node_ordering);
      };

      SCOPED_TIMER("Compress CSR Graph");
      SCOPED_HEAP_PROFILER("Compress CSR Graph");

      const bool sequential_compression = ctx.parallel.num_threads <= 1;
      if (sequential_compression) {
        Graph graph =
            Graph(std::make_unique<CompressedGraph>(CompressedGraphBuilder::compress(csr_graph)));
        ctx.setup(graph);
      } else {
        Graph graph = Graph(std::make_unique<CompressedGraph>(parallel_compress(csr_graph)));
        ctx.setup(graph);
      }
    } else {
      Graph graph =
          io::read(graph_filename, graph_file_format, ctx.node_ordering, ctx.compression.enabled);
      ctx.setup(graph);
    }
  };

  DISABLE_HEAP_PROFILER();
  STOP_TIMER();

  cio::print_delimiter("Input Summary", '#');
  std::cout << "Execution mode:               " << ctx.parallel.num_threads << "\n";
  std::cout << "Seed:                         " << Random::get_seed() << "\n";
  cio::print_delimiter("Graph Compression", '-');
  shm::print(ctx.compression, std::cout);
  LOG;

  cio::print_delimiter("Result Summary");
  Timer::global().print_human_readable(std::cout);
  LOG;
  heap_profiler::HeapProfiler::global().set_detailed_summary_options();
  PRINT_HEAP_PROFILE(std::cout);

  return 0;
}
