/*******************************************************************************
 * This file is part of KaMinPar.
 *
 * Copyright (C) 2021 Daniel Seemaier <daniel.seemaier@kit.edu>
 *
 * KaMinPar is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * KaMinPar is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with KaMinPar.  If not, see <http://www.gnu.org/licenses/>.
 *
******************************************************************************/
#pragma once

#include "dkaminpar/distributed_definitions.h"

#include <concepts>
#include <mpi.h>
#include <utility>
#include <vector>

namespace dkaminpar::mpi {
inline std::pair<int, int> get_comm_info(MPI_Comm comm = MPI_COMM_WORLD) {
  int size;
  MPI_Comm_size(comm, &size);
  int rank;
  MPI_Comm_rank(comm, &rank);
  return {size, rank};
}

inline int get_comm_size(MPI_Comm comm = MPI_COMM_WORLD) {
  int size;
  MPI_Comm_size(comm, &size);
  return size;
}

inline int get_comm_rank(MPI_Comm comm = MPI_COMM_WORLD) {
  int rank;
  MPI_Comm_rank(comm, &rank);
  return rank;
}

template<typename Lambda>
inline void sequentially(Lambda &&lambda, MPI_Comm comm = MPI_COMM_WORLD) {
  constexpr bool use_rank_argument = requires { lambda(int()); };
  const auto [size, rank] = get_comm_info();
  for (int p = 0; p < size; ++p) {
    if (p == rank) {
      if constexpr (use_rank_argument) {
        lambda(p);
      } else {
        lambda();
      }
    }
    MPI_Barrier(comm);
  }
}

template<std::ranges::range Distribution>
inline std::vector<int> build_distribution_recvcounts(Distribution &&dist) {
  ASSERT(!dist.empty());
  std::vector<int> recvcounts(dist.size() - 1);
  for (std::size_t i = 0; i + 1 < dist.size(); ++i) { recvcounts[i] = dist[i + 1] - dist[i]; }
  return recvcounts;
}

template<std::ranges::range Distribution>
inline std::vector<int> build_distribution_displs(Distribution &&dist) {
  ASSERT(!dist.empty());
  std::vector<int> displs(dist.size() - 1);
  for (std::size_t i = 0; i + 1 < dist.size(); ++i) { displs[i] = static_cast<int>(dist[i]); }
  return displs;
}
} // namespace dkaminpar::mpi