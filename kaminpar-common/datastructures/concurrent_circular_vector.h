/*******************************************************************************
 * A circular vector that allows concurrent incremental updates.
 *
 * @file:   concurrent_circular_vector.h
 * @author: Daniel Salwasser
 * @date:   03.05.2024
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <limits>
#include <vector>

#include "kaminpar-common/assert.h"

namespace kaminpar {

template <typename Size, typename Value> class ConcurrentCircularVector {
  // This value is used to indicate that an entry has not been set.
  static constexpr Value kLock = std::numeric_limits<Value>::max();

public:
  /**
   * Constructs a ConcurrentCircularVector.
   *
   * @param size The size of the vector. Note that this value has to be at least as large as the
   * number of parallel tasks that synchronize.
   */
  ConcurrentCircularVector(const Size size) : _counter(0), _buffer(size + 1) {
    std::fill(_buffer.begin(), _buffer.end() - 1, kLock);
  }

  /*!
   * Returns the next entry to write to.
   *
   * @return The next entry to write to.
   */
  [[nodiscard]] Size next() {
    return __atomic_fetch_add(&_counter, 1, __ATOMIC_RELAXED);
  }

  /*!
   * Fetches the value of the previous entry when it is set and sets the given entry to this value
   * plus a given delta. Note that this method blocks until the previous entry is set.
   *
   * @param entry The entry to update.
   * @param delta The value to add to the previous value whose result is then stored in the entry.
   * @return The value of the previous entry
   */
  [[nodiscard]] Value fetch_and_update(const Size entry, const Value delta) {
    const Size pos = entry % _buffer.size();
    const Size prev_pos = (pos == 0) ? (_buffer.size() - 1) : (pos - 1);

    Value value;
    do {
      value = __atomic_load_n(&_buffer[prev_pos], __ATOMIC_RELAXED);
    } while (value == kLock);

    KASSERT((value + delta) != kLock);
    __atomic_store_n(&_buffer[prev_pos], kLock, __ATOMIC_RELAXED);
    __atomic_store_n(&_buffer[pos], value + delta, __ATOMIC_RELAXED);

    return value;
  }

private:
  Size _counter;
  std::vector<Value> _buffer;
};

} // namespace kaminpar
