/*******************************************************************************
 * Reader and writer for binary files.
 *
 * @file:   bianry_util.h
 * @author: Daniel Salwasser
 * @date:   07.07.2024
 ******************************************************************************/
#pragma once

#include <cstddef>
#include <cstdint>
#include <exception>
#include <fstream>
#include <string>
#include <vector>

#include "kaminpar-common/datastructures/static_array.h"

namespace kaminpar::io {

class BinaryReaderException : public std::exception {
public:
  BinaryReaderException(std::string msg) : _msg(std::move(msg)) {}

  [[nodiscard]] const char *what() const noexcept override {
    return _msg.c_str();
  }

private:
  std::string _msg;
};

class BinaryReader {
public:
  BinaryReader(const std::string &filename) {
    _in.open(filename, std::ios::binary);
    if (!_in) {
      throw BinaryReaderException("Cannot read the file that stores the graph");
    }

    _in.seekg(0, std::ios::end);
    _length = _in.tellg();
    _in.seekg(0, std::ios::beg);

    _data.resize(_length);
    _in.read(reinterpret_cast<char*>(_data.data()), _length);
  }

  template <typename T> [[nodiscard]] T read(const std::size_t position) const {
    return *reinterpret_cast<const T *>(_data.data() + position);
  }

  template <typename T> [[nodiscard]] const T *fetch(const std::size_t position) const {
    return reinterpret_cast<const T *>(_data.data() + position);
  }

private:
  std::ifstream _in;
  std::size_t _length;
  std::vector<std::uint8_t> _data;
};

class BinaryWriter {
public:
  BinaryWriter(const std::string &filename) : _out(filename, std::ios::binary) {}

  void write(const char *data, const std::size_t size) {
    _out.write(data, size);
  }

  template <typename T> void write_int(const T value) {
    _out.write(reinterpret_cast<const char *>(&value), sizeof(T));
  }

  template <typename T> void write_raw_static_array(const StaticArray<T> &static_array) {
    const char *data = reinterpret_cast<const char *>(static_array.data());
    const std::size_t size = static_array.size() * sizeof(T);
    write(data, size);
  }

private:
  std::ofstream _out;
};

} // namespace kaminpar::io
