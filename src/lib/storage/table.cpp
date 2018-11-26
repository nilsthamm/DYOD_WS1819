#include "table.hpp"

#include <algorithm>
#include <iomanip>
#include <limits>
#include <memory>
#include <numeric>
#include <string>
#include <utility>
#include <vector>

#include "dictionary_segment.hpp"
#include "value_segment.hpp"

#include "resolve_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

Table::Table(const uint32_t chunk_size) {
  _chunk_size = chunk_size;
  _add_chunk();
}

void Table::add_column_definition(const std::string& name, const std::string& type) {
  _column_names.push_back(name);
  _column_types.push_back(type);
}

void Table::add_column(const std::string& name, const std::string& type) {
  _column_names.push_back(name);
  _column_types.push_back(type);
  _chunks.back().add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
}

void Table::append(std::vector<AllTypeVariant> values) {
  if (_chunks.back().size() == _chunk_size) {
    _add_chunk();
  }
  _chunks.back().append(values);
}

void Table::create_new_chunk() {
  // Implementation goes here
}

uint16_t Table::column_count() const { return _column_types.size(); }

uint64_t Table::row_count() const {
  uint64_t count = 0;
  for (const auto& chunk : _chunks) {
    count += chunk.size();
  }
  return count;
}

ChunkID Table::chunk_count() const { return static_cast<ChunkID>(_chunks.size()); }

ColumnID Table::column_id_by_name(const std::string& column_name) const {
  auto const pos = std::find(_column_names.begin(), _column_names.end(), column_name);
  if (pos == _column_names.end()) {
    throw std::runtime_error("Could not find table.");
  } else {
    return ColumnID(pos - _column_names.begin());
  }
}

uint32_t Table::chunk_size() const { return _chunk_size; }

const std::vector<std::string>& Table::column_names() const { return _column_names; }

const std::string& Table::column_name(ColumnID column_id) const { return _column_names[column_id]; }

const std::string& Table::column_type(ColumnID column_id) const { return _column_types[column_id]; }

Chunk& Table::get_chunk(ChunkID chunk_id) {
  std::unique_lock<std::shared_mutex> lock(_mutex_chunk_access);
  return _chunks[chunk_id];
}

const Chunk& Table::get_chunk(ChunkID chunk_id) const {
  std::shared_lock<std::shared_mutex> lock(_mutex_chunk_access);
  return _chunks[chunk_id];
}

void Table::_add_chunk() {
  Chunk new_chunk;
  for (auto const& type : _column_types) {
    new_chunk.add_segment(make_shared_by_data_type<BaseSegment, ValueSegment>(type));
  }
  _chunks.emplace_back(std::move(new_chunk));
}

void Table::compress_chunk(ChunkID chunk_id) {
  Assert(chunk_id < _chunks.size(), "Chunk ID out of range");

  Chunk new_chunk;

  const auto& old_chunk = get_chunk(chunk_id);

  Assert(old_chunk.size() == _chunk_size, "Chunk not full");

  for (ColumnID column_id = ColumnID{0}; column_id < old_chunk.column_count(); ++column_id) {
    const auto segment = old_chunk.get_segment(column_id);

    new_chunk.add_segment(make_shared_by_data_type<BaseSegment, DictionarySegment>(column_type(column_id), segment));
  }

  std::unique_lock<std::shared_mutex> lock(_mutex_chunk_access);
  _chunks[chunk_id] = std::move(new_chunk);
}

void Table::emplace_chunk(Chunk& chunk) {
  _chunks.emplace_back(std::move(chunk));
}

}  // namespace opossum
