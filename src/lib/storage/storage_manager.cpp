#include "storage_manager.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

StorageManager& StorageManager::get() {
  static StorageManager instance;
  return instance;
}

void StorageManager::add_table(const std::string& name, std::shared_ptr<Table> table) { _tables[name] = table; }

void StorageManager::drop_table(const std::string& name) {
  if (_tables.erase(name) == 0) {
    throw std::runtime_error("Table to drop does not exist.");
  }
}

std::shared_ptr<Table> StorageManager::get_table(const std::string& name) const { return _tables.at(name); }

bool StorageManager::has_table(const std::string& name) const { return _tables.count(name) != 0; }

std::vector<std::string> StorageManager::table_names() const {
  std::vector<std::string> names;
  for (auto const& table : _tables) {
    names.push_back(table.first);
  }
  return names;
}

void StorageManager::print(std::ostream& out) const {
  for (const auto& [name, table] : _tables) {
    out << name << ", " << table->column_count() << ", " << table->row_count() << ", " << table->chunk_count()
        << std::endl;
  }
}

void StorageManager::reset() { _tables.clear(); }

}  // namespace opossum
