#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

class BaseTableScanImpl {
};
class Table;

class TableScan : public AbstractOperator {
 public:
  TableScan(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value);

  ~TableScan();

  ColumnID column_id() const;
  ScanType scan_type() const;
  const AllTypeVariant& search_value() const;

 protected:
  std::shared_ptr<const Table> _on_execute() override;

  ColumnID _column_id;
  ScanType _scan_type;
  AllTypeVariant _search_value;

  std::shared_ptr<const BaseTableScanImpl> _table_scan_impl;

  template<typename T>
  class TableScanImpl : public BaseTableScanImpl {
   public:
    TableScanImpl(ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value, std::shared_ptr<const Table> input_table ) : _column_id(column_id), _scan_type(scan_type), _search_value(type_cast<T>(search_value)), _input_table(input_table) {}
  
    std::shared_ptr<const Table> _on_execute() {
      auto output_table = std::make_shared<Table>(_input_table->chunk_size());
      auto pos_list = std::make_shared<PosList>();

      for (auto chunk_id = ChunkID{0}; chunk_id < _input_table->chunk_count(); ++chunk_id) {
        // const auto& chunk = _input_table->get_chunk(chunk_id);
        for (auto column_id = ColumnID{0}; column_id < _input_table->column_count(); column_id++) {
        // Scan value segment

        // Scan reference segment
          // if(auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk.get_segment(column_id))) {
          //   const auto referenced_pos_list = reference_segment->pos_list();
          //   const auto reference_table = reference_segment->reference_table();

          //   for (const auto row_id : referenced_pos_list) {

          //   }
          // }

        // Scan dictionary segment

        }
      }
    // Create table structure

      return output_table;
    }


   protected:
  ColumnID _column_id;
  ScanType _scan_type;
  T _search_value;
  std::shared_ptr<const Table> _input_table;

  };

};

}  // namespace opossum
