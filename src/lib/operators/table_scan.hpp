#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "storage/dictionary_segment.hpp"
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
    
    auto compare_lambda(ScanType comp) {
      switch(_scan_type) {
        case ScanType::OpEquals:
          return [=](const T& value) { return _search_value == value; };
        case ScanType::OpNotEquals:
          return [=](const T& value) { return _search_value != value; };
        case ScanType::OpLessThan:
          return [=](const T& value) { return _search_value < value; };
        case ScanType::OpLessThanEquals:
          return [=](const T& value) { return _search_value <= value; };
        case ScanType::OpGreaterThan:
          return [=](const T& value) { return _search_value > value; };
        case ScanType::OpGreaterThanEquals:
          return [=](const T& value) { return _search_value >= value; };
        default:
          Fail("Unreconized ScanType");
      }
    }


    std::shared_ptr<const Table> _on_execute() {
      auto output_table = std::make_shared<Table>(_input_table->chunk_size());
      auto pos_list = std::make_shared<PosList>();
      const auto comapre = compare_lambda(_scan_type); 

      ValueID offset{0};
      for (auto chunk_id = ChunkID{0}; chunk_id < _input_table->chunk_count(); ++chunk_id) {
        const auto& chunk = _input_table->get_chunk(chunk_id);
        // Scan value segment

        // Scan reference segment
          // if(auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk.get_segment(_column_id))) {
          //   const auto referenced_pos_list = reference_segment->pos_list();
          //   const auto reference_table = reference_segment->reference_table();

          //   for (const auto row_id : referenced_pos_list) {
          //     if(auto value_segment = std::dynamic_pointer_cast<ValueSegment>(reference_table->get_chunk(row_id.chunk_id).get_segment(_column_id))) {
          //       if()
          //     } else if(auto dictionary = std::dynamic_pointer_cast<DictionarySegment>(reference_table->get_chunk(row_id.chunk_id).get_segment(_column_id))) {

          //     } else {
          //       Fail("Column type could not be reconized");
          //     }
          //   }
          // }
        // Scan dictionary segment
        if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(chunk.get_segment(_column_id))) {
          const auto dictionary = dictionary_segment->dictionary();
          const auto attribute_vector = dictionary_segment->attribute_vector();

          std::vector<bool> contained_values(chunk.size(), false);
          for (size_t index = 0; index < contained_values.size(); ++index) {
            bool contained = false;
            // TODO perform comparation!
            contained_values[index] = contained;
          }

          for (size_t index = 0; index < attribute_vector->size(); ++index) {
            ValueID value_id = attribute_vector->get(index);
            if (contained_values[value_id]) {
              pos_list->emplace_back(offset + value_id);
            }
          }
        }

        offset += chunk.size();
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
