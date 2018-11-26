#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "abstract_operator.hpp"
#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/base_segment.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/reference_segment.hpp"
#include "storage/table.hpp"
#include "types.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"

namespace opossum {

class Table;

class BaseTableScanImpl {
public:
  virtual ~BaseTableScanImpl() {}

  virtual std::shared_ptr<const Table> _on_execute() const = 0;
};

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
    
    const std::function<bool(const T&)> compare_lambda(ScanType comp) const {
      switch(_scan_type) {
        case ScanType::OpEquals:
          return [=](const T& value) -> bool { return value == _search_value; };
        case ScanType::OpNotEquals:
          return [=](const T& value) -> bool { return value != _search_value; };
        case ScanType::OpLessThan:
          return [=](const T& value) -> bool { return value < _search_value; };
        case ScanType::OpLessThanEquals:
          return [=](const T& value) -> bool { return value <= _search_value; };
        case ScanType::OpGreaterThan:
          return [=](const T& value) -> bool { return value > _search_value; };
        case ScanType::OpGreaterThanEquals:
          return [=](const T& value) -> bool { return value >= _search_value; };
        default:
          Fail("Unreconized ScanType");
          // compiler complains that nothing is returned otherwise
          return [=](const T& value) -> bool { return false; };
      }
    }

    std::shared_ptr<const Table> _on_execute() const override {
      auto output_table = std::make_shared<Table>(_input_table->chunk_size());
      auto pos_list = std::make_shared<PosList>();
      const auto compare = compare_lambda(_scan_type);
      std::shared_ptr<const Table> output_reference_table = _input_table;

      ValueID offset{0};
      for (auto chunk_id = ChunkID{0}; chunk_id < _input_table->chunk_count(); ++chunk_id) {
        const auto& chunk = _input_table->get_chunk(chunk_id);

        if(chunk.size() == 0) {
          continue;
        }

        // Scan value segment
        if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(chunk.get_segment(_column_id))) {
          const auto& values = value_segment->values();
          for (ChunkOffset index = 0; index < values.size(); ++index) {
            if (compare(values[index])) {
              pos_list->emplace_back(RowID{chunk_id, index});
            }
          }
        }

        // Scan reference segment
          if(auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk.get_segment(_column_id))) {
            output_reference_table = reference_segment->referenced_table();
            const auto referenced_pos_list = reference_segment->pos_list();
            const auto reference_table = reference_segment->referenced_table();

            for (uint32_t index = 0; index < referenced_pos_list->size(); index++) {
              const auto& row_id = (*referenced_pos_list)[index];
              if(auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(reference_table->get_chunk(row_id.chunk_id).get_segment(_column_id))) {
                if(compare(value_segment->values()[row_id.chunk_offset])) {
                  pos_list->emplace_back(row_id);
                }
              } else if(auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(reference_table->get_chunk(row_id.chunk_id).get_segment(_column_id))) {
                if(compare(dictionary_segment->get(row_id.chunk_offset))) {
                  pos_list->emplace_back(row_id);
                }
              } else {
                Fail("Column type could not be reconized");
              }
            }
          }
        // Scan dictionary segment
        if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(chunk.get_segment(_column_id))) {
          const auto& dictionary = dictionary_segment->dictionary();
          const auto& attribute_vector = dictionary_segment->attribute_vector();

          std::vector<bool> contained_values(chunk.size(), false);
          for (ChunkOffset index = 0; index < contained_values.size(); ++index) {
            // TODO optimize
            contained_values[index] = compare((*dictionary)[index]);
          }

          for (ChunkOffset index = 0; index < attribute_vector->size(); ++index) {
            ValueID value_id = attribute_vector->get(index);
            if (contained_values[value_id]) {
              pos_list->emplace_back(RowID{chunk_id, index});
            }
          }
        }

        offset += chunk.size();
      }
      // Create table structure

      Chunk output_chunk;

      for(ColumnID column_id = ColumnID{0}; column_id < _input_table->column_count(); column_id++) {
        output_table->add_column_definition(_input_table->column_name(column_id), _input_table->column_type(column_id));
        output_chunk.add_segment(std::make_shared<ReferenceSegment>(output_reference_table, column_id, pos_list));
      }

      output_table->emplace_chunk(output_chunk);

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
