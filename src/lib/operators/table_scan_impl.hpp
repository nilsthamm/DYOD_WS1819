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

class BaseTableScanImpl {
public:
  virtual ~BaseTableScanImpl() {}

  virtual std::shared_ptr<const Table> on_execute() const = 0;
};

// Add all ValueIDs of an DictionarySegment's attribute vector that fulfill a specific condition (templated Comparator, see dictionary segment scan part)
// with the given search_pos to a PosList
template <typename Compare>
void add_to_pos_list(std::shared_ptr<PosList> pos_list, const ChunkID chunk_id, const std::shared_ptr<const BaseAttributeVector> attribute_vector, const ValueID search_pos) {
  Compare compare = Compare();
  for (ChunkOffset index = 0; index < attribute_vector->size(); ++index) {
    if (compare(attribute_vector->get(index), search_pos)) {
      pos_list->emplace_back(RowID{chunk_id, index});
    }
  }
}

// Add all ValueIDs of an DictionarySegment's attribute vector to a PosList
void add_all_to_pos_list(std::shared_ptr<PosList> pos_list, ChunkID chunk_id, const std::shared_ptr<const BaseAttributeVector> attribute_vector);

template<typename T>
  class TableScanImpl : public BaseTableScanImpl {
   public:
    TableScanImpl(ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value, std::shared_ptr<const Table> input_table ) : _column_id(column_id), _scan_type(scan_type), _search_value(type_cast<T>(search_value)), _input_table(input_table) {}

    std::shared_ptr<const Table> on_execute() const override {
      auto output_table = std::make_shared<Table>(_input_table->chunk_size());
      auto pos_list = std::make_shared<PosList>();
      // Use a pre defined lambda to capsual the comparison logic from the actual scan procedure, since the scan_type and _search_value don't change
      const auto compare = _compare_lambda(_scan_type);
      // Defined here to be used later in the reference segment. If the input table itselfs contains reference segments, the variable is set to the source table
      std::shared_ptr<const Table> output_reference_table = _input_table;

      for (auto chunk_id = ChunkID{0}; chunk_id < _input_table->chunk_count(); ++chunk_id) {
        const auto& chunk = _input_table->get_chunk(chunk_id);

        if(chunk.size() == 0) {
          continue;
        }

        // Scan value segment
        if (const auto value_segment = std::dynamic_pointer_cast<ValueSegment<T>>(chunk.get_segment(_column_id))) {
          const auto& values = value_segment->values();
          // Iterating over the value vector from the value segment and comparing every value with the pre defined compare lambda
          for (ChunkOffset index = 0; index < values.size(); ++index) {
            if (compare(values[index])) {
              pos_list->emplace_back(RowID{chunk_id, index});
            }
          }
        // Scan reference segment
        } else if (auto reference_segment = std::dynamic_pointer_cast<ReferenceSegment>(chunk.get_segment(_column_id))) {
          // set the reference table for the output segments to the original source table
          output_reference_table = reference_segment->referenced_table();
          const auto referenced_pos_list = reference_segment->pos_list();
          const auto reference_table = reference_segment->referenced_table();

          for (uint32_t index = 0; index < referenced_pos_list->size(); index++) {
            const auto& row_id = (*referenced_pos_list)[index];
            // Depending on the source segment type we need to get the value on different ways
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
        // Scan dictionary segment
        } else if (const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<T>>(chunk.get_segment(_column_id))) {

          const auto dictionary = dictionary_segment->dictionary();
          const auto attribute_vector = dictionary_segment->attribute_vector();

          ValueID search_pos;
          switch ( _scan_type ) {
            case ScanType::OpEquals:
              search_pos = dictionary_segment->lower_bound(_search_value);
              if (search_pos != INVALID_VALUE_ID && (*dictionary)[search_pos] == _search_value) {
                add_to_pos_list<std::equal_to<ValueID>>(pos_list, chunk_id, attribute_vector, search_pos);
              }
              break;

            case ScanType::OpNotEquals:
              search_pos = dictionary_segment->lower_bound(_search_value);
              if (search_pos != INVALID_VALUE_ID && (*dictionary)[search_pos] == _search_value) {
                add_to_pos_list<std::not_equal_to<ValueID>>(pos_list, chunk_id, attribute_vector, search_pos);
              } else {
                add_all_to_pos_list(pos_list, chunk_id, attribute_vector);
              }
              break;

            case ScanType::OpLessThan:
              search_pos = dictionary_segment->lower_bound(_search_value);
              if (search_pos != INVALID_VALUE_ID && (*dictionary)[search_pos] == _search_value) {
                add_to_pos_list<std::less<ValueID>>(pos_list, chunk_id, attribute_vector, search_pos);
              } else if (dictionary->back() < _search_value) {
                add_all_to_pos_list(pos_list, chunk_id, attribute_vector);
              }
              break;

            case ScanType::OpLessThanEquals:
              search_pos = dictionary_segment->lower_bound(_search_value);
              if (search_pos != INVALID_VALUE_ID) {
                if ((*dictionary)[search_pos] == _search_value) {
                  add_to_pos_list<std::less_equal<ValueID>>(pos_list, chunk_id, attribute_vector, search_pos);
                } else {
                  add_to_pos_list<std::less<ValueID>>(pos_list, chunk_id, attribute_vector, search_pos);
                }
              } else if (dictionary->back() < _search_value) {
                add_all_to_pos_list(pos_list, chunk_id, attribute_vector);
              }
              break;

            case ScanType::OpGreaterThan:
              search_pos = dictionary_segment->upper_bound(_search_value);

              if (search_pos != INVALID_VALUE_ID) {
                if ((*dictionary)[search_pos] == _search_value) {
                  add_to_pos_list<std::greater<ValueID>>(pos_list, chunk_id, attribute_vector, search_pos);
                } else {
                  add_to_pos_list<std::greater_equal<ValueID>>(pos_list, chunk_id, attribute_vector, search_pos);
                }
              }
              break;

            case ScanType::OpGreaterThanEquals:
              search_pos = dictionary_segment->upper_bound(_search_value);
              if (search_pos != INVALID_VALUE_ID) {
                if (search_pos != ValueID{0}) {
                  search_pos--;
                }
                add_to_pos_list<std::greater_equal<ValueID>>(pos_list, chunk_id, attribute_vector, search_pos);
              }
              break;

            default:
              Fail("Unreconized ScanType");
          }
        }
      }

      // Create table structure
      // Therefor we fill the Chunk created at the initialization of the output table with 
      // reference segments and add the column definitions manually to the table
      Chunk &output_chunk = output_table->get_chunk(ChunkID{0});

      for(ColumnID column_id = ColumnID{0}; column_id < _input_table->column_count(); column_id++) {
        output_table->add_column_definition(_input_table->column_name(column_id), _input_table->column_type(column_id));
        // Every segment gets the same pos list and referenced table
        output_chunk.add_segment(std::make_shared<ReferenceSegment>(output_reference_table, column_id, pos_list));
      }

      return output_table;
    }


   protected:
    ColumnID _column_id;
    ScanType _scan_type;
    T _search_value;
    std::shared_ptr<const Table> _input_table;

    const std::function<bool(const T&)> _compare_lambda(const ScanType comp) const {
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
  };

}  // namespace opossum
