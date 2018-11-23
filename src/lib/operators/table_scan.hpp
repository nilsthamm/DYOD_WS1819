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

class BaseTableScanImpl;
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
  class TableScanImpl {
   public:
  	TableScanImpl(const std::shared_ptr<const AbstractOperator> in, ColumnID column_id, const ScanType scan_type,
            const AllTypeVariant search_value) : _column_id(column_id), _scan_type(scan_type), _search_value(type_cast<T>(search_value)) {}
	
	std::shared_ptr<const Table> _on_execute() {
  	  throw std::logic_error("Not implemented yet");
  	}


   protected:
	ColumnID _column_id;
	ScanType _scan_type;
	T _search_value;

  };

};

}  // namespace opossum
