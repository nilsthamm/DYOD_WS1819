#pragma once

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "all_type_variant.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

#include "../lib/storage/base_attribute_vector.hpp"
#include "../lib/storage/fitted_attribute_vector.hpp"
#include "../lib/storage/value_segment.hpp"
#include "../lib/type_cast.hpp"

namespace opossum {

class BaseAttributeVector;
class BaseSegment;

// Even though ValueIDs do not have to use the full width of ValueID (uint32_t), this will also work for smaller ValueID
// types (uint8_t, uint16_t) since after a down-cast INVALID_VALUE_ID will look like their numeric_limit::max()
constexpr ValueID INVALID_VALUE_ID{std::numeric_limits<ValueID::base_type>::max()};

// Dictionary is a specific segment type that stores all its values in a vector
template <typename T>
class DictionarySegment : public BaseSegment {
 public:
  /**
   * Creates a Dictionary segment from a given value segment.
   */
  explicit DictionarySegment(const std::shared_ptr<BaseSegment>& base_segment) {
    const auto& values = std::static_pointer_cast<ValueSegment<T>>(base_segment)->values();

    _dictionary = std::make_shared<std::vector<T>>(values.cbegin(), values.cend());

    std::sort(_dictionary->begin(), _dictionary->end());
    _dictionary->erase(std::unique(_dictionary->begin(), _dictionary->end()), _dictionary->end());
    _dictionary->shrink_to_fit();

    _attribute_vector = make_fitted_attribute_vector(_dictionary->size(), base_segment->size());

    for (ValueID index{0}; index < base_segment->size(); index++) {
      const auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), values[index]);
      DebugAssert(it != _dictionary->cend(), "Value must be contained in dictionary.");
      ValueID position = static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
      _attribute_vector->set(index, position);
    }
  }

  // SEMINAR INFORMATION: Since most of these methods depend on the template parameter, you will have to implement
  // the DictionarySegment in this file. Replace the method signatures with actual implementations.

  // return the value at a certain position. If you want to write efficient operators, back off!
  const AllTypeVariant operator[](const size_t i) const override {
    PerformanceWarning("operator[] used");

    return AllTypeVariant{get(i)};
  }

  // return the value at a certain position.
  const T get(const size_t i) const { return (*_dictionary)[_attribute_vector->get(i)]; }

  // dictionary segments are immutable
  void append(const AllTypeVariant&) override { Fail("DictionarySegment is immutable."); }

  // returns an underlying dictionary
  std::shared_ptr<const std::vector<T>> dictionary() const { return _dictionary; }

  // returns an underlying data structure
  std::shared_ptr<const BaseAttributeVector> attribute_vector() const { return _attribute_vector; }

  // return the value represented by a given ValueID
  const T& value_by_value_id(ValueID value_id) const { return _dictionary[value_id]; }

  // returns the first value ID that refers to a value >= the search value
  // returns INVALID_VALUE_ID if all values are smaller than the search value
  ValueID lower_bound(T value) const {
    const auto bound = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (bound == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    return static_cast<ValueID>(std::distance(_dictionary->cbegin(), bound));
  }

  // same as lower_bound(T), but accepts an AllTypeVariant
  ValueID lower_bound(const AllTypeVariant& value) const { return lower_bound(type_cast<T>(value)); }

  // returns the first value ID that refers to a value > the search value
  // returns INVALID_VALUE_ID if all values are smaller than or equal to the search value
  ValueID upper_bound(T value) const {
    const auto bound = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
    if (bound == _dictionary->cend()) {
      return INVALID_VALUE_ID;
    }
    return static_cast<ValueID>(std::distance(_dictionary->cbegin(), bound));
  }

  // same as upper_bound(T), but accepts an AllTypeVariant
  ValueID upper_bound(const AllTypeVariant& value) const { return upper_bound(type_cast<T>(value)); }

  // return the number of unique_values (dictionary entries)
  size_t unique_values_count() const { return _dictionary->size(); }

  // return the number of entries
  size_t size() const override { return _attribute_vector->size(); }

 protected:
  std::shared_ptr<std::vector<T>> _dictionary;
  std::shared_ptr<BaseAttributeVector> _attribute_vector;
};

}  // namespace opossum
