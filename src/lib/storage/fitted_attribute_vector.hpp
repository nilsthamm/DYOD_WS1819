#pragma once

#include <limits>
#include <memory>
#include <vector>

#include "types.hpp"
#include "base_attribute_vector.hpp"

namespace opossum {

// An AttributeVector that can adopt to the number of items in the dictionary
template<typename T>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  explicit FittedAttributeVector(size_t size) {
    _attribute_vector.resize(size);
  }
  // returns the value id at a given position
  ValueID get(const size_t i) const {
    return ValueID{_attribute_vector[i]};
  }

  // sets the value id at a given position
  void set(const size_t i, const ValueID value_id) {
    DebugAssert(
        value_id <= ValueID{std::numeric_limits<T>::max()},
        "ValueID must fit within templated type.");
    _attribute_vector[i] = value_id;
  }

  // returns the number of values
  size_t size() const {
    return _attribute_vector.size();
  }

  // returns the width of biggest value id in bytes
  AttributeVectorWidth width() const {
    return AttributeVectorWidth{sizeof(T)};
  }

 protected:
  std::vector<T> _attribute_vector;
};

static std::shared_ptr<BaseAttributeVector> make_fitted_attribute_vector(size_t dict_size, size_t segment_size) {
  if (dict_size <= std::numeric_limits<uint8_t>::max()) {
    return std::make_shared<FittedAttributeVector<uint8_t>>(segment_size);
  } else if (dict_size <= std::numeric_limits<uint16_t>::max()) {
    return std::make_shared<FittedAttributeVector<uint16_t>>(segment_size);
  } else if (dict_size <= std::numeric_limits<uint32_t>::max()) {
    return std::make_shared<FittedAttributeVector<uint32_t>>(segment_size);
  } else {
    // we would use Fail, but the compiler complains that no value is returned
    throw std::logic_error("Dictionary size is too large.");
  }
}

}  // namespace opossum
