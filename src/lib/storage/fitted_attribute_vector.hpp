#pragma once

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

// An AttributeVector that can adopt to the number of items in the dictionary
template <typename T>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  explicit FittedAttributeVector(size_t size);
  
  // returns the value id at a given position
  ValueID get(const size_t i) const;

  // sets the value id at a given position
  void set(const size_t i, const ValueID value_id);

  // returns the number of values
  size_t size() const;

  // returns the width of biggest value id in bytes
  AttributeVectorWidth width() const;

 protected:
  std::vector<T> _attribute_vector;
};

std::shared_ptr<BaseAttributeVector> make_fitted_attribute_vector(size_t dictionary_size, size_t segment_size) {
  if (dictionary_size <= std::numeric_limits<uint8_t>::max()) {
    return std::make_shared<FittedAttributeVector<uint8_t>>(segment_size);
  } else if (dictionary_size <= std::numeric_limits<uint16_t>::max()) {
    return std::make_shared<FittedAttributeVector<uint16_t>>(segment_size);
  } else if (dictionary_size <= std::numeric_limits<uint32_t>::max()) {
    return std::make_shared<FittedAttributeVector<uint32_t>>(segment_size);
  } else {
    // we would use Fail, but the compiler complains that no value is returned
    throw std::logic_error("Dictionary size is too large.");
  }
}

}  // namespace opossum
