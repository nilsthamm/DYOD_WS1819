#pragma once

#include <memory>
#include <vector>

#include "base_attribute_vector.hpp"
#include "types.hpp"

namespace opossum {

// An AttributeVector that can adapt its internal member size to the number of items in the dictionary
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

std::shared_ptr<BaseAttributeVector> make_fitted_attribute_vector(size_t dictionary_size, size_t segment_size);

}  // namespace opossum
