#include "fitted_attribute_vector.hpp"

#include <limits>
#include <memory>
#include <vector>

#include "utils/assert.hpp"

namespace opossum {

template <typename T>
FittedAttributeVector<T>::FittedAttributeVector(size_t size) { _attribute_vector.resize(size); }

template <typename T>
ValueID FittedAttributeVector<T>::get(const size_t i) const { return ValueID{_attribute_vector[i]}; }

template <typename T>
void FittedAttributeVector<T>::set(const size_t i, const ValueID value_id) {
	DebugAssert(value_id <= ValueID{std::numeric_limits<T>::max()}, "ValueID must fit within templated type.");
	_attribute_vector[i] = value_id;
}

template <typename T>
size_t FittedAttributeVector<T>::size() const { return _attribute_vector.size(); }

template <typename T>
AttributeVectorWidth FittedAttributeVector<T>::width() const { return AttributeVectorWidth{sizeof(T)}; }

}  // namespace opossum
