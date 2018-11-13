#include <memory>
#include <string>

#include "../base_test.hpp"
#include "gtest/gtest.h"

#include "../../lib/resolve_type.hpp"
#include "../../lib/storage/base_segment.hpp"
#include "../../lib/storage/dictionary_segment.hpp"
#include "../../lib/storage/fitted_attribute_vector.hpp"
#include "../../lib/storage/value_segment.hpp"

class StorageFittedAttributeVectorTest : public ::testing::Test {
};

TEST_F(StorageFittedAttributeVectorTest, SetGet) {
  opossum::FittedAttributeVector<uint8_t> fitted_vector_uint8(1);
  opossum::FittedAttributeVector<uint16_t> fitted_vector_uint16(1);
  opossum::FittedAttributeVector<uint32_t> fitted_vector_uint32(1);

  EXPECT_NO_THROW(fitted_vector_uint8.set(0, opossum::ValueID{255}));
  EXPECT_THROW(fitted_vector_uint8.set(0, opossum::ValueID{256}), std::exception);
  EXPECT_NO_THROW(fitted_vector_uint16.set(0, opossum::ValueID{256}));
  EXPECT_THROW(fitted_vector_uint16.set(0, opossum::ValueID{65536}), std::exception);
  EXPECT_NO_THROW(fitted_vector_uint32.set(0, opossum::ValueID{65536}));

  EXPECT_EQ(fitted_vector_uint8.get(0), opossum::ValueID{255});
}

TEST_F(StorageFittedAttributeVectorTest, Width) {
  opossum::FittedAttributeVector<uint8_t> fitted_vector_uint8(1);
  opossum::FittedAttributeVector<uint16_t> fitted_vector_uint16(1);
  opossum::FittedAttributeVector<uint32_t> fitted_vector_uint32(1);

  EXPECT_EQ(fitted_vector_uint8.width(), 1);
  EXPECT_EQ(fitted_vector_uint16.width(), 2);
  EXPECT_EQ(fitted_vector_uint32.width(), 4);
}

TEST_F(StorageFittedAttributeVectorTest, MakeFittedHelper) {
  auto fitted_vector_uint8_1 = opossum::make_fitted_attribute_vector(1, 1);
  auto fitted_vector_uint8_2 = opossum::make_fitted_attribute_vector(255, 1);
  auto fitted_vector_uint16_1 = opossum::make_fitted_attribute_vector(256, 1);
  auto fitted_vector_uint16_2 = opossum::make_fitted_attribute_vector(65535, 1);
  auto fitted_vector_uint32_1 = opossum::make_fitted_attribute_vector(65536, 1);
  auto fitted_vector_uint32_2 = opossum::make_fitted_attribute_vector(4294967295, 1);

  EXPECT_EQ(fitted_vector_uint8_1->width(), 1);
  EXPECT_EQ(fitted_vector_uint8_2->width(), 1);
  EXPECT_EQ(fitted_vector_uint16_1->width(), 2);
  EXPECT_EQ(fitted_vector_uint16_2->width(), 2);
  EXPECT_EQ(fitted_vector_uint32_1->width(), 4);
  EXPECT_EQ(fitted_vector_uint32_2->width(), 4);
  EXPECT_THROW(opossum::make_fitted_attribute_vector(4294967296, opossum::ValueID{1}), std::exception);
}

