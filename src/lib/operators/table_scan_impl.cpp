#include "table_scan_impl.hpp"

namespace opossum {

void add_all_to_pos_list(std::shared_ptr<PosList> pos_list, ChunkID chunk_id,
                         const std::shared_ptr<const BaseAttributeVector> attribute_vector) {
  for (ChunkOffset index = 0; index < attribute_vector->size(); ++index) {
    pos_list->emplace_back(RowID{chunk_id, index});
  }
}

}  // namespace opossum
