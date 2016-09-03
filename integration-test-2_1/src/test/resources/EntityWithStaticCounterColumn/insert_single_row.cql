UPDATE achilles_embedded.entity_static_counter SET count = count + ${count}, static_count = static_count + ${staticCount}
WHERE id = ${id} AND uuid = ${uuid};
