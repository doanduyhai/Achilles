UPDATE achilles_embedded.entity_complex_counters SET count = count + ${count},
static_count = static_count + ${staticCount},
codec_count = codec_count + ${codecCount}
WHERE id = ${id} AND uuid = ${uuid};
