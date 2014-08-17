package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.type.Options.CASCondition;

import java.util.List;

public class EntityMetaTranscoder extends EntityMetaView {
    protected EntityMetaTranscoder(EntityMeta meta) {
        super(meta);
    }

    public Object encodeCasConditionValue(CASCondition CASCondition) {
        Object rawValue = CASCondition.getValue();
        final String columnName = CASCondition.getColumnName();
        Object encodedValue = encodeValueForProperty(columnName, rawValue);
        CASCondition.encodedValue(encodedValue);
        return encodedValue;
    }

    public Object encodeIndexConditionValue(IndexCondition indexCondition) {
        Object rawValue = indexCondition.getColumnValue();
        final String columnName = indexCondition.getColumnName();
        Object encodedValue = encodeValueForProperty(columnName, rawValue);
        indexCondition.encodedValue(encodedValue);
        return encodedValue;
    }

    public List<Object> encodePartitionComponents(List<Object> rawPartitionComponents) {
        return meta.getIdMeta().forTranscoding().encodePartitionComponents(rawPartitionComponents);
    }

    public List<Object> encodePartitionComponentsIN(List<Object> rawPartitionComponentsIN) {
        return meta.getIdMeta().forTranscoding().encodePartitionComponentsIN(rawPartitionComponentsIN);
    }

    public List<Object> encodeClusteringKeys(List<Object> rawClusteringKeys) {
        return meta.getIdMeta().forTranscoding().encodeClusteringKeys(rawClusteringKeys);
    }

    public List<Object> encodeClusteringKeysIN(List<Object> rawClusteringKeysIN) {
        return meta.getIdMeta().forTranscoding().encodeClusteringKeysIN(rawClusteringKeysIN);
    }

    private Object encodeValueForProperty(String columnName, Object rawValue) {
        Object encodedValue = rawValue;
        if (rawValue != null) {
            final PropertyMeta propertyMeta = findPropertyMetaByCQL3Name(columnName);
            encodedValue = propertyMeta.forTranscoding().encodeToCassandra(rawValue);
        }
        return encodedValue;
    }

    private PropertyMeta findPropertyMetaByCQL3Name(String cql3Name) {
        for (PropertyMeta propertyMeta : meta.getAllMetasExceptCounters()) {
            if (propertyMeta.getCQL3ColumnName().equals(cql3Name) || propertyMeta.getPropertyName().equals(cql3Name)) {
                return propertyMeta;
            }
        }
        throw new AchillesException(String.format("Cannot find matching property meta for the cql3 field %s", cql3Name));
    }
}
