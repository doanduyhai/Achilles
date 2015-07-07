package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.type.IndexCondition;
import info.archinnov.achilles.options.Options.LWTCondition;

import java.util.List;

public class EntityMetaTranscoder extends EntityMetaView {
    protected EntityMetaTranscoder(EntityMeta meta) {
        super(meta);
    }

    public Object encodeCasConditionValue(LWTCondition LWTCondition) {
        Object rawValue = LWTCondition.getValue();
        final String columnName = LWTCondition.getColumnName();
        Object encodedValue = encodeValueForProperty(columnName, rawValue);
        LWTCondition.encodedValue(encodedValue);
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
            final PropertyMeta propertyMeta = findPropertyMetaByCQLName(columnName);
            encodedValue = propertyMeta.forTranscoding().encodeToCassandra(rawValue);
        }
        return encodedValue;
    }

    PropertyMeta findPropertyMetaByCQLName(String cqlName) {
        for (PropertyMeta propertyMeta : meta.getAllMetasExceptCounters()) {
            if (propertyMeta.type() == PropertyType.COMPOUND_PRIMARY_KEY) {
                for (PropertyMeta partitionMeta : propertyMeta.getCompoundPKProperties().getPartitionComponents().getPropertyMetas()) {
                    if (partitionMeta.getCQLColumnName().equals(cqlName)) {
                        return partitionMeta;
                    }
                }
                for (PropertyMeta clusteringMeta : propertyMeta.getCompoundPKProperties().getClusteringComponents().getPropertyMetas()) {
                    if (clusteringMeta.getCQLColumnName().equals(cqlName)) {
                        return clusteringMeta;
                    }
                }
            } else {
                if (propertyMeta.getCQLColumnName().equals(cqlName)) {
                    return propertyMeta;
                }
            }
        }
        throw new AchillesException(String.format("Cannot find matching property meta for the cql field %s", cqlName));
    }
}
