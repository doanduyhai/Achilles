package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.type.ConsistencyLevel;

public class PropertyMetaConfig extends PropertyMetaView {

    protected PropertyMetaConfig(PropertyMeta meta) {
        super(meta);
    }

    public <T> Class<T> getCQL3ValueType() {
        return meta.getCql3ValueClass();
    }

    public <T> Class<T> getCQL3KeyType() {
        return meta.getCql3KeyClass();
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return meta.getConsistencyLevels() != null ? meta.getConsistencyLevels().left : null;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return meta.getConsistencyLevels() != null ? meta.getConsistencyLevels().right : null;
    }

}
