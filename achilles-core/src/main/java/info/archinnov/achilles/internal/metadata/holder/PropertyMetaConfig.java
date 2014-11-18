package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.type.ConsistencyLevel;

public class PropertyMetaConfig extends PropertyMetaView {

    protected PropertyMetaConfig(PropertyMeta meta) {
        super(meta);
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return meta.getConsistencyLevels() != null ? meta.getConsistencyLevels().left : null;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return meta.getConsistencyLevels() != null ? meta.getConsistencyLevels().right : null;
    }

}
