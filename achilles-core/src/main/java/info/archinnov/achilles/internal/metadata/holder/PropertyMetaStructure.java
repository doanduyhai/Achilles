package info.archinnov.achilles.internal.metadata.holder;


public class PropertyMetaStructure extends PropertyMetaView {

    protected PropertyMetaStructure(PropertyMeta meta) {
        super(meta);
    }

    public boolean isEmbeddedId() {
        return meta.type().isEmbeddedId();
    }

    public boolean isClustered() {
        if (isEmbeddedId()) {
            return meta.getEmbeddedIdProperties().getClusteringComponents().isClustered();
        }
        return false;
    }

    public boolean isStaticColumn() {
        return meta.isStaticColumn();
    }

    public boolean isCounter() {
        return meta.type().isCounter();
    }

    public boolean isIndexed() {
        return meta.getIndexProperties() != null;
    }

    public boolean isCollectionAndMap() {
        return meta.type().isCollectionAndMap();
    }

    public boolean isTimeUUID() {
        return meta.isTimeUUID();
    }

    public <T> Class<T> getCQL3ValueType() {
        return meta.getCql3ValueClass();
    }

    public <T> Class<T> getCQL3KeyType() {
        return meta.getCql3KeyClass();
    }

}
