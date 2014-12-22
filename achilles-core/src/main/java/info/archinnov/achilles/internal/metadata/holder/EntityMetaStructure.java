package info.archinnov.achilles.internal.metadata.holder;

public class EntityMetaStructure extends EntityMetaView {
    protected EntityMetaStructure(EntityMeta meta) {
        super(meta);
    }

    public boolean hasEmbeddedId() {
        return meta.idMeta.structure().isCompoundPK();
    }

    public boolean isEmbeddedId() {
        return meta.idMeta.structure().isCompoundPK();
    }

    public boolean isClusteredEntity() {
        return meta.clusteredEntity;
    }

    public boolean isClusteredCounter() {
        return meta.clusteredCounter;
    }

    public boolean isValueless() {
        return meta.getPropertyMetas().size() == 1;
    }

    public boolean hasOnlyStaticColumns() {
        return meta.hasOnlyStaticColumns;
    }

}
