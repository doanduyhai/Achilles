package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.schemabuilder.Create;

import java.util.List;

public class EntityMetaSliceQuerySupport extends EntityMetaView {

    protected EntityMetaSliceQuerySupport(EntityMeta meta) {
        super(meta);
    }

    public void validatePartitionComponents(Object...partitionComponents) {
        meta.getIdMeta().forSliceQuery().validatePartitionComponents(partitionComponents);
    }

    public void validatePartitionComponentsIn(Object...partitionComponents) {
        meta.getIdMeta().forSliceQuery().validatePartitionComponentsIn(partitionComponents);
    }

    public void validateClusteringComponents(Object...clusteringComponents) {
        meta.getIdMeta().forSliceQuery().validateClusteringComponents(clusteringComponents);
    }

    public void validateClusteringComponentsIn(Object...clusteringComponents) {
        meta.getIdMeta().forSliceQuery().validateClusteringComponentsIn(clusteringComponents);
    }

    public List<String> getPartitionKeysName(int size) {
        return meta.getIdMeta().forSliceQuery().getPartitionKeysName(size);
    }

    public String getLastPartitionKeyName() {
        return meta.getIdMeta().forSliceQuery().getLastPartitionKeyName();
    }

    public List<String> getClusteringKeysName(int size) {
        return meta.getIdMeta().forSliceQuery().getClusteringKeysName(size);
    }

    public String getLastClusteringKeyName() {
        return meta.getIdMeta().forSliceQuery().getLastClusteringKeyName();
    }

    public int getPartitionKeysSize() {
        return meta.getIdMeta().forSliceQuery().getPartitionKeysSize();
    }

    public int getClusteringKeysSize() {
        return meta.getIdMeta().forSliceQuery().getClusteringKeysSize();
    }

    public Create.Options.ClusteringOrder getClusteringOrderForSliceQuery() {
        return meta.getIdMeta().forSliceQuery().getClusteringOrder();
    }




}
