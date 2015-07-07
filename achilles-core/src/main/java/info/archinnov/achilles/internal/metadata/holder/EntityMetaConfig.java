package info.archinnov.achilles.internal.metadata.holder;

import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.InsertStrategy;
import info.archinnov.achilles.internal.utils.Pair;

public class EntityMetaConfig extends EntityMetaView {
    protected EntityMetaConfig(EntityMeta meta) {
        super(meta);
    }

    public ConsistencyLevel getReadConsistencyLevel() {
        return meta.consistencyLevels.left;
    }

    public ConsistencyLevel getWriteConsistencyLevel() {
        return meta.consistencyLevels.right;
    }

    public Pair<ConsistencyLevel, ConsistencyLevel> getConsistencyLevels() {
        return meta.consistencyLevels;
    }

    public InsertStrategy getInsertStrategy() {
        return meta.insertStrategy;
    }

    public boolean isSchemaUpdateEnabled() {
        return meta.schemaUpdateEnabled;
    }

    public String getTableName() {return meta.tableName;}

    public String getKeyspaceName() {return meta.keyspaceName;}

    public String getQualifiedTableName() {
        return meta.qualifiedTableName;
    }

    public String getTableComment() {
        return meta.tableComment;
    }

}
