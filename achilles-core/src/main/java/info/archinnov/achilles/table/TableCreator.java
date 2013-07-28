package info.archinnov.achilles.table;

import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import java.util.Map;
import java.util.Map.Entry;

/**
 * TableCreator
 * 
 * @author DuyHai DOAN
 * 
 */
public abstract class TableCreator {
    public static final String TABLE_PATTERN = "[a-zA-Z0-9_]+";
    static final String ACHILLES_DDL_SCRIPT = "ACHILLES_DDL_SCRIPT";

    public void validateOrCreateTables(Map<Class<?>, EntityMeta> entityMetaMap,
            ConfigurationContext configContext, boolean hasCounter) {
        for (Entry<Class<?>, EntityMeta> entry : entityMetaMap.entrySet()) {
            validateOrCreateTableForEntity(entry.getValue(), configContext.isForceColumnFamilyCreation());
        }

        if (hasCounter) {
            validateOrCreateTableForCounter(configContext.isForceColumnFamilyCreation());
        }
    }

    protected abstract void validateOrCreateTableForEntity(EntityMeta entityMeta, boolean forceColumnFamilyCreation);

    protected abstract void validateOrCreateTableForCounter(boolean forceColumnFamilyCreation);

}
