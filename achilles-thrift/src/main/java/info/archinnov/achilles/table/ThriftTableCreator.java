package info.archinnov.achilles.table;

import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;
import info.archinnov.achilles.validation.Validator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftTableCreator
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftTableCreator extends TableCreator {
    private static final Logger log = LoggerFactory.getLogger(ThriftTableCreator.class);
    private Cluster cluster;
    private Keyspace keyspace;
    private ThriftColumnFamilyFactory columnFamilyFactory = new ThriftColumnFamilyFactory();
    private ThriftColumnFamilyValidator columnFamilyValidator = new ThriftColumnFamilyValidator();

    private List<ColumnFamilyDefinition> cfDefs;
    private Set<String> columnFamilyNames = new HashSet<String>();

    public ThriftTableCreator(Cluster cluster, Keyspace keyspace) {
        this.cluster = cluster;
        this.keyspace = keyspace;
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspace.getKeyspaceName());

        Validator.validateNotNull(keyspaceDef, "The keyspace '" + keyspace.getKeyspaceName()
                + "' provided by configuration does not exist");

        if (keyspaceDef != null && keyspaceDef.getCfDefs() != null) {
            cfDefs = keyspaceDef.getCfDefs();
        }

    }

    @Override
    protected void validateOrCreateTableForEntity(EntityMeta entityMeta, boolean forceTableCreation) {
        ColumnFamilyDefinition cfDef = this.discoverTable(entityMeta.getTableName());
        if (cfDef == null) {
            if (forceTableCreation) {
                log.debug("Force creation of column family for entityMeta {}", entityMeta.getClassName());

                createTable(entityMeta);
            } else {
                throw new AchillesInvalidColumnFamilyException("The required column family '"
                        + entityMeta.getTableName() + "' does not exist for entity '" + entityMeta.getClassName()
                        + "'");
            }
        } else {
            columnFamilyValidator.validateCFForEntity(cfDef, entityMeta);
        }
    }

    @Override
    protected void validateOrCreateTableForWideMap(EntityMeta meta, PropertyMeta<?, ?> pm, boolean forceTableCreation) {

        String entityName = meta.getClassName();
        String externalTableName = pm.getExternalTableName();
        Class<?> idClass = pm.getIdClass();

        ColumnFamilyDefinition cfDef = discoverTable(externalTableName);

        if (cfDef == null) {
            String propertyName = pm.getPropertyName();
            if (forceTableCreation) {
                log.debug("Force creation of column family for propertyMeta {}", propertyName);

                cfDef = columnFamilyFactory.createWideRowCF(keyspace.getKeyspaceName(), pm, idClass,
                        externalTableName, entityName);
                this.addTable(cfDef);
            } else {
                throw new AchillesInvalidColumnFamilyException("The required column family '" + externalTableName
                        + "' does not exist for field '" + pm.getPropertyName() + "' of entity '"
                        + entityName + "'");
            }
        } else {
            columnFamilyValidator.validateWideRowForProperty(cfDef, pm, externalTableName);
        }
    }

    @Override
    protected void validateOrCreateTableForCounter(boolean forceColumnFamilyCreation) {
        ColumnFamilyDefinition cfDef = this.discoverTable(AchillesCounter.THRIFT_COUNTER_CF);
        if (cfDef == null) {
            if (forceColumnFamilyCreation) {
                log.debug("Force creation of column family for counters");

                this.createCounterColumnFamily();
            } else {
                throw new AchillesInvalidColumnFamilyException("The required column family '"
                        + AchillesCounter.THRIFT_COUNTER_CF + "' does not exist");
            }
        } else {
            columnFamilyValidator.validateCounterCF(cfDef);
        }

    }

    protected ColumnFamilyDefinition discoverTable(String columnFamilyName) {
        log.debug("Start discovery of column family {}", columnFamilyName);
        for (ColumnFamilyDefinition cfDef : this.cfDefs) {
            if (StringUtils.equals(cfDef.getName(), columnFamilyName)) {
                log.debug("Existing column family {} found", columnFamilyName);
                return cfDef;
            }
        }
        return null;
    }

    protected void addTable(ColumnFamilyDefinition cfDef) {
        if (!columnFamilyNames.contains(cfDef.getName())) {
            columnFamilyNames.add(cfDef.getName());
            cluster.addColumnFamily(cfDef, true);
        }

    }

    protected void createTable(EntityMeta entityMeta) {
        log.debug("Creating column family for entityMeta {}", entityMeta.getClassName());
        String columnFamilyName = entityMeta.getTableName();
        if (!columnFamilyNames.contains(columnFamilyName)) {
            ColumnFamilyDefinition cfDef;
            if (entityMeta.isClusteredEntity()) {
                cfDef = columnFamilyFactory.createClusteredEntityCF(this.keyspace.getKeyspaceName(), entityMeta);
            } else {
                cfDef = columnFamilyFactory.createEntityCF(entityMeta, this.keyspace.getKeyspaceName());

            }
            this.addTable(cfDef);
        }

    }

    private void createCounterColumnFamily() {
        log.debug("Creating generic counter column family");
        if (!columnFamilyNames.contains(AchillesCounter.THRIFT_COUNTER_CF)) {
            ColumnFamilyDefinition cfDef = columnFamilyFactory.createCounterCF(this.keyspace.getKeyspaceName());
            this.addTable(cfDef);
        }
    }
}
