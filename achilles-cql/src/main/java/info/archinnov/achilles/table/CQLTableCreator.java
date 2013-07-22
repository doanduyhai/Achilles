package info.archinnov.achilles.table;

import static info.archinnov.achilles.counter.AchillesCounter.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesInvalidColumnFamilyException;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.validation.Validator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class CQLTableCreator extends TableCreator {
    private static final Logger log = LoggerFactory.getLogger(CQLTableCreator.class);

    private static final String SINGLE_WIDE_MAP_KEY = "wide_map_key";

    private Session session;
    private String keyspaceName;
    private Cluster cluster;
    private Map<String, TableMetadata> tableMetas;
    private Set<String> tableNames = new HashSet<String>();

    private CQLTableValidator validator = new CQLTableValidator();

    public CQLTableCreator(Cluster cluster, Session session, String keyspaceName) {
        this.cluster = cluster;
        this.session = session;
        this.keyspaceName = keyspaceName;
        this.tableMetas = fetchTableMetaData();
    }

    @Override
    protected void validateOrCreateTableForEntity(EntityMeta entityMeta, boolean forceColumnFamilyCreation) {
        String tableName = entityMeta.getTableName();
        if (tableMetas.containsKey(tableName))
        {
            validator.validateForEntity(entityMeta, tableMetas.get(tableName));
        }
        else
        {
            if (forceColumnFamilyCreation)
            {
                log.debug("Force creation of table for entityMeta {}", entityMeta.getClassName());
                createTableForEntity(entityMeta);
            }
            else
            {
                throw new AchillesInvalidColumnFamilyException("The required table '"
                        + tableName + "' does not exist for entity '" + entityMeta.getClassName()
                        + "'");
            }
        }

    }

    @Override
    protected void validateOrCreateTableForWideMap(EntityMeta meta, PropertyMeta<?, ?> pm,
            boolean forceColumnFamilyCreation) {

        String entityName = meta.getClassName();
        String externalTableName = pm.getExternalTableName();

        if (tableMetas.containsKey(externalTableName))
        {
            validator.validateForWideMap(meta, pm, tableMetas.get(externalTableName));
        }
        else
        {
            String propertyName = pm.getPropertyName();
            if (forceColumnFamilyCreation)
            {
                log.debug("Force creation of column family for propertyMeta {}", pm.getPropertyName());
                createTableForWideMap(meta, pm);
            }
            else
            {
                throw new AchillesInvalidColumnFamilyException("The required table '" + externalTableName
                        + "' does not exist for field '" + propertyName + "' of entity '"
                        + entityName + "'");
            }
        }
    }

    @Override
    protected void validateOrCreateTableForCounter(boolean forceColumnFamilyCreation) {
        if (tableMetas.containsKey(CQL_COUNTER_TABLE))
        {
            validator.validateAchillesCounter();
        }
        else
        {
            if (forceColumnFamilyCreation)
            {
                CQLTableBuilder builder = CQLTableBuilder.createTable(CQL_COUNTER_TABLE);
                builder.addColumn(CQL_COUNTER_FQCN, String.class);
                builder.addColumn(CQL_COUNTER_PRIMARY_KEY, String.class);
                builder.addColumn(CQL_COUNTER_PROPERTY_NAME, String.class);
                builder.addColumn(CQL_COUNTER_VALUE, Counter.class);
                builder.addPrimaryKeys(Arrays
                        .asList(CQL_COUNTER_FQCN, CQL_COUNTER_PRIMARY_KEY, CQL_COUNTER_PROPERTY_NAME));

                builder.addComment("Create default Achilles counter table '" + CQL_COUNTER_TABLE + "'");

                session.execute(builder.generateDDLScript());
            }
            else
            {
                throw new AchillesInvalidColumnFamilyException("The required generic table '" + CQL_COUNTER_TABLE
                        + "' does not exist");
            }
        }

    }

    private void createTableForEntity(EntityMeta entityMeta) {
        log.debug("Creating table for entityMeta {}", entityMeta.getClassName());
        String tableName = entityMeta.getTableName();
        if (!tableNames.contains(tableName))
        {
            CQLTableBuilder builder = CQLTableBuilder.createTable(tableName);
            for (PropertyMeta<?, ?> pm : entityMeta.getAllMetasExceptIdMeta())
            {
                String propertyName = pm.getPropertyName();
                Class<?> keyClass = pm.getKeyClass();
                Class<?> valueClass = pm.getValueClass();
                switch (pm.type())
                {
                    case SIMPLE:
                    case LAZY_SIMPLE:
                        builder.addColumn(propertyName, valueClass);
                        break;
                    case LIST:
                    case LAZY_LIST:
                        builder.addList(propertyName, valueClass);
                        break;
                    case SET:
                    case LAZY_SET:
                        builder.addSet(propertyName, valueClass);
                        break;
                    case MAP:
                    case LAZY_MAP:
                        builder.addMap(propertyName, keyClass, pm.getValueClass());
                        break;
                    case JOIN_SIMPLE:
                        builder.addColumn(propertyName, pm.joinIdMeta().getValueClass());
                        break;
                    case JOIN_LIST:
                        builder.addList(propertyName, pm.joinIdMeta().getValueClass());
                        break;
                    case JOIN_SET:
                        builder.addSet(propertyName, pm.joinIdMeta().getValueClass());
                        break;
                    case JOIN_MAP:
                        builder.addMap(propertyName, keyClass, pm.joinIdMeta().getValueClass());
                        break;
                    default:
                        break;
                }
            }

            buildPrimaryKeys(entityMeta.getIdMeta(), builder);
            builder.addComment("Create table for entity '" + entityMeta.getClassName() + "'");

            session.execute(builder.generateDDLScript());

            tableNames.add(tableName);
        }
    }

    private void createTableForWideMap(EntityMeta meta, PropertyMeta<?, ?> pm) {
        log.debug("Creating table for wide map {} for entity {}", pm.getPropertyName(), meta.getClassName());

        String externalTableName = pm.getExternalTableName();
        if (!tableNames.contains(externalTableName))
        {

            CQLTableBuilder builder;
            if (pm.isCounter())
            {
                builder = CQLTableBuilder.createCounterTable(externalTableName);
            }
            else
            {
                builder = CQLTableBuilder.createTable(externalTableName);
            }
            PropertyMeta<?, ?> idMeta = meta.getIdMeta();
            buildPrimaryKeys(idMeta, builder);
            buildPrimaryKeys(pm, builder);

            if (pm.isJoin())
            {
                builder.addColumn(pm.getPropertyName(), pm.joinIdMeta().getValueClass());
            }
            else
            {
                builder.addColumn(pm.getPropertyName(), pm.getValueClass());
            }

            builder.addComment("Create table for wide map property '" + pm.getPropertyName() + "' of entity '"
                    + meta.getClassName() + "'");

            session.execute(builder.generateDDLScript());

            tableNames.add(externalTableName);
        }
    }

    private Map<String, TableMetadata> fetchTableMetaData()
    {
        Map<String, TableMetadata> tableMetas = new HashMap<String, TableMetadata>();
        KeyspaceMetadata keyspaceMeta = cluster.getMetadata().getKeyspace(keyspaceName);

        Validator.validateNotNull(keyspaceMeta, "Keyspace '" + keyspaceName + "' doest not exist or cannot be found");

        for (TableMetadata tableMeta : keyspaceMeta.getTables())
        {
            tableMetas.put(tableMeta.getName(), tableMeta);
        }
        return tableMetas;
    }

    private void buildPrimaryKeys(PropertyMeta<?, ?> pm, CQLTableBuilder builder) {
        if (pm.isCompound())
        {
            List<String> componentNames = pm.getComponentNames();
            List<Class<?>> componentClasses = pm.getComponentClasses();
            for (int i = 0; i < componentNames.size(); i++)
            {
                String componentName = componentNames.get(i);
                builder.addColumn(componentName, componentClasses.get(i));
                builder.addPrimaryKey(componentName);
            }
        }
        else if (pm.isWideMap())
        {
            builder.addColumn(SINGLE_WIDE_MAP_KEY, pm.getKeyClass());
            builder.addPrimaryKey(SINGLE_WIDE_MAP_KEY);
        }
        else
        {
            String columnName = pm.getPropertyName();
            builder.addColumn(columnName, pm.getValueClass());
            builder.addPrimaryKey(columnName);
        }
    }
}
