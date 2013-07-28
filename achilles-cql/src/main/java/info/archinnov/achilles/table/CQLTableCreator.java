package info.archinnov.achilles.table;

import static info.archinnov.achilles.counter.AchillesCounter.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.validation.Validator;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public class CQLTableCreator extends TableCreator {
    private static final Logger log = LoggerFactory.getLogger(CQLTableCreator.class);

    private Session session;
    private String keyspaceName;
    private Cluster cluster;
    private Map<String, TableMetadata> tableMetas;

    private CQLTableValidator validator;

    public CQLTableCreator(Cluster cluster, Session session, String keyspaceName) {
        this.cluster = cluster;
        this.session = session;
        this.keyspaceName = keyspaceName;
        this.tableMetas = fetchTableMetaData();
        validator = new CQLTableValidator(cluster, keyspaceName);
    }

    @Override
    protected void validateOrCreateTableForEntity(EntityMeta entityMeta, boolean forceColumnFamilyCreation) {
        String tableName = entityMeta.getTableName().toLowerCase();
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
                throw new AchillesInvalidTableException("The required table '"
                        + tableName + "' does not exist for entity '" + entityMeta.getClassName()
                        + "'");
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
                throw new AchillesInvalidTableException("The required generic table '" + CQL_COUNTER_TABLE
                        + "' does not exist");
            }
        }

    }

    private void createTableForEntity(EntityMeta entityMeta) {
        log.debug("Creating table for entityMeta {}", entityMeta.getClassName());
        String tableName = entityMeta.getTableName();

        if (entityMeta.isClusteredCounter())
        {
            createTableForClusteredCounter(entityMeta);
        }
        else
        {
            createTable(entityMeta, tableName);
        }
    }

    private void createTable(EntityMeta entityMeta, String tableName) {
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
    }

    private void createTableForClusteredCounter(EntityMeta meta) {
        PropertyMeta<?, ?> pm = meta.getFirstMeta();

        log.debug("Creating table for counter property {} for entity {}", pm.getPropertyName(), meta.getClassName());

        CQLTableBuilder builder = CQLTableBuilder.createCounterTable(meta.getTableName());
        PropertyMeta<?, ?> idMeta = meta.getIdMeta();
        buildPrimaryKeys(idMeta, builder);
        builder.addColumn(pm.getPropertyName(), pm.getValueClass());

        builder.addComment("Create table for counter property '" + pm.getPropertyName() + "' of entity '"
                + meta.getClassName() + "'");

        session.execute(builder.generateDDLScript());

    }

    private Map<String, TableMetadata> fetchTableMetaData()
    {
        Map<String, TableMetadata> tableMetas = new HashMap<String, TableMetadata>();
        KeyspaceMetadata keyspaceMeta = cluster.getMetadata().getKeyspace(keyspaceName);

        Validator.validateTableTrue(keyspaceMeta != null, "Keyspace '" + keyspaceName
                + "' doest not exist or cannot be found");

        for (TableMetadata tableMeta : keyspaceMeta.getTables())
        {
            tableMetas.put(tableMeta.getName(), tableMeta);
        }
        return tableMetas;
    }

    private void buildPrimaryKeys(PropertyMeta<?, ?> pm, CQLTableBuilder builder) {
        if (pm.isEmbeddedId())
        {
            List<String> componentNames = pm.getComponentNames();
            List<Class<?>> componentClasses = pm.getComponentClasses();
            for (int i = 0; i < componentNames.size(); i++)
            {
                String componentName = componentNames.get(i);
                builder.addColumn(componentName, componentClasses.get(i));
                builder.addPrimaryKey(componentName);
            }
            System.out.println(" builder.primaryKeys = " + builder.toString());
        }
        else
        {
            String columnName = pm.getPropertyName();
            builder.addColumn(columnName, pm.getValueClass());
            builder.addPrimaryKey(columnName);
        }
    }
}
