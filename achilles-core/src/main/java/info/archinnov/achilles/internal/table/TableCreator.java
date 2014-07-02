/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.table;

import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLDataType;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.SchemaBuilder.createIndex;
import static org.apache.commons.lang.StringUtils.isBlank;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.InternalTimeUUID;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.schemabuilder.Create;
import info.archinnov.achilles.schemabuilder.SchemaBuilder;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.Counter;

public class TableCreator {
    private static final Logger log = LoggerFactory.getLogger(TableCreator.class);

    public static final String TABLE_PATTERN = "[a-zA-Z0-9_]+";
    static final String ACHILLES_DDL_SCRIPT = "ACHILLES_DDL_SCRIPT";

    public Map<String, TableMetadata> fetchTableMetaData(KeyspaceMetadata keyspaceMeta, String keyspaceName) {

        log.debug("Fetch existing table meta data from Cassandra");

        Map<String, TableMetadata> tableMetas = new HashMap<>();

        Validator.validateTableTrue(keyspaceMeta != null, "Keyspace '%s' doest not exist or cannot be found",
                keyspaceName);

        for (TableMetadata tableMeta : keyspaceMeta.getTables()) {
            tableMetas.put(tableMeta.getName(), tableMeta);
        }
        return tableMetas;
    }

    public void createTableForEntity(Session session, EntityMeta entityMeta, boolean forceColumnFamilyCreation) {

        log.debug("Create table for entity {}", entityMeta);

        String tableName = entityMeta.getTableName().toLowerCase();
        if (forceColumnFamilyCreation) {
            log.debug("Force creation of table for entityMeta {}", entityMeta.getClassName());
            createTableForEntity(session, entityMeta);
        } else {
            throw new AchillesInvalidTableException("The required table '" + tableName + "' does not exist for entity '" + entityMeta.getClassName() + "'");
        }
    }

    private void createTableForEntity(Session session, EntityMeta entityMeta) {
        log.debug("Creating table for entityMeta {}", entityMeta.getClassName());
        if (entityMeta.isClusteredCounter()) {
            createTableForClusteredCounter(session, entityMeta);
        } else {
            createTable(session, entityMeta);
        }
    }

    public void createTableForCounter(Session session, boolean forceColumnFamilyCreation) {
        log.debug("Create table for Achilles counters");

        if (forceColumnFamilyCreation) {
            final String createTable = SchemaBuilder.createTable(CQL_COUNTER_TABLE)
                    .addPartitionKey(CQL_COUNTER_FQCN, DataType.text())
                    .addPartitionKey(CQL_COUNTER_PRIMARY_KEY, DataType.text())
                    .addClusteringKey(CQL_COUNTER_PROPERTY_NAME, DataType.text())
                    .addColumn(CQL_COUNTER_VALUE, DataType.counter())
                    .withOptions().comment("Create default Achilles counter table \"" + CQL_COUNTER_TABLE + "\"")
                    .build();

            session.execute(createTable);
        } else {
            throw new AchillesInvalidTableException("The required generic table '" + CQL_COUNTER_TABLE + "' does not exist");
        }
    }

    private void createTable(Session session, EntityMeta entityMeta) {
        String tableName = TableNameNormalizer.normalizerAndValidateColumnFamilyName(entityMeta.getTableName());
        final List<String> indexes = new LinkedList<>();
        final Create createTable = SchemaBuilder.createTable(tableName);
        for (PropertyMeta pm : entityMeta.getAllMetasExceptIdAndCounters()) {
            String propertyName = pm.getCQL3PropertyName();
            Class<?> keyClass = pm.getKeyClass();
            Class<?> valueClass = pm.getValueClassForTableCreation();
            switch (pm.type()) {
                case SIMPLE:
                    createTable.addColumn(propertyName, toCQLDataType(valueClass));
                    if (pm.isIndexed()) {
                        final String optionalIndexName = pm.getIndexProperties().getIndexName();
                        final String indexName = isBlank(optionalIndexName) ? tableName + "_" + propertyName : optionalIndexName;
                        indexes.add(createIndex(indexName).onTable(tableName).andColumn(propertyName));
                    }
                    break;
                case LIST:
                    createTable.addColumn(propertyName, DataType.list(toCQLDataType(valueClass)));
                    break;
                case SET:
                    createTable.addColumn(propertyName, DataType.set(toCQLDataType(valueClass)));
                    break;
                case MAP:
                    createTable.addColumn(propertyName, DataType.map(toCQLDataType(keyClass), toCQLDataType(valueClass)));
                    break;
                default:
                    break;
            }
        }
        final PropertyMeta idMeta = entityMeta.getIdMeta();
        buildPrimaryKey(idMeta, createTable);
        final Create.Options tableOptions = createTable.withOptions();
        addClusteringOrder(idMeta, tableOptions);
        tableOptions.comment("Create table for entity \"" + entityMeta.getClassName() + "\"");

        session.execute(tableOptions.build());
        if (!indexes.isEmpty()) {
            for (String indexScript : indexes) {
                session.execute(indexScript);
            }
        }

    }

    private void createTableForClusteredCounter(Session session, EntityMeta meta) {
        log.debug("Creating table for clustered counter entity {}", meta.getClassName());

        final Create createTable = SchemaBuilder.createTable(TableNameNormalizer.normalizerAndValidateColumnFamilyName(meta.getTableName()));

        PropertyMeta idMeta = meta.getIdMeta();
        buildPrimaryKey(idMeta, createTable);
        for (PropertyMeta counterMeta : meta.getAllCounterMetas()) {
            createTable.addColumn(counterMeta.getCQL3PropertyName(),DataType.counter());
        }
        final Create.Options tableOptions = createTable.withOptions();
        addClusteringOrder(idMeta, tableOptions);
        tableOptions.comment("Create table for clustered counter entity \"" + meta.getClassName() + "\"");

        session.execute(tableOptions.build());

    }

    private void addClusteringOrder(PropertyMeta idMeta, Create.Options tableOptions) {
        if (idMeta.isClustered()) {
            final List<ClusteringOrder> clusteringOrders = idMeta.getClusteringOrders();
            tableOptions.clusteringOrder(clusteringOrders.toArray(new ClusteringOrder[clusteringOrders.size()]));
        }
    }

    private List<ClusteringOrder> buildPrimaryKey(PropertyMeta pm, Create createTable) {
        List<ClusteringOrder> clusteringOrders = new LinkedList<>();

        if (pm.isEmbeddedId()) {
            addPartitionKeys(pm, createTable);
            addClusteringKeys(pm, createTable);
        } else {
            String columnName = pm.getPropertyName();
            createTable.addPartitionKey(columnName, toCQLDataType(pm.getValueClassForTableCreation()));
        }
        return clusteringOrders;
    }

    private void addPartitionKeys(PropertyMeta pm, Create createTable) {
        List<String> componentNames = pm.getPartitionComponentNames();
        List<Class<?>> componentClasses = pm.getPartitionComponentClasses();
        for (int i = 0; i < componentNames.size(); i++) {
            String componentName = componentNames.get(i);
            Class<?> javaType = pm.isPrimaryKeyTimeUUID(componentName) ? InternalTimeUUID.class:componentClasses.get(i);
            createTable.addPartitionKey(componentName,toCQLDataType(javaType));
        }
    }

    private void addClusteringKeys(PropertyMeta pm, Create createTable) {
        List<String> componentNames = pm.getClusteringComponentNames();
        List<Class<?>> componentClasses = pm.getClusteringComponentClasses();
        for (int i = 0; i < componentNames.size(); i++) {
            String componentName = componentNames.get(i);
            Class<?> javaType = pm.isPrimaryKeyTimeUUID(componentName) ? InternalTimeUUID.class:componentClasses.get(i);
            createTable.addClusteringKey(componentName, toCQLDataType(javaType));

        }
    }
}
