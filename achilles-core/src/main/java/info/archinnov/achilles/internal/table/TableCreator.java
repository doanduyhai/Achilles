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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.IndexProperties;
import info.archinnov.achilles.internal.metadata.holder.InternalTimeUUID;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
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

    public void createTableForCounter(Session session, boolean forceColumnFamilyCreation) {

        log.debug("Create table for Achilles counters");

        if (forceColumnFamilyCreation) {
            TableBuilder builder = TableBuilder.createTable(CQL_COUNTER_TABLE);
            builder.addColumn(CQL_COUNTER_FQCN, String.class);
            builder.addColumn(CQL_COUNTER_PRIMARY_KEY, String.class);
            builder.addColumn(CQL_COUNTER_PROPERTY_NAME, String.class);
            builder.addColumn(CQL_COUNTER_VALUE, Counter.class);
            builder.addPartitionComponent(CQL_COUNTER_FQCN);
            builder.addPartitionComponent(CQL_COUNTER_PRIMARY_KEY);
            builder.addClusteringComponent(CQL_COUNTER_PROPERTY_NAME);

            builder.addComment("Create default Achilles counter table '" + CQL_COUNTER_TABLE + "'");

            session.execute(builder.generateDDLScript());
        } else {
            throw new AchillesInvalidTableException("The required generic table '" + CQL_COUNTER_TABLE + "' does not exist");
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

    private void createTable(Session session, EntityMeta entityMeta) {
        String tableName = entityMeta.getTableName();
        TableBuilder builder = TableBuilder.createTable(tableName);
        for (PropertyMeta pm : entityMeta.getAllMetasExceptIdAndCounters()) {
            String propertyName = pm.getPropertyName();
            Class<?> keyClass = pm.getKeyClass();
            Class<?> valueClass = pm.getValueClassForTableCreation();
            switch (pm.type()) {
                case SIMPLE:
                    builder.addColumn(propertyName, valueClass);
                    if (pm.isIndexed()) {
                        builder.addIndex(new IndexProperties(pm.getIndexProperties().getName(), propertyName));
                    }
                    break;
                case LIST:
                    builder.addList(propertyName, valueClass);
                    break;
                case SET:
                    builder.addSet(propertyName, valueClass);
                    break;
                case MAP:
                    builder.addMap(propertyName, keyClass, pm.getValueClass());
                    break;
                default:
                    break;
            }

        }
        buildPrimaryKey(entityMeta.getIdMeta(), builder);
        builder.addComment("Create table for entity '" + entityMeta.getClassName() + "'");
        session.execute(builder.generateDDLScript());
        if (builder.hasIndices()) {
            for (String indexScript : builder.generateIndices()) {
                session.execute(indexScript);
            }
        }

    }

    private void createTableForClusteredCounter(Session session, EntityMeta meta) {
        log.debug("Creating table for clustered counter entity {}", meta.getClassName());

        TableBuilder builder = TableBuilder.createCounterTable(meta.getTableName());
        PropertyMeta idMeta = meta.getIdMeta();
        buildPrimaryKey(idMeta, builder);
        for (PropertyMeta counterMeta : meta.getAllCounterMetas()) {
            builder.addColumn(counterMeta.getPropertyName(), Counter.class);
        }
        builder.addComment("Create table for clustered counter entity '" + meta.getClassName() + "'");

        session.execute(builder.generateDDLScript());

    }

    private void buildPrimaryKey(PropertyMeta pm, TableBuilder builder) {
        if (pm.isEmbeddedId()) {
            addPrimaryKeyComponents(pm, builder, true);
            addPrimaryKeyComponents(pm, builder, false);
        } else {
            String columnName = pm.getPropertyName();
            builder.addColumn(columnName, pm.getValueClassForTableCreation());
            builder.addPartitionComponent(columnName);
        }
    }

    private void addPrimaryKeyComponents(PropertyMeta pm, TableBuilder builder, boolean partitionKey) {
        List<String> componentNames;
        List<Class<?>> componentClasses;

        if (partitionKey) {
            componentNames = pm.getPartitionComponentNames();
            componentClasses = pm.getPartitionComponentClasses();
        } else {
            componentNames = pm.getClusteringComponentNames();
            componentClasses = pm.getClusteringComponentClasses();
            builder.setReversedClusteredComponent(pm.getReversedComponent());
        }
        for (int i = 0; i < componentNames.size(); i++) {
            String componentName = componentNames.get(i);
            Class<?> javaType = componentClasses.get(i);
            if (pm.isComponentTimeUUID(componentName)) {
                javaType = InternalTimeUUID.class;
            }

            builder.addColumn(componentName, javaType);
            if (partitionKey) {
                builder.addPartitionComponent(componentName);
            } else {
                builder.addClusteringComponent(componentName);
            }
        }
    }
}
