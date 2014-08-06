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

import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_VALUE;
import static info.archinnov.achilles.internal.cql.TypeMapper.toCQLDataType;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static java.lang.String.format;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.schemabuilder.Create;
import info.archinnov.achilles.schemabuilder.SchemaBuilder;

public class TableCreator {
    static final String ACHILLES_DDL_SCRIPT = "ACHILLES_DDL_SCRIPT";

    public static final String TABLE_PATTERN = "[a-zA-Z0-9_]+";
    private static final Logger log = LoggerFactory.getLogger(TableCreator.class);
    private static final Logger DML_LOG = LoggerFactory.getLogger(ACHILLES_DDL_SCRIPT);

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

    public void createTableForEntity(Session session, EntityMeta entityMeta, ConfigurationContext configContext) {

        log.debug("Create table for entity {}", entityMeta);

        String tableName = entityMeta.config().getTableName();
        if (configContext.isForceColumnFamilyCreation()) {
            log.debug("Force creation of table for entityMeta {}", entityMeta.getClassName());
            createTableForEntity(session, entityMeta);
        } else {
            throw new AchillesInvalidTableException(format("The required table '%s' does not exist for entity '%s'", tableName, entityMeta.getClassName()));
        }
    }

    private void createTableForEntity(Session session, EntityMeta entityMeta) {
        log.debug("Creating table for entityMeta {}", entityMeta.getClassName());
        if (entityMeta.structure().isClusteredCounter()) {
            createTableForClusteredCounter(session, entityMeta);
        } else {
            createTable(session, entityMeta);
        }
    }

    public void createTableForCounter(Session session, ConfigurationContext configContext) {
        log.debug("Create table for Achilles counters");

        if (configContext.isForceColumnFamilyCreation()) {
            final String createTable = SchemaBuilder.createTable(ACHILLES_COUNTER_TABLE)
                    .addPartitionKey(ACHILLES_COUNTER_FQCN, DataType.text())
                    .addPartitionKey(ACHILLES_COUNTER_PRIMARY_KEY, DataType.text())
                    .addClusteringKey(ACHILLES_COUNTER_PROPERTY_NAME, DataType.text())
                    .addColumn(ACHILLES_COUNTER_VALUE, DataType.counter())
                    .withOptions().comment("Create default Achilles counter table \"" + ACHILLES_COUNTER_TABLE + "\"")
                    .build();

            session.execute(createTable);
        } else {
            throw new AchillesInvalidTableException("The required generic table '" + ACHILLES_COUNTER_TABLE + "' does not exist");
        }
    }

    private void createTable(Session session, EntityMeta entityMeta) {
        String tableName = TableNameNormalizer.normalizerAndValidateColumnFamilyName(entityMeta.config().getTableName());
        final List<String> indexes = new LinkedList<>();
        final Create createTable = SchemaBuilder.createTable(tableName);
        for (PropertyMeta pm : entityMeta.getAllMetasExceptIdAndCounters()) {
            String cql3ColumnName = pm.getCQL3ColumnName();
            Class<?> keyClass = pm.forTableCreation().getKeyClassForTableCreationAndValidation();
            Class<?> valueClass = pm.forTableCreation().getValueClassForTableCreationAndValidation();
            final boolean staticColumn = pm.structure().isStaticColumn();
            switch (pm.type()) {
                case SIMPLE:
                    createTable.addColumn(cql3ColumnName, toCQLDataType(valueClass), staticColumn);
                    if (pm.structure().isIndexed()) {
                        indexes.add(pm.forTableCreation().createNewIndexScript(tableName));
                    }
                    break;
                case LIST:
                    createTable.addColumn(cql3ColumnName, DataType.list(toCQLDataType(valueClass)), staticColumn);
                    break;
                case SET:
                    createTable.addColumn(cql3ColumnName, DataType.set(toCQLDataType(valueClass)), staticColumn);
                    break;
                case MAP:
                    createTable.addColumn(cql3ColumnName, DataType.map(toCQLDataType(keyClass), toCQLDataType(valueClass)), staticColumn);
                    break;
                default:
                    break;
            }
        }
        final PropertyMeta idMeta = entityMeta.getIdMeta();
        buildPrimaryKey(idMeta, createTable);
        final Create.Options tableOptions = createTable.withOptions();
        idMeta.forTableCreation().addClusteringOrder(tableOptions);
        final String tableComment = entityMeta.config().getTableComment();
        if (StringUtils.isNotBlank(tableComment)) {
            tableOptions.comment(tableComment);
        }

        final String createTableScript = tableOptions.build();
        session.execute(createTableScript);
        DML_LOG.debug(createTableScript);

        if (!indexes.isEmpty()) {
            for (String indexScript : indexes) {
                session.execute(indexScript);
                DML_LOG.debug(indexScript);
            }
        }

    }

    private void createTableForClusteredCounter(Session session, EntityMeta meta) {
        log.debug("Creating table for clustered counter entity {}", meta.getClassName());

        final Create createTable = SchemaBuilder.createTable(TableNameNormalizer.normalizerAndValidateColumnFamilyName(meta.config().getTableName()));

        PropertyMeta idMeta = meta.getIdMeta();
        buildPrimaryKey(idMeta, createTable);
        for (PropertyMeta counterMeta : meta.getAllCounterMetas()) {
            createTable.addColumn(counterMeta.getCQL3ColumnName(), DataType.counter(),counterMeta.structure().isStaticColumn());
        }
        final Create.Options tableOptions = createTable.withOptions();
        idMeta.forTableCreation().addClusteringOrder(tableOptions);
        tableOptions.comment(meta.config().getTableComment());

        final String createTableScript = tableOptions.build();
        session.execute(createTableScript);
        DML_LOG.debug(createTableScript);
    }

    private List<ClusteringOrder> buildPrimaryKey(PropertyMeta pm, Create createTable) {
        List<ClusteringOrder> clusteringOrders = new LinkedList<>();

        if (pm.structure().isEmbeddedId()) {
            pm.forTableCreation().addPartitionKeys(createTable);
            pm.forTableCreation().addClusteringKeys(createTable);
        } else {
            String cql3ColumnName = pm.getCQL3ColumnName();
            createTable.addPartitionKey(cql3ColumnName, toCQLDataType(pm.forTableCreation().getValueClassForTableCreationAndValidation()));
        }
        return clusteringOrders;
    }
}
