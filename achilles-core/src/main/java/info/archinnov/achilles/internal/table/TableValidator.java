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

import static com.datastax.driver.core.DataType.counter;
import static com.datastax.driver.core.DataType.text;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_VALUE;

import java.util.Collection;

import info.archinnov.achilles.internal.metadata.holder.PropertyMetaTableValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType.Name;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;

public class TableValidator {

    private static final Logger log = LoggerFactory.getLogger(TableValidator.class);

    private ColumnMetaDataComparator columnMetaDataComparator = ColumnMetaDataComparator.Singleton.INSTANCE.get();

    public void validateForEntity(EntityMeta entityMeta, AbstractTableMetadata tableMetadata, ConfigurationContext configContext) {
        log.debug("Validate existing table {} for {}", tableMetadata.getName(), entityMeta);

        // Primary key Validation
        PropertyMeta idMeta = entityMeta.getIdMeta();
        final PropertyMetaTableValidator primaryKeyValidator = idMeta.forTableValidation();
        if (entityMeta.structure().isCompoundPK()) {
            primaryKeyValidator.validatePrimaryKeyComponents(tableMetadata, true);
            primaryKeyValidator.validatePrimaryKeyComponents(tableMetadata, false);
        } else {
            primaryKeyValidator.validateColumn(tableMetadata, entityMeta, configContext);
        }

        // Other fields validation
        for (PropertyMeta pm : entityMeta.getAllMetasExceptIdAndCounters()) {
            final PropertyMetaTableValidator columnValidator = pm.forTableValidation();
            switch (pm.type()) {
                case SIMPLE:
                    columnValidator.validateColumn(tableMetadata, entityMeta, configContext);
                    break;
                case LIST:
                case SET:
                case MAP:
                    columnValidator.validateCollectionAndMapColumn(tableMetadata, entityMeta);
                    break;
                default:
                    break;
            }
        }

        // Clustered Counter fields validation
        if (entityMeta.structure().isClusteredCounter()) {
            for (PropertyMeta counterMeta : entityMeta.getAllCounterMetas()) {
                counterMeta.forTableValidation().validateClusteredCounterColumn(tableMetadata, entityMeta);
            }
        }
    }

    public void validateAchillesCounter(KeyspaceMetadata keyspaceMetaData, String keyspaceName) {
        log.debug("Validate existing Achilles Counter table");
        Name textTypeName = text().getName();
        Name counterTypeName = counter().getName();

        TableMetadata tableMetaData = keyspaceMetaData.getTable(ACHILLES_COUNTER_TABLE);
        Validator.validateTableTrue(tableMetaData != null, "Cannot find table '%s' from keyspace '%s'",ACHILLES_COUNTER_TABLE, keyspaceName);

        ColumnMetadata fqcnColumn = tableMetaData.getColumn(ACHILLES_COUNTER_FQCN);
        Validator.validateTableTrue(fqcnColumn != null, "Cannot find column '%s' from table '%s'", ACHILLES_COUNTER_FQCN,ACHILLES_COUNTER_TABLE);
        Validator.validateTableTrue(fqcnColumn.getType().getName() == textTypeName,
                "Column '%s' of type '%s' should be of type '%s'", ACHILLES_COUNTER_FQCN, fqcnColumn.getType().getName(),
                textTypeName);
        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), fqcnColumn),
                "Column '%s' of table '%s' should be a partition key component", ACHILLES_COUNTER_FQCN, ACHILLES_COUNTER_TABLE);


        ColumnMetadata pkColumn = tableMetaData.getColumn(ACHILLES_COUNTER_PRIMARY_KEY);
        Validator.validateTableTrue(pkColumn != null, "Cannot find column '%s' from table '%s'",ACHILLES_COUNTER_PRIMARY_KEY, ACHILLES_COUNTER_TABLE);
        Validator.validateTableTrue(pkColumn.getType().getName() == textTypeName,
                "Column '%s' of type '%s' should be of type '%s'", ACHILLES_COUNTER_PRIMARY_KEY, pkColumn.getType()
                        .getName(), textTypeName);
        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getPartitionKey(), pkColumn),
                "Column '%s' of table '%s' should be a partition key component", ACHILLES_COUNTER_PRIMARY_KEY,ACHILLES_COUNTER_TABLE);


        ColumnMetadata propertyNameColumn = tableMetaData.getColumn(ACHILLES_COUNTER_PROPERTY_NAME);
        Validator.validateTableTrue(propertyNameColumn != null, "Cannot find column '%s' from table '%s'",ACHILLES_COUNTER_PROPERTY_NAME, ACHILLES_COUNTER_TABLE);
        Validator.validateTableTrue(propertyNameColumn.getType().getName() == textTypeName,
                "Column '%s' of type '%s' should be of type '%s'", ACHILLES_COUNTER_PROPERTY_NAME, propertyNameColumn
                        .getType().getName(), textTypeName);
        Validator.validateBeanMappingTrue(hasColumnMeta(tableMetaData.getClusteringColumns(), propertyNameColumn),
                "Column '%s' of table '%s' should be a clustering key component", ACHILLES_COUNTER_PROPERTY_NAME,ACHILLES_COUNTER_TABLE);



        ColumnMetadata counterValueColumn = tableMetaData.getColumn(ACHILLES_COUNTER_VALUE);
        Validator.validateTableTrue(counterValueColumn != null, "Cannot find column '%s' from table '%s'",ACHILLES_COUNTER_VALUE, ACHILLES_COUNTER_TABLE);
        Validator.validateTableTrue(counterValueColumn.getType().getName() == counterTypeName,
                "Column '%s' of type '%s' should be of type '%s'", ACHILLES_COUNTER_VALUE, counterValueColumn.getType().getName(), counterTypeName);
    }



    private boolean hasColumnMeta(Collection<ColumnMetadata> columnMetadatas, ColumnMetadata columnMetaToVerify) {
        boolean fqcnColumnMatches = false;
        for (ColumnMetadata columnMetadata : columnMetadatas) {
            fqcnColumnMatches = fqcnColumnMatches || columnMetaDataComparator.isEqual(columnMetaToVerify, columnMetadata);
        }
        return fqcnColumnMatches;
    }

    public static enum Singleton {
        INSTANCE;

        private final TableValidator instance = new TableValidator();

        public TableValidator get() {
            return instance;
        }
    }
}
