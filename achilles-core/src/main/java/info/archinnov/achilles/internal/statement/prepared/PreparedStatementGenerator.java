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
package info.archinnov.achilles.internal.statement.prepared;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.decr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.collect.ImmutableMap.of;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DECR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DELETE;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.INCR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.SELECT;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.ACHILLES_COUNTER_VALUE;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.DELETE_ALL;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.SELECT_ALL;
import static info.archinnov.achilles.type.Options.CASCondition;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import info.archinnov.achilles.internal.metadata.holder.PropertyMetaStatementGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.base.Optional;
import info.archinnov.achilles.counter.AchillesCounter.CQLQueryType;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.query.slice.SliceQueryProperties;
import info.archinnov.achilles.type.Options;

public class PreparedStatementGenerator {
    private static final Logger log = LoggerFactory.getLogger(PreparedStatementGenerator.class);

    public PreparedStatement prepareInsert(Session session, EntityMeta entityMeta, List<PropertyMeta> pms, Options options) {
        log.trace("Generate prepared statement for INSERT on {}", entityMeta);
        PropertyMeta idMeta = entityMeta.getIdMeta();
        Insert insert = insertInto(entityMeta.config().getTableName());
        if (options.isIfNotExists()) {
            insert.ifNotExists();
        }

        idMeta.forStatementGeneration().generateInsertPrimaryKey(insert);

        for (PropertyMeta pm : pms) {
            String cql3ColumnName = pm.getCQL3ColumnName();
            insert.value(cql3ColumnName, bindMarker(cql3ColumnName));
        }

        final Insert.Options insertOptions = insert.using(ttl(bindMarker("ttl")));
        if (options.getTimestamp().isPresent()) {
            insertOptions.and(timestamp(bindMarker("timestamp")));
        }
        return session.prepare(insert.getQueryString());
    }

    public PreparedStatement prepareSelectField(Session session, EntityMeta entityMeta, PropertyMeta pm) {
        log.trace("Generate prepared statement for SELECT property {}", pm);

        PropertyMeta idMeta = entityMeta.getIdMeta();

        if (pm.structure().isCounter()) {
            throw new IllegalArgumentException(String.format("Cannot prepare statement for property '%s' of entity '%s' because it is a counter type",pm.getPropertyName(),entityMeta.getClassName()));
        } else {
            Selection select = pm.forStatementGeneration().prepareSelectField(select());
            Select from = select.from(entityMeta.config().getTableName());
            RegularStatement statement = idMeta.forStatementGeneration().generateWhereClauseForSelect(Optional.fromNullable(pm), from);
            return session.prepare(statement.getQueryString());
        }
    }

    public PreparedStatement prepareUpdateFields(Session session, EntityMeta entityMeta, List<PropertyMeta> pms, Options options) {

        log.trace("Generate prepared statement for UPDATE properties {}", pms);

        PropertyMeta idMeta = entityMeta.getIdMeta();
        Update update = update(entityMeta.config().getTableName());
        final Update.Conditions updateConditions = update.onlyIf();
        if (options.hasCasConditions()) {
            for (CASCondition CASCondition : options.getCASConditions()) {
                updateConditions.and(CASCondition.toClauseForPreparedStatement());
            }
        }

        Assignments assignments = null;
        boolean onlyStaticColumns = true;
        for (int i = 0; i < pms.size(); i++) {
            PropertyMeta pm = pms.get(i);

            if (!pm.structure().isStaticColumn()) {
                onlyStaticColumns = false;
            }
            if (i == 0) {
                assignments = pm.forStatementGeneration().prepareUpdateField(updateConditions);
            } else {
                assignments = pm.forStatementGeneration().prepareUpdateField(assignments);
            }
        }
        RegularStatement statement = prepareWhereClauseWithTTLForUpdate(idMeta, assignments, onlyStaticColumns, options);
        return session.prepare(statement.getQueryString());
    }

    public PreparedStatement prepareSelectAll(Session session, EntityMeta entityMeta) {
        log.trace("Generate prepared statement for SELECT of {}", entityMeta);

        PropertyMeta idMeta = entityMeta.getIdMeta();

        Selection select = select();

        for (PropertyMeta pm : entityMeta.forOperations().getColumnsMetaToLoad()) {
            select = pm.forStatementGeneration().prepareSelectField(select);
        }
        Select from = select.from(entityMeta.config().getTableName());

        Optional<PropertyMeta> staticMeta = Optional.absent();
        if (entityMeta.structure().hasOnlyStaticColumns()) {
            staticMeta = Optional.fromNullable(entityMeta.getAllMetasExceptId().get(0));
        }

        RegularStatement statement = idMeta.forStatementGeneration().generateWhereClauseForSelect(staticMeta, from);
        return session.prepare(statement.getQueryString());
    }

    public Map<CQLQueryType, PreparedStatement> prepareSimpleCounterQueryMap(Session session) {

        final String incr = update(ACHILLES_COUNTER_TABLE)
                .with(incr(ACHILLES_COUNTER_VALUE, bindMarker()))
                .where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
                .and(eq(ACHILLES_COUNTER_PRIMARY_KEY, bindMarker()))
                .and(eq(ACHILLES_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString();

        final String decr = update(ACHILLES_COUNTER_TABLE)
                .with(decr(ACHILLES_COUNTER_VALUE, bindMarker()))
                .where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
                .and(eq(ACHILLES_COUNTER_PRIMARY_KEY, bindMarker()))
                .and(eq(ACHILLES_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString();

        final String select = select(ACHILLES_COUNTER_VALUE).from(ACHILLES_COUNTER_TABLE)
                .where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
                .and(eq(ACHILLES_COUNTER_PRIMARY_KEY, bindMarker()))
                .and(eq(ACHILLES_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString();

        final String delete = delete().from(ACHILLES_COUNTER_TABLE)
                .where(eq(ACHILLES_COUNTER_FQCN, bindMarker()))
                .and(eq(ACHILLES_COUNTER_PRIMARY_KEY, bindMarker()))
                .and(eq(ACHILLES_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString();

        Map<CQLQueryType, PreparedStatement> counterPSMap = new HashMap<>();
        counterPSMap.put(INCR, session.prepare(incr));
        counterPSMap.put(DECR, session.prepare(decr));
        counterPSMap.put(SELECT, session.prepare(select));
        counterPSMap.put(DELETE, session.prepare(delete));

        return counterPSMap;
    }

    public Map<CQLQueryType, Map<String, PreparedStatement>> prepareClusteredCounterQueryMap(Session session, EntityMeta meta) {
        PropertyMeta idMeta = meta.getIdMeta();
        String tableName = meta.config().getTableName();

        Map<CQLQueryType, Map<String, PreparedStatement>> clusteredCounterPSMap = new HashMap<>();
        Map<String, PreparedStatement> incrStatementPerCounter = new HashMap<>();
        Map<String, PreparedStatement> decrStatementPerCounter = new HashMap<>();
        Map<String, PreparedStatement> selectStatementPerCounter = new HashMap<>();

        final PropertyMetaStatementGenerator statementGenerator = idMeta.forStatementGeneration();

        for (PropertyMeta counterMeta : meta.getAllCounterMetas()) {
            final String cql3ColumnName = counterMeta.getCQL3ColumnName();
            final boolean staticColumn = counterMeta.structure().isStaticColumn();

            RegularStatement incrementStatement = prepareWhereClauseForCounterUpdate(statementGenerator, update(tableName).with(incr(cql3ColumnName, bindMarker(cql3ColumnName))), staticColumn, noOptions());
            RegularStatement decrementStatement = prepareWhereClauseForCounterUpdate(statementGenerator, update(tableName).with(decr(cql3ColumnName, bindMarker(cql3ColumnName))), staticColumn, noOptions());

            RegularStatement selectStatement = statementGenerator.generateWhereClauseForSelect(Optional.fromNullable(counterMeta), select(cql3ColumnName).from(tableName));

            incrStatementPerCounter.put(cql3ColumnName, session.prepare(incrementStatement));
            decrStatementPerCounter.put(cql3ColumnName, session.prepare(decrementStatement));
            selectStatementPerCounter.put(cql3ColumnName, session.prepare(selectStatement));
        }
        clusteredCounterPSMap.put(INCR, incrStatementPerCounter);
        clusteredCounterPSMap.put(DECR, decrStatementPerCounter);

        RegularStatement selectStatement = statementGenerator.generateWhereClauseForSelect(Optional.<PropertyMeta>absent(), select().from(tableName));
        selectStatementPerCounter.put(SELECT_ALL.name(), session.prepare(selectStatement));
        clusteredCounterPSMap.put(SELECT, selectStatementPerCounter);

        RegularStatement deleteStatement = statementGenerator.generateWhereClauseForDelete(false, delete().from(tableName));
        clusteredCounterPSMap.put(DELETE, of(DELETE_ALL.name(), session.prepare(deleteStatement)));

        return clusteredCounterPSMap;
    }

    private RegularStatement prepareWhereClauseWithTTLForUpdate(PropertyMeta idMeta, Assignments assignments, boolean onlyStaticColumns, Options options) {
        Update.Where where = idMeta.forStatementGeneration().prepareCommonWhereClauseForUpdate(assignments, onlyStaticColumns);
        final Update.Options updateOptions = where.using(ttl(bindMarker("ttl")));
        if (options.getTimestamp().isPresent()) {
            updateOptions.and(timestamp(bindMarker("timestamp")));
        }
        return updateOptions;
    }

    private RegularStatement prepareWhereClauseForCounterUpdate(PropertyMetaStatementGenerator statementGenerator, Assignments assignments, boolean onlyStaticColumns, Options options) {
        Update.Where where = statementGenerator.prepareCommonWhereClauseForUpdate(assignments, onlyStaticColumns);
        if (options.getTimestamp().isPresent()) {
            return where.using(timestamp(bindMarker("timestamp")));
        } else {
            return where;
        }
    }

    public Map<String, PreparedStatement> prepareRemovePSs(Session session, EntityMeta entityMeta) {

        log.trace("Generate prepared statement for DELETE of {}", entityMeta);

        PropertyMeta idMeta = entityMeta.getIdMeta();

        Map<String, PreparedStatement> removePSs = new HashMap<>();

        Delete mainFrom = delete().from(entityMeta.config().getTableName());
        RegularStatement mainStatement = idMeta.forStatementGeneration().generateWhereClauseForDelete(entityMeta.structure().hasOnlyStaticColumns(), mainFrom);
        removePSs.put(entityMeta.config().getTableName(), session.prepare(mainStatement.getQueryString()));

        return removePSs;
    }

    public PreparedStatement prepareCollectionAndMapUpdate(Session session, EntityMeta meta, DirtyCheckChangeSet changeSet, Options options) {

        final Update.Conditions conditions = update(meta.config().getTableName()).onlyIf();

        if (options.hasCasConditions()) {
            for (CASCondition CASCondition : options.getCASConditions()) {
                conditions.and(CASCondition.toClauseForPreparedStatement());
            }
        }

        CollectionAndMapChangeType changeType = changeSet.getChangeType();
        Assignments assignments = null;
        switch (changeType) {
            case ASSIGN_VALUE_TO_LIST:
            case ASSIGN_VALUE_TO_SET:
            case ASSIGN_VALUE_TO_MAP:
            case REMOVE_COLLECTION_OR_MAP:
                assignments = changeSet.generateUpdateForRemoveAll(conditions);
                break;
            case ADD_TO_SET:
                assignments = changeSet.generateUpdateForAddedElements(conditions);
                break;
            case REMOVE_FROM_SET:
                assignments = changeSet.generateUpdateForRemovedElements(conditions);
                break;
            case APPEND_TO_LIST:
                assignments = changeSet.generateUpdateForAppendedElements(conditions);
                break;
            case PREPEND_TO_LIST:
                assignments = changeSet.generateUpdateForPrependedElements(conditions);
                break;
            case REMOVE_FROM_LIST:
                assignments = changeSet.generateUpdateForRemoveListElements(conditions);
                break;
            case SET_TO_LIST_AT_INDEX:
                throw new IllegalStateException("Cannot prepare statement to set element at index for list");
            case REMOVE_FROM_LIST_AT_INDEX:
                throw new IllegalStateException("Cannot prepare statement to remove element at index for list");
            case ADD_TO_MAP:
                assignments = changeSet.generateUpdateForAddedEntries(conditions);
                break;
            case REMOVE_FROM_MAP:
                assignments = changeSet.generateUpdateForRemovedKey(conditions);
                break;
        }

        final RegularStatement regularStatement = prepareWhereClauseWithTTLForUpdate(meta.getIdMeta(), assignments,changeSet.getPropertyMeta().structure().isStaticColumn(), options);
        final PreparedStatement preparedStatement = session.prepare(regularStatement);
        return preparedStatement;
    }

    // Slice Queries
    public PreparedStatement prepareSelectSliceQuery(Session session, SliceQueryProperties<?> sliceQueryProperties) {

        log.trace("Generate SELECT statement for slice query");

        EntityMeta entityMeta = sliceQueryProperties.getEntityMeta();

        Selection select = select();

        for (PropertyMeta pm : entityMeta.forOperations().getColumnsMetaToLoad()) {
            select = pm.forStatementGeneration().prepareSelectField(select);
        }

        Select from = select.from(entityMeta.config().getTableName());

        final RegularStatement whereClause = sliceQueryProperties.generateWhereClauseForSelect(from);

        return session.prepare(whereClause.getQueryString());
    }

    public PreparedStatement prepareDeleteSliceQuery(Session session, SliceQueryProperties<?> sliceQueryProperties) {

        log.trace("Generate DELETE statement for slice query");

        final Delete delete = delete().from(sliceQueryProperties.getEntityMeta().config().getTableName());

        final Delete.Where whereClause = sliceQueryProperties.generateWhereClauseForDelete(delete);

        return session.prepare(whereClause.getQueryString());
    }

}
