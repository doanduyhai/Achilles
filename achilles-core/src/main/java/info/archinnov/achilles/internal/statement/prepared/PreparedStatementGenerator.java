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
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.ImmutableMap.of;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DECR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.DELETE;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.INCR;
import static info.archinnov.achilles.counter.AchillesCounter.CQLQueryType.SELECT;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_FQCN;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PRIMARY_KEY;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_PROPERTY_NAME;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_TABLE;
import static info.archinnov.achilles.counter.AchillesCounter.CQL_COUNTER_VALUE;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.DELETE_ALL;
import static info.archinnov.achilles.counter.AchillesCounter.ClusteredCounterStatement.SELECT_ALL;
import static info.archinnov.achilles.type.Options.CASCondition;
import static info.archinnov.achilles.type.OptionsBuilder.noOptions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
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
import info.archinnov.achilles.type.Pair;

public class PreparedStatementGenerator {
    private static final Logger log = LoggerFactory.getLogger(PreparedStatementGenerator.class);

    public PreparedStatement prepareInsert(Session session, EntityMeta entityMeta, List<PropertyMeta> pms, Options options) {
        log.trace("Generate prepared statement for INSERT on {}", entityMeta);
        PropertyMeta idMeta = entityMeta.getIdMeta();
        Insert insert = insertInto(entityMeta.getTableName());
        if (options.isIfNotExists()) {
            insert.ifNotExists();
        }

        prepareInsertPrimaryKey(idMeta, insert);

        for (PropertyMeta pm : pms) {
            String property = pm.getPropertyName();
            insert.value(property, bindMarker(property));
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

        if (pm.isCounter()) {
            throw new IllegalArgumentException("Cannot prepare statement for property '" + pm.getPropertyName()
                    + "' of entity '" + entityMeta.getClassName() + "' because it is a counter type");
        } else {
            Selection select = prepareSelectField(pm, select());
            Select from = select.from(entityMeta.getTableName());
            RegularStatement statement = prepareWhereClauseForSelect(idMeta, Optional.fromNullable(pm), from);
            return session.prepare(statement.getQueryString());
        }
    }

    public PreparedStatement prepareUpdateFields(Session session, EntityMeta entityMeta, List<PropertyMeta> pms, Options options) {

        log.trace("Generate prepared statement for UPDATE properties {}", pms);

        PropertyMeta idMeta = entityMeta.getIdMeta();
        Update update = update(entityMeta.getTableName());
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

            if (!pm.isStaticColumn()) {
                onlyStaticColumns = false;
            }

            String property = pm.getPropertyName();
            if (i == 0) {
                assignments = updateConditions.with(set(property, bindMarker(property)));
            } else {
                assignments.and(set(property, bindMarker(property)));
            }
        }

        RegularStatement statement = prepareWhereClauseWithTTLForUpdate(idMeta, assignments, onlyStaticColumns, options);
        return session.prepare(statement.getQueryString());
    }

    public PreparedStatement prepareSelectAll(Session session, EntityMeta entityMeta) {
        log.trace("Generate prepared statement for SELECT of {}", entityMeta);

        PropertyMeta idMeta = entityMeta.getIdMeta();

        Selection select = select();

        for (PropertyMeta pm : entityMeta.getColumnsMetaToLoad()) {
            select = prepareSelectField(pm, select);
        }
        Select from = select.from(entityMeta.getTableName());

        Optional<PropertyMeta> staticMeta = Optional.absent();
        if (entityMeta.hasOnlyStaticColumns()) {
            staticMeta = Optional.fromNullable(entityMeta.getAllMetasExceptId().get(0));
        }

        RegularStatement statement = prepareWhereClauseForSelect(idMeta, staticMeta, from);
        return session.prepare(statement.getQueryString());
    }

    public Map<CQLQueryType, PreparedStatement> prepareSimpleCounterQueryMap(Session session) {

        final String incr = update(CQL_COUNTER_TABLE)
                .with(incr(CQL_COUNTER_VALUE, bindMarker()))
                .where(eq(CQL_COUNTER_FQCN, bindMarker()))
                .and(eq(CQL_COUNTER_PRIMARY_KEY, bindMarker()))
                .and(eq(CQL_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString();

        final String decr = update(CQL_COUNTER_TABLE)
                .with(decr(CQL_COUNTER_VALUE, bindMarker()))
                .where(eq(CQL_COUNTER_FQCN, bindMarker()))
                .and(eq(CQL_COUNTER_PRIMARY_KEY, bindMarker()))
                .and(eq(CQL_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString();

        final String select = select(CQL_COUNTER_VALUE).from(CQL_COUNTER_TABLE)
                .where(eq(CQL_COUNTER_FQCN, bindMarker()))
                .and(eq(CQL_COUNTER_PRIMARY_KEY, bindMarker()))
                .and(eq(CQL_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString();

        final String delete = delete().from(CQL_COUNTER_TABLE)
                .where(eq(CQL_COUNTER_FQCN, bindMarker()))
                .and(eq(CQL_COUNTER_PRIMARY_KEY, bindMarker()))
                .and(eq(CQL_COUNTER_PROPERTY_NAME, bindMarker())).getQueryString();

        Map<CQLQueryType, PreparedStatement> counterPSMap = new HashMap<>();
        counterPSMap.put(INCR, session.prepare(incr));
        counterPSMap.put(DECR, session.prepare(decr));
        counterPSMap.put(SELECT, session.prepare(select));
        counterPSMap.put(DELETE, session.prepare(delete));

        return counterPSMap;
    }

    public Map<CQLQueryType, Map<String, PreparedStatement>> prepareClusteredCounterQueryMap(Session session, EntityMeta meta) {
        PropertyMeta idMeta = meta.getIdMeta();
        String tableName = meta.getTableName();

        Map<CQLQueryType, Map<String, PreparedStatement>> clusteredCounterPSMap = new HashMap<>();
        Map<String, PreparedStatement> incrStatementPerCounter = new HashMap<>();
        Map<String, PreparedStatement> decrStatementPerCounter = new HashMap<>();
        Map<String, PreparedStatement> selectStatementPerCounter = new HashMap<>();

        for (PropertyMeta counterMeta : meta.getAllCounterMetas()) {
            String counterName = counterMeta.getPropertyName();

            RegularStatement incrementStatement = prepareWhereClauseForCounterUpdate(idMeta, update(tableName).with(incr(counterName, bindMarker(counterName))),
                    counterMeta.isStaticColumn(), noOptions());
            RegularStatement decrementStatement = prepareWhereClauseForCounterUpdate(idMeta, update(tableName).with(decr(counterName, bindMarker(counterName))),
                    counterMeta.isStaticColumn(), noOptions());

            RegularStatement selectStatement = prepareWhereClauseForSelect(idMeta, Optional.fromNullable(counterMeta), select(counterName).from(tableName));

            incrStatementPerCounter.put(counterName, session.prepare(incrementStatement));
            decrStatementPerCounter.put(counterName, session.prepare(decrementStatement));
            selectStatementPerCounter.put(counterName, session.prepare(selectStatement));
        }
        clusteredCounterPSMap.put(INCR, incrStatementPerCounter);
        clusteredCounterPSMap.put(DECR, decrStatementPerCounter);

        RegularStatement selectStatement = prepareWhereClauseForSelect(idMeta, Optional.<PropertyMeta>absent(), select().from(tableName));
        selectStatementPerCounter.put(SELECT_ALL.name(), session.prepare(selectStatement));
        clusteredCounterPSMap.put(SELECT, selectStatementPerCounter);

        RegularStatement deleteStatement = prepareWhereClauseForDelete(idMeta, false, QueryBuilder.delete().from(tableName));
        clusteredCounterPSMap.put(DELETE, of(DELETE_ALL.name(), session.prepare(deleteStatement)));

        return clusteredCounterPSMap;
    }

    private Selection prepareSelectField(PropertyMeta pm, Selection select) {
        if (pm.isEmbeddedId()) {
            for (String component : pm.getComponentNames()) {
                select = select.column(component);
            }
        } else {
            select = select.column(pm.getPropertyName());
        }
        return select;
    }

    private void prepareInsertPrimaryKey(PropertyMeta idMeta, Insert insert) {
        if (idMeta.isEmbeddedId()) {
            for (String component : idMeta.getComponentNames()) {
                insert.value(component, bindMarker(component));
            }
        } else {
            String idName = idMeta.getPropertyName();
            insert.value(idName, bindMarker(idName));
        }
    }

    private RegularStatement prepareWhereClauseForSelect(PropertyMeta idMeta, Optional<PropertyMeta> pmO, Select from) {
        RegularStatement statement;
        if (idMeta.isEmbeddedId()) {
            Select.Where where = null;
            int i = 0;
            List<String> componentNames;
            if (pmO.isPresent() && pmO.get().isStaticColumn()) {
                componentNames = idMeta.getPartitionComponentNames();
            } else {
                componentNames = idMeta.getComponentNames();
            }
            for (String partitionKey : componentNames) {
                if (i++ == 0) {
                    where = from.where(eq(partitionKey, bindMarker(partitionKey)));
                } else {
                    where.and(eq(partitionKey, bindMarker(partitionKey)));
                }
            }
            statement = where;
        } else {
            String idName = idMeta.getPropertyName();
            statement = from.where(eq(idName, bindMarker(idName)));
        }
        return statement;
    }

    private RegularStatement prepareWhereClauseWithTTLForUpdate(PropertyMeta idMeta, Assignments assignments, boolean onlyStaticColumns, Options options) {
        Update.Where where = null;
        if (idMeta.isEmbeddedId()) {
            where = prepareCommonWhereClauseForUpdate(idMeta, assignments, onlyStaticColumns, where);
        } else {
            String idName = idMeta.getPropertyName();
            where = assignments.where(eq(idName, bindMarker(idName)));
        }

        final Update.Options updateOptions = where.using(ttl(bindMarker("ttl")));
        if (options.getTimestamp().isPresent()) {
            updateOptions.and(timestamp(bindMarker("timestamp")));
        }
        return updateOptions;
    }

    private RegularStatement prepareWhereClauseForCounterUpdate(PropertyMeta idMeta, Assignments assignments, boolean onlyStaticColumns, Options options) {
        Update.Where where = null;
        if (idMeta.isEmbeddedId()) {
            where = prepareCommonWhereClauseForUpdate(idMeta, assignments, onlyStaticColumns, where);
        } else {
            String idName = idMeta.getPropertyName();
            where = assignments.where(eq(idName, bindMarker(idName)));
        }

        if (options.getTimestamp().isPresent()) {
            return where.using(timestamp(bindMarker("timestamp")));
        } else {
            return where;
        }
    }

    private Update.Where prepareCommonWhereClauseForUpdate(PropertyMeta idMeta, Assignments assignments, boolean onlyStaticColumns, Update.Where where) {
        int i = 0;
        if (onlyStaticColumns) {
            for (String partitionKeys : idMeta.getPartitionComponentNames()) {
                if (i++ == 0) {
                    where = assignments.where(eq(partitionKeys, bindMarker(partitionKeys)));
                } else {
                    where.and(eq(partitionKeys, bindMarker(partitionKeys)));
                }
            }
        } else {
            for (String clusteredId : idMeta.getComponentNames()) {
                if (i++ == 0) {
                    where = assignments.where(eq(clusteredId, bindMarker(clusteredId)));
                } else {
                    where.and(eq(clusteredId, bindMarker(clusteredId)));
                }
            }
        }
        return where;
    }

    public Map<String, PreparedStatement> prepareRemovePSs(Session session, EntityMeta entityMeta) {

        log.trace("Generate prepared statement for DELETE of {}", entityMeta);

        PropertyMeta idMeta = entityMeta.getIdMeta();

        Map<String, PreparedStatement> removePSs = new HashMap<>();

        Delete mainFrom = QueryBuilder.delete().from(entityMeta.getTableName());
        RegularStatement mainStatement = prepareWhereClauseForDelete(idMeta, entityMeta.hasOnlyStaticColumns(),mainFrom);
        removePSs.put(entityMeta.getTableName(), session.prepare(mainStatement.getQueryString()));

        return removePSs;
    }

    private RegularStatement prepareWhereClauseForDelete(PropertyMeta idMeta, boolean onlyStaticColumns, Delete mainFrom) {
        RegularStatement mainStatement;
        if (idMeta.isEmbeddedId()) {
            Delete.Where where = null;
            List<String> componentNames;
            if (onlyStaticColumns) {
                componentNames = idMeta.getPartitionComponentNames();
            } else {
                componentNames = idMeta.getComponentNames();
            }

            int i = 0;
            for (String clusteredId : componentNames) {
                if (i ++== 0) {
                    where = mainFrom.where(eq(clusteredId, bindMarker(clusteredId)));
                } else {
                    where.and(eq(clusteredId, bindMarker(clusteredId)));
                }
            }
            mainStatement = where;
        } else {
            String idName = idMeta.getPropertyName();
            mainStatement = mainFrom.where(eq(idName, bindMarker(idName)));
        }
        return mainStatement;
    }

    public PreparedStatement prepareCollectionAndMapUpdate(Session session, EntityMeta meta, DirtyCheckChangeSet changeSet, Options options) {

        final Update.Conditions conditions = update(meta.getTableName()).onlyIf();

        if (options.hasCasConditions()) {
            for (CASCondition CASCondition : options.getCASConditions()) {
                conditions.and(CASCondition.toClauseForPreparedStatement());
            }
        }

        CollectionAndMapChangeType changeType = changeSet.getChangeType();
        Pair<Assignments, Object[]> updateClauseAndBoundValues = null;
        switch (changeType) {
            case ASSIGN_VALUE_TO_LIST:
            case ASSIGN_VALUE_TO_SET:
            case ASSIGN_VALUE_TO_MAP:
            case REMOVE_COLLECTION_OR_MAP:
                updateClauseAndBoundValues = changeSet.generateUpdateForRemoveAll(conditions, true);
                break;
            case ADD_TO_SET:
                updateClauseAndBoundValues = changeSet.generateUpdateForAddedElements(conditions, true);
                break;
            case REMOVE_FROM_SET:
                updateClauseAndBoundValues = changeSet.generateUpdateForRemovedElements(conditions, true);
                break;
            case APPEND_TO_LIST:
                updateClauseAndBoundValues = changeSet.generateUpdateForAppendedElements(conditions, true);
                break;
            case PREPEND_TO_LIST:
                updateClauseAndBoundValues = changeSet.generateUpdateForPrependedElements(conditions, true);
                break;
            case REMOVE_FROM_LIST:
                updateClauseAndBoundValues = changeSet.generateUpdateForRemoveListElements(conditions, true);
                break;
            case SET_TO_LIST_AT_INDEX:
                throw new IllegalStateException("Cannot prepare statement to set element at index for list");
            case REMOVE_FROM_LIST_AT_INDEX:
                throw new IllegalStateException("Cannot prepare statement to remove element at index for list");
            case ADD_TO_MAP:
                updateClauseAndBoundValues = changeSet.generateUpdateForAddedEntries(conditions, true);
                break;
            case REMOVE_FROM_MAP:
                updateClauseAndBoundValues = changeSet.generateUpdateForRemovedKey(conditions, true);
                break;
        }

        final RegularStatement regularStatement = prepareWhereClauseWithTTLForUpdate(meta.getIdMeta(), updateClauseAndBoundValues.left,
                changeSet.getPropertyMeta().isStaticColumn(), options);
        final PreparedStatement preparedStatement = session.prepare(regularStatement);
        return preparedStatement;
    }

    // Slice Queries
    public PreparedStatement prepareSelectSliceQuery(Session session, SliceQueryProperties<?> sliceQueryProperties) {

        log.trace("Generate SELECT statement for slice query");

        EntityMeta entityMeta = sliceQueryProperties.getEntityMeta();

        Selection select = select();

        for (PropertyMeta pm : entityMeta.getColumnsMetaToLoad()) {
            select = prepareSelectField(pm, select);
        }

        Select from = select.from(entityMeta.getTableName());

        final Select whereClause = sliceQueryProperties.generateWhereClauseForSelect(from);

        return session.prepare(whereClause.getQueryString());
    }

    public PreparedStatement prepareDeleteSliceQuery(Session session, SliceQueryProperties<?> sliceQueryProperties) {

        log.trace("Generate DELETE statement for slice query");

        final Delete delete = QueryBuilder.delete().from(sliceQueryProperties.getEntityMeta().getTableName());

        final Delete.Where whereClause = sliceQueryProperties.generateWhereClauseForDelete(delete);

        return session.prepare(whereClause.getQueryString());
    }

}
