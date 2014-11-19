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

import static info.archinnov.achilles.internal.consistency.ConsistencyConverter.getCQLLevel;
import static org.apache.commons.lang3.ArrayUtils.addAll;

import java.util.ArrayList;
import java.util.List;

import info.archinnov.achilles.internal.statement.StatementHelper;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.internal.consistency.ConsistencyOverrider;
import info.archinnov.achilles.internal.context.facade.PersistentStateHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;

public class PreparedStatementBinder {

    private static final Logger log = LoggerFactory.getLogger(PreparedStatementBinder.class);

    protected static final Optional<CASResultListener> NO_LISTENER = Optional.absent();
    protected static final Optional<com.datastax.driver.core.ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    private ConsistencyOverrider overrider = ConsistencyOverrider.Singleton.INSTANCE.get();

    public BoundStatementWrapper bindForInsert(PersistentStateHolder context, PreparedStatement ps, List<PropertyMeta> pms) {

        EntityMeta entityMeta = context.getEntityMeta();
        Object entity = context.getEntity();

        log.trace("Bind prepared statement {} for insert of entity {}", ps.getQueryString(), entity);

        ConsistencyLevel consistencyLevel = overrider.getWriteLevel(context);

        List<Object> values = new ArrayList<>();
        values.addAll(fetchPrimaryKeyValues(entityMeta, entity, StatementHelper.hasOnlyStaticColumns(pms)));
        values.addAll(fetchPropertiesValues(pms, entity));
        values.addAll(fetchTTLAndTimestampValues(context));

        BoundStatement bs = ps.bind(values.toArray());
        return new BoundStatementWrapper(context.getEntityClass(), bs, values.toArray(), getCQLLevel(consistencyLevel),
                context.getCASResultListener(), context.getSerialConsistencyLevel());
    }


    public BoundStatementWrapper bindForUpdate(PersistentStateHolder context, PreparedStatement ps, List<PropertyMeta> pms) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object entity = context.getEntity();

        if (log.isTraceEnabled()) {
            log.trace("Bind prepared statement {} for properties {} update of entity {}", ps.getQueryString(), pms, entity);
        }

        ConsistencyLevel consistencyLevel = overrider.getWriteLevel(context);

        List<Object> values = new ArrayList<>();

        final int staticColumnsCount = FluentIterable.from(pms).filter(PropertyMeta.STATIC_COLUMN_FILTER).size();
        final boolean onlyStaticColumns = staticColumnsCount>0 && pms.size()==staticColumnsCount;

        values.addAll(fetchTTLAndTimestampValues(context));
        values.addAll(fetchPropertiesValues(pms, entity));
        values.addAll(fetchPrimaryKeyValues(entityMeta, entity, onlyStaticColumns));
        values.addAll(fetchCASConditionsValues(context, entityMeta));
        BoundStatement bs = ps.bind(values.toArray());

        return new BoundStatementWrapper(context.getEntityClass(), bs, values.toArray(), getCQLLevel(consistencyLevel),
                context.getCASResultListener(), context.getSerialConsistencyLevel());
    }

    public BoundStatementWrapper bindForCollectionAndMapUpdate(PersistentStateHolder context, PreparedStatement ps, DirtyCheckChangeSet changeSet) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object entity = context.getEntity();

        log.trace("Bind prepared statement {} for collection/map update of entity {}", ps.getQueryString(), entity);

        ConsistencyLevel consistencyLevel = overrider.getWriteLevel(context);

        List<Object> values = new ArrayList<>();
        final CollectionAndMapChangeType changeType = changeSet.getChangeType();

        values.addAll(fetchTTLAndTimestampValues(context));

        switch (changeType) {
            case ASSIGN_VALUE_TO_LIST:
                values.add(changeSet.getEncodedListChanges());
                break;
            case ASSIGN_VALUE_TO_SET:
                values.add(changeSet.getEncodedSetChanges());
                break;
            case ASSIGN_VALUE_TO_MAP:
                values.add(changeSet.getEncodedMapChanges());
                break;
            case REMOVE_COLLECTION_OR_MAP:
                values.add(null);
                break;
            case ADD_TO_SET:
            case REMOVE_FROM_SET:
                values.add(changeSet.getEncodedSetChanges());
                break;
            case APPEND_TO_LIST:
            case PREPEND_TO_LIST:
            case REMOVE_FROM_LIST:
                values.add(changeSet.getEncodedListChanges());
                break;
            case SET_TO_LIST_AT_INDEX:
                // No prepared statement for set list element at index
                throw new IllegalStateException("Cannot bind statement to set element at index for list");
            case REMOVE_FROM_LIST_AT_INDEX:
                // No prepared statement for set list element at index
                throw new IllegalStateException("Cannot bind statement to remove element at index for list");
            case ADD_TO_MAP:
                values.add(changeSet.getEncodedMapChanges());
                break;
            case REMOVE_FROM_MAP:
                values.add(changeSet.getEncodedMapChanges().keySet().iterator().next());
                values.add(null);
                break;
        }

        values.addAll(fetchPrimaryKeyValues(entityMeta, entity, changeSet.getPropertyMeta().structure().isStaticColumn()));
        values.addAll(fetchCASConditionsValues(context, entityMeta));
        BoundStatement bs = ps.bind(values.toArray());

        return new BoundStatementWrapper(context.getEntityClass(), bs, values.toArray(), getCQLLevel(consistencyLevel),
                context.getCASResultListener(), context.getSerialConsistencyLevel());
    }


    public BoundStatementWrapper bindStatementWithOnlyPKInWhereClause(PersistentStateHolder context, PreparedStatement ps, boolean onlyStaticColumns,ConsistencyLevel consistencyLevel) {

        Object primaryKey = context.getPrimaryKey();

        log.trace("Bind prepared statement {} with primary key {}", ps.getQueryString(), primaryKey);

        PropertyMeta idMeta = context.getIdMeta();
        List<Object> values = bindPrimaryKey(primaryKey, idMeta,onlyStaticColumns);

        BoundStatement bs = ps.bind(values.toArray());
        return new BoundStatementWrapper(context.getEntityClass(), bs, values.toArray(), getCQLLevel(consistencyLevel),
                context.getCASResultListener(), context.getSerialConsistencyLevel());
    }

    public BoundStatementWrapper bindForSimpleCounterIncrementDecrement(PersistentStateHolder context, PreparedStatement ps, PropertyMeta pm, Long increment, ConsistencyLevel consistencyLevel) {

        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        if (log.isTraceEnabled()) {
            log.trace("Bind prepared statement {} for simple counter increment of {} using primary key {} and value {}", ps.getQueryString(), pm, primaryKey, increment);
        }

        Object[] boundValues = ArrayUtils.add(extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey), 0, increment);

        BoundStatement bs = ps.bind(boundValues);
        return new BoundStatementWrapper(context.getEntityClass(), bs, boundValues, getCQLLevel(consistencyLevel), NO_LISTENER, NO_SERIAL_CONSISTENCY);
    }

    public BoundStatementWrapper bindForSimpleCounterSelect(PersistentStateHolder context, PreparedStatement ps, PropertyMeta pm, ConsistencyLevel consistencyLevel) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        if (log.isTraceEnabled()) {
            log.trace("Bind prepared statement {} for simple counter read of {} using primary key {}", ps.getQueryString(), pm, primaryKey);
        }

        Object[] boundValues = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
        BoundStatement bs = ps.bind(boundValues);
        return new BoundStatementWrapper(context.getEntityClass(), bs, boundValues, getCQLLevel(consistencyLevel), NO_LISTENER, NO_SERIAL_CONSISTENCY);
    }

    public BoundStatementWrapper bindForSimpleCounterDelete(PersistentStateHolder context, PreparedStatement ps, PropertyMeta pm) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        if (log.isTraceEnabled()) {
            log.trace("Bind prepared statement {} for simple counter delete for {} using primary key {}", ps.getQueryString(), pm, primaryKey);
        }

        ConsistencyLevel consistencyLevel = overrider.getWriteLevel(context);

        Object[] boundValues = extractValuesForSimpleCounterBinding(entityMeta, pm, primaryKey);
        BoundStatement bs = ps.bind(boundValues);
        return new BoundStatementWrapper(context.getEntityClass(), bs, boundValues, getCQLLevel(consistencyLevel), NO_LISTENER, NO_SERIAL_CONSISTENCY);
    }

    public BoundStatementWrapper bindForClusteredCounterIncrementDecrement(PersistentStateHolder context, PreparedStatement ps, PropertyMeta counterMeta,Long increment) {

        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        if (log.isTraceEnabled()) {
            log.trace("Bind prepared statement {} for clustered counter increment/decrement for {} using primary key {} and value {}", ps.getQueryString(), entityMeta, primaryKey, increment);
        }

        ConsistencyLevel consistencyLevel = overrider.getWriteLevel(context);

        List<Object> primaryKeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta(), counterMeta.structure().isStaticColumn());
        Object[] keys = addAll(new Object[]{increment}, primaryKeys.toArray());

        BoundStatement bs = ps.bind(keys);

        return new BoundStatementWrapper(context.getEntityClass(), bs, keys, getCQLLevel(consistencyLevel), NO_LISTENER, NO_SERIAL_CONSISTENCY);
    }

    public BoundStatementWrapper bindForClusteredCounterSelect(PersistentStateHolder context, PreparedStatement ps, boolean onlyStaticColumns,ConsistencyLevel consistencyLevel) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        if (log.isTraceEnabled()) {
            log.trace("Bind prepared statement {} for clustered counter read for {} using primary key {}", ps.getQueryString(), entityMeta, primaryKey);
        }

        List<Object> primaryKeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta(), onlyStaticColumns);
        Object[] boundValues = primaryKeys.toArray();

        BoundStatement bs = ps.bind(boundValues);
        return new BoundStatementWrapper(context.getEntityClass(), bs, boundValues, getCQLLevel(consistencyLevel), NO_LISTENER, NO_SERIAL_CONSISTENCY);
    }

    public BoundStatementWrapper bindForClusteredCounterDelete(PersistentStateHolder context, PreparedStatement ps) {
        EntityMeta entityMeta = context.getEntityMeta();
        Object primaryKey = context.getPrimaryKey();

        if (log.isTraceEnabled()) {
            log.trace("Bind prepared statement {} for simple counter delete for {} using primary key {}", ps.getQueryString(), entityMeta, primaryKey);
        }

        ConsistencyLevel consistencyLevel = overrider.getWriteLevel(context);
        List<Object> primaryKeys = bindPrimaryKey(primaryKey, entityMeta.getIdMeta(), false);
        Object[] boundValues = primaryKeys.toArray(new Object[primaryKeys.size()]);
        BoundStatement bs = ps.bind(boundValues);

        return new BoundStatementWrapper(context.getEntityClass(), bs, boundValues, getCQLLevel(consistencyLevel), NO_LISTENER, NO_SERIAL_CONSISTENCY);
    }

    private List<Object> fetchPrimaryKeyValues(EntityMeta entityMeta, Object entity, boolean onlyStaticColumns) {
        List<Object> values = new ArrayList<>();
        Object primaryKey = entityMeta.forOperations().getPrimaryKey(entity);
        values.addAll(bindPrimaryKey(primaryKey, entityMeta.getIdMeta(),onlyStaticColumns));
        return values;
    }

    private List<Object> fetchTTLAndTimestampValues(PersistentStateHolder context) {
        List<Object> values = new ArrayList<>();

        // TTL or default value 0
        values.add(context.getTtl().or(0));
        if (context.getTimestamp().isPresent()) {
            values.add(context.getTimestamp().get());
        }
        return values;
    }

    private List<Object> fetchPropertiesValues(List<PropertyMeta> pms, Object entity) {
        List<Object> values = new ArrayList<>();
        for (PropertyMeta pm : pms) {
            Object value = pm.forTranscoding().getAndEncodeValueForCassandra(entity);
            values.add(value);
        }

        return values;
    }

    private List<Object> fetchCASConditionsValues(PersistentStateHolder context, EntityMeta entityMeta) {
        List<Object> values = new ArrayList<>();
        if (context.hasCasConditions()) {
            for (Options.CASCondition CASCondition : context.getCasConditions()) {
                values.add(entityMeta.forTranscoding().encodeCasConditionValue(CASCondition));
            }
        }
        return values;
    }


    private List<Object> bindPrimaryKey(Object primaryKey, PropertyMeta idMeta, boolean onlyStaticColumns) {
        List<Object> values = new ArrayList<>();
        if (idMeta.structure().isEmbeddedId()) {
            values.addAll(idMeta.forTranscoding().encodeToComponents(primaryKey, onlyStaticColumns));
        } else {
            values.add(idMeta.forTranscoding().encodeToCassandra(primaryKey));
        }
        return values;
    }

    private Object[] extractValuesForSimpleCounterBinding(EntityMeta entityMeta, PropertyMeta pm, Object primaryKey) {
        PropertyMeta idMeta = entityMeta.getIdMeta();
        String fqcn = entityMeta.getClassName();
        String primaryKeyAsString = idMeta.forTranscoding().forceEncodeToJSONForCounter(primaryKey);
        String cql3ColumnName = pm.getCQL3ColumnName();

        return new Object[] { fqcn, primaryKeyAsString, cql3ColumnName };
    }

    public static enum Singleton {
        INSTANCE;

        private final PreparedStatementBinder instance = new PreparedStatementBinder();

        public PreparedStatementBinder get() {
            return instance;
        }
    }
}
