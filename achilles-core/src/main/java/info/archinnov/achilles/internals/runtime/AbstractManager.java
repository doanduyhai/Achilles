/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.internals.runtime;

import static info.archinnov.achilles.internals.runtime.BeanInternalValidator.validateColumnsForInsertOrUpdateStatic;
import static info.archinnov.achilles.internals.runtime.BeanInternalValidator.validatePrimaryKey;
import static info.archinnov.achilles.internals.statement.StatementHelper.isSelectStatement;
import static info.archinnov.achilles.validation.Validator.*;
import static java.lang.String.format;

import java.util.Optional;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.*;

import info.archinnov.achilles.internals.dsl.crud.DeleteWithOptions;
import info.archinnov.achilles.internals.dsl.crud.InsertJSONWithOptions;
import info.archinnov.achilles.internals.dsl.crud.InsertWithOptions;
import info.archinnov.achilles.internals.dsl.crud.UpdateWithOptions;
import info.archinnov.achilles.internals.dsl.raw.NativeQuery;
import info.archinnov.achilles.internals.dsl.raw.TypedQuery;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.type.tuples.Tuple2;

public abstract class AbstractManager<ENTITY> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractManager.class);

    protected final Class<ENTITY> entityClass;
    protected final RuntimeEngine rte;
    protected final AbstractEntityProperty<ENTITY> meta_internal;

    public AbstractManager(Class<ENTITY> entityClass, AbstractEntityProperty<ENTITY> meta_internal, RuntimeEngine rte) {
        this.entityClass = entityClass;
        this.meta_internal = meta_internal;
        this.rte = rte;
    }

    /**
     * Map a given row back to an entity instance.
     * This method provides a raw object mapping facility. Advanced features like interceptors are not available.
     * User codecs are taken into account though.
     *
     * @param row given Cassandra row
     * @return entity instance
     */
    public ENTITY mapFromRow(Row row) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Map row %s back to entity of type %s", row, entityClass.getCanonicalName()));
        }

        validateNotNull(row, "Row object should not be null");
        final String tableName = row.getColumnDefinitions().asList().get(0).getTable();
        final String entityTableName = meta_internal.getTableOrViewName();
        validateTableTrue(entityTableName.equals(tableName),
                "CQL row is from table '%s', it cannot be mapped to entity '%s' associated to table '%s'",
                tableName, entityClass.getCanonicalName(), entityTableName);
        return meta_internal.createEntityFrom(row);
    }

    /**
     * Return the native Session object used by this Manager
     *
     * @return {@link com.datastax.driver.core.Session} instance used by this Manager
     */
    public Session getNativeSession() {
        return rte.session;
    }

    /**
     * Return the native Cluster object used by this Manager
     *
     * @return {@link com.datastax.driver.core.Cluster} instance used by this Manager
     */
    public Cluster getNativeCluster() {
        return rte.getCluster();
    }

    protected InsertWithOptions<ENTITY> insertInternal(ENTITY instance, boolean insertStatic, Optional<CassandraOptions> cassandraOptions) {

        validateNotNull(instance, "Entity to be inserted should not be null");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create insert CRUD for entity %s", instance));
        }

        if (insertStatic) {
            validateColumnsForInsertOrUpdateStatic(instance, meta_internal, cassandraOptions);
        } else {
            validatePrimaryKey(instance, meta_internal, cassandraOptions);
        }

        return new InsertWithOptions<>(meta_internal, rte, instance, insertStatic, cassandraOptions);
    }

    protected UpdateWithOptions<ENTITY> updateInternal(ENTITY instance, boolean updateStatic, Optional<CassandraOptions> cassandraOptions) {

        validateNotNull(instance, "Entity to be updated to Cassandra should not be null");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create update CRUD for entity %s", instance));
        }

        if (updateStatic) {
            validateColumnsForInsertOrUpdateStatic(instance, meta_internal, cassandraOptions);
        } else {
            validatePrimaryKey(instance, meta_internal, cassandraOptions);
        }

        return new UpdateWithOptions<>(meta_internal, rte, instance, updateStatic, cassandraOptions);
    }

    protected InsertJSONWithOptions insertJSONInternal(String json, Optional<CassandraOptions> cassandraOptions) {

        validateNotBlank(json, "The JSON string to be used for INSERT JSON should not be blank");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create insert json CRUD for with JSON %s", json));
        }

        return new InsertJSONWithOptions(meta_internal, rte, json, cassandraOptions);
    }

    protected DeleteWithOptions<ENTITY> deleteInternal(ENTITY instance, Optional<CassandraOptions> cassandraOptions) {
        validateNotNull(instance, "Entity to be deleted should not be null");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create delete CRUD for entity %s", instance));
        }

        validatePrimaryKey(instance, meta_internal, cassandraOptions);
        final Tuple2<Object[], Object[]> tuple = BeanValueExtractor.extractPrimaryKeyValues(instance, meta_internal, cassandraOptions);
        return new DeleteWithOptions<>(entityClass, meta_internal, rte, tuple._1(), tuple._2(), Optional.of(instance), cassandraOptions);
    }

    protected TypedQuery<ENTITY> typedQueryForSelectInternal(BoundStatement boundStatement) {
        validateTrue(isSelectStatement(boundStatement), "Statement provided for typed query should be an SELECT statement");

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create typed query for SELECT : %s",
                    boundStatement.preparedStatement().getQueryString()));
        }

        return new TypedQuery<>(rte, meta_internal, boundStatement, new Object[]{});
    }


    protected TypedQuery<ENTITY> typedQueryForSelectInternal(PreparedStatement preparedStatement, Object... encodedBoundValues) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create typed query for SELECT : %s",
                    preparedStatement.getQueryString()));
        }

        validateTrue(isSelectStatement(preparedStatement), "Statement provided for typed query should be an SELECT statement");
        validateNotEmpty(encodedBoundValues, "Encoded values provided for typed query should not be empty");
        return new TypedQuery<>(rte, meta_internal, preparedStatement.bind(encodedBoundValues), encodedBoundValues);
    }

    protected TypedQuery<ENTITY> typedQueryForSelectInternal(RegularStatement regularStatement, Object... encodedBoundValues) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create typed query for SELECT : %s",
                    regularStatement.getQueryString()));
        }

        validateTrue(isSelectStatement(regularStatement), "Statement provided for typed query should be an SELECT statement");
        final PreparedStatement preparedStatement = rte.prepareDynamicQuery(regularStatement);
        final BoundStatement boundStatement = ArrayUtils.isEmpty(encodedBoundValues)
                ? preparedStatement.bind()
                : preparedStatement.bind(encodedBoundValues);

        if (regularStatement.getConsistencyLevel() != null)
            boundStatement.setConsistencyLevel(regularStatement.getConsistencyLevel());
        if (regularStatement.getSerialConsistencyLevel() != null)
            boundStatement.setSerialConsistencyLevel(regularStatement.getSerialConsistencyLevel());
        if (regularStatement.isTracing())
            boundStatement.enableTracing();
        if (regularStatement.getRetryPolicy() != null)
            boundStatement.setRetryPolicy(regularStatement.getRetryPolicy());
        if (regularStatement.getOutgoingPayload() != null)
            boundStatement.setOutgoingPayload(regularStatement.getOutgoingPayload());
        if (regularStatement.isIdempotent() != null) {
            boundStatement.setIdempotent(regularStatement.isIdempotent());
        }
        boundStatement.setFetchSize(regularStatement.getFetchSize());

        return new TypedQuery<>(rte, meta_internal, boundStatement, encodedBoundValues);
    }

    protected NativeQuery nativeQueryInternal(BoundStatement boundStatement) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create native query : %s",
                    boundStatement.preparedStatement().getQueryString()));
        }

        if (meta_internal.isView()) {
            validateTrue(isSelectStatement(boundStatement), "Statement provided for the materialized view '%s' should be an SELECT statement",
                    meta_internal.entityClass.getCanonicalName());
        }

        return new NativeQuery(meta_internal, rte, boundStatement, new Object[0]);
    }

    protected NativeQuery nativeQueryInternal(PreparedStatement preparedStatement, Object... encodedBoundValues) {

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create native query : %s",
                    preparedStatement.getQueryString()));
        }

        if (meta_internal.isView()) {
            validateTrue(isSelectStatement(preparedStatement), "Statement provided for the materialized view '%s' should be an SELECT statement",
                    meta_internal.entityClass.getCanonicalName());
        }

        validateNotEmpty(encodedBoundValues, "Encoded values provided for native query should not be empty");
        return new NativeQuery(meta_internal, rte, preparedStatement.bind(encodedBoundValues), encodedBoundValues);
    }


    protected NativeQuery nativeQueryInternal(RegularStatement regularStatement, Object... encodedBoundValues) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Create native query : %s",
                    regularStatement.getQueryString()));
        }

        if (meta_internal.isView()) {
            validateTrue(isSelectStatement(regularStatement), "Statement provided for the materialized view '%s' should be an SELECT statement",
                    meta_internal.entityClass.getCanonicalName());
        }

        final PreparedStatement preparedStatement = rte.prepareDynamicQuery(regularStatement);
        final BoundStatement boundStatement = ArrayUtils.isEmpty(encodedBoundValues)
                ? preparedStatement.bind()
                : preparedStatement.bind(encodedBoundValues);

        return new NativeQuery(meta_internal, rte, boundStatement, encodedBoundValues);
    }
}
