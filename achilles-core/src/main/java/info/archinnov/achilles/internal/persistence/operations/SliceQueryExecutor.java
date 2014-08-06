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
package info.archinnov.achilles.internal.persistence.operations;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.Row;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.context.DaoContext;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.context.PersistenceContextFactory;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.statement.wrapper.BoundStatementWrapper;
import info.archinnov.achilles.iterator.SliceQueryIterator;
import info.archinnov.achilles.query.slice.SliceQueryProperties;
import info.archinnov.achilles.type.ConsistencyLevel;

public class SliceQueryExecutor {

    private static final Logger log = LoggerFactory.getLogger(SliceQueryExecutor.class);

    private EntityMapper mapper = new EntityMapper();
    private EntityProxifier proxifier = new EntityProxifier();
    private PersistenceContextFactory contextFactory;
    private DaoContext daoContext;
    private ConsistencyLevel defaultReadLevel;
    private ConsistencyLevel defaultWriteLevel;

    public SliceQueryExecutor(PersistenceContextFactory contextFactory, ConfigurationContext configContext,
            DaoContext daoContext) {
        this.contextFactory = contextFactory;
        this.daoContext = daoContext;
        this.defaultReadLevel = configContext.getDefaultReadConsistencyLevel();
        this.defaultWriteLevel = configContext.getDefaultWriteConsistencyLevel();
    }

    public <T> List<T> get(SliceQueryProperties<T> sliceQueryProperties) {
        log.debug("Get slice query");
        EntityMeta meta = sliceQueryProperties.getEntityMeta();

        List<T> clusteredEntities = new ArrayList<>();

        final BoundStatementWrapper bsWrapper = daoContext.bindForSliceQuerySelect(sliceQueryProperties, defaultReadLevel);
        List<Row> rows = daoContext.execute(bsWrapper).all();

        for (Row row : rows) {
            T clusteredEntity = meta.forOperations().instanciate();
            mapper.setNonCounterPropertiesToEntity(row, meta, clusteredEntity);
            meta.forInterception().intercept(clusteredEntity, Event.POST_LOAD);
            clusteredEntities.add(clusteredEntity);
        }

        return new ArrayList<>(FluentIterable.from(clusteredEntities).transform(this.<T>getProxyTransformer()).toList());
    }

    public <T> Iterator<T> iterator(SliceQueryProperties<T> sliceQueryProperties) {
        log.debug("Get iterator for slice query");
        final BoundStatementWrapper bsWrapper = daoContext.bindForSliceQuerySelect(sliceQueryProperties, defaultReadLevel);
        Iterator<Row> iterator = daoContext.execute(bsWrapper).iterator();
        PersistenceContext context = buildContextForQuery(sliceQueryProperties);
        return new SliceQueryIterator<>(sliceQueryProperties, context, iterator);
    }

    public <T> void delete(SliceQueryProperties<T> sliceQueryProperties) {
        log.debug("Slice delete");
        final BoundStatementWrapper bsWrapper = daoContext.bindForSliceQueryDelete(sliceQueryProperties, defaultWriteLevel);
        daoContext.execute(bsWrapper);
    }

    protected <T> PersistenceContext buildContextForQuery(SliceQueryProperties<T> sliceQueryProperties) {
        log.trace("Build PersistenceContext for slice query");
        ConsistencyLevel cl = sliceQueryProperties.getConsistencyLevelOr(defaultReadLevel);
        return contextFactory.newContextForSliceQuery(sliceQueryProperties.getEntityClass(), sliceQueryProperties.getPartitionKeys(), cl);
    }

    private <T> Function<T, T> getProxyTransformer() {
        return new Function<T, T>() {
            @Override
            public T apply(T clusteredEntity) {
                PersistenceContext context = contextFactory.newContext(clusteredEntity);
                return proxifier.buildProxyWithAllFieldsLoadedExceptCounters(clusteredEntity, context.getEntityFacade());
            }
        };
    }
}
