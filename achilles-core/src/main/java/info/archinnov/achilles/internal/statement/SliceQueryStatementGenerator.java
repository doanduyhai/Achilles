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
package info.archinnov.achilles.internal.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static info.archinnov.achilles.type.OrderingMode.ASCENDING;
import java.util.List;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.listener.CASResultListener;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.type.OrderingMode;

public class SliceQueryStatementGenerator {

    private static final Logger log = LoggerFactory.getLogger(SliceQueryStatementGenerator.class);

    private static final Optional<CASResultListener> NO_CAS_LISTENER = Optional.absent();
    private static final Optional<com.datastax.driver.core.ConsistencyLevel> NO_SERIAL_CONSISTENCY = Optional.absent();

    public RegularStatementWrapper generateWhereClauseForSelectSliceQuery(CQLSliceQuery<?> sliceQuery, Select select) {
        Where where = select.where();
        List<Object> fixedComponents = sliceQuery.getFixedComponents();
        List<String> componentNames = sliceQuery.getComponentNames();
        String varyingComponentName = sliceQuery.getVaryingComponentName();
        OrderingMode ordering = sliceQuery.getOrdering();

        Object lastStartComp = sliceQuery.getLastStartComponent();
        Object lastEndComp = sliceQuery.getLastEndComponent();
        Object[] boundValues = new Object[fixedComponents.size()];

        for (int i = 0; i < fixedComponents.size(); i++) {
            where.and(eq(componentNames.get(i), fixedComponents.get(i)));
            boundValues[i] = fixedComponents.get(i);
        }

        if (lastStartComp != null)
            boundValues = ArrayUtils.add(boundValues, lastStartComp);
        if (lastEndComp != null)
            boundValues = ArrayUtils.add(boundValues, lastEndComp);

        if (ordering == ASCENDING) {

            switch (sliceQuery.getBounding()) {
                case INCLUSIVE_BOUNDS:
                    if (lastStartComp != null)
                        where.and(gte(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(lte(varyingComponentName, lastEndComp));
                    break;
                case EXCLUSIVE_BOUNDS:
                    if (lastStartComp != null)
                        where.and(gt(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(lt(varyingComponentName, lastEndComp));
                    break;
                case INCLUSIVE_START_BOUND_ONLY:
                    if (lastStartComp != null)
                        where.and(gte(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(lt(varyingComponentName, lastEndComp));
                    break;
                case INCLUSIVE_END_BOUND_ONLY:
                    if (lastStartComp != null)
                        where.and(gt(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(lte(varyingComponentName, lastEndComp));
                    break;
            }
        } else // ordering == DESCENDING
        {
            switch (sliceQuery.getBounding()) {
                case INCLUSIVE_BOUNDS:
                    if (lastStartComp != null)
                        where.and(lte(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(gte(varyingComponentName, lastEndComp));
                    break;
                case EXCLUSIVE_BOUNDS:
                    if (lastStartComp != null)
                        where.and(lt(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(gt(varyingComponentName, lastEndComp));
                    break;
                case INCLUSIVE_START_BOUND_ONLY:
                    if (lastStartComp != null)
                        where.and(lte(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(gt(varyingComponentName, lastEndComp));
                    break;
                case INCLUSIVE_END_BOUND_ONLY:
                    if (lastStartComp != null)
                        where.and(lt(varyingComponentName, lastStartComp));
                    if (lastEndComp != null)
                        where.and(gte(varyingComponentName, lastEndComp));
                    break;
            }
        }
        where.setFetchSize(sliceQuery.getBatchSize());
        log.trace("Generated WHERE clause for slice query : {}", where.getQueryString());
        return new RegularStatementWrapper(sliceQuery.getEntityClass(), where, boundValues, sliceQuery.getConsistencyLevel(), NO_CAS_LISTENER,NO_SERIAL_CONSISTENCY);
    }

    public RegularStatementWrapper generateWhereClauseForDeleteSliceQuery(CQLSliceQuery<?> sliceQuery, Delete delete) {
        List<Object> fixedComponents = sliceQuery.getFixedComponents();
        List<String> componentNames = sliceQuery.getComponentNames();

        com.datastax.driver.core.querybuilder.Delete.Where where = delete.where();
        Object[] boundValues = new Object[fixedComponents.size()];

        for (int i = 0; i < fixedComponents.size(); i++) {
            where.and(eq(componentNames.get(i), fixedComponents.get(i)));
            boundValues[i] = fixedComponents.get(i);
        }
        log.trace("Generated WHERE clause for slice delete query : {}", where.getQueryString());
        return new RegularStatementWrapper(sliceQuery.getEntityClass(), where, boundValues, sliceQuery.getConsistencyLevel(), NO_CAS_LISTENER,NO_SERIAL_CONSISTENCY);
    }

}
