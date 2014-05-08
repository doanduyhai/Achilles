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
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static info.archinnov.achilles.type.Options.CasCondition;
import static org.apache.commons.lang.ArrayUtils.addAll;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.internal.statement.wrapper.RegularStatementWrapper;
import info.archinnov.achilles.query.slice.CQLSliceQuery;
import info.archinnov.achilles.type.Pair;

public class StatementGenerator {

    private static final Logger log = LoggerFactory.getLogger(StatementGenerator.class);

    private SliceQueryStatementGenerator sliceQueryGenerator = new SliceQueryStatementGenerator();

    public RegularStatementWrapper generateSelectSliceQuery(CQLSliceQuery<?> sliceQuery, int limit, int batchSize) {

        log.trace("Generate SELECT statement for slice query");
        EntityMeta meta = sliceQuery.getMeta();

        Select select = generateSelectEntityInternal(meta);
        select = select.limit(limit);
        if (sliceQuery.getCQLOrdering() != null) {
            select.orderBy(sliceQuery.getCQLOrdering());
        }
        select.setFetchSize(batchSize);
        return sliceQueryGenerator.generateWhereClauseForSelectSliceQuery(sliceQuery, select);
    }

    public RegularStatementWrapper generateRemoveSliceQuery(CQLSliceQuery<?> sliceQuery) {

        log.trace("Generate DELETE statement for slice query");

        EntityMeta meta = sliceQuery.getMeta();

        Delete delete = QueryBuilder.delete().from(meta.getTableName());
        return sliceQueryGenerator.generateWhereClauseForDeleteSliceQuery(sliceQuery, delete);
    }


    protected Select generateSelectEntityInternal(EntityMeta entityMeta) {

        log.trace("Generate SELECT statement for entity class {}", entityMeta.getClassName());

        PropertyMeta idMeta = entityMeta.getIdMeta();

        Selection select = select();

        generateSelectForPrimaryKey(idMeta, select);

        for (PropertyMeta pm : entityMeta.getColumnsMetaToInsert()) {
            select.column(pm.getPropertyName());
        }
        return select.from(entityMeta.getTableName());
    }


    public Pair<Update.Where, Object[]> generateCollectionAndMapUpdateOperation(PersistenceContext context, DirtyCheckChangeSet changeSet) {

        final Object entity = context.getEntity();
        final EntityMeta meta = context.getEntityMeta();
        final List<CasCondition> casConditions = context.getCasConditions();

        final Update.Conditions conditions = update(meta.getTableName()).onlyIf();
        List<Object> casEncodedValues = addAndEncodeCasConditions(meta, casConditions, conditions);

        final CollectionAndMapChangeType operationType = changeSet.getChangeType();

        Pair<Assignments, Object[]> updateClauseAndBoundValues = null;
        switch (operationType) {
            case SET_TO_LIST_AT_INDEX:
                updateClauseAndBoundValues = changeSet.generateUpdateForSetAtIndexElement(conditions);
                break;
            case REMOVE_FROM_LIST_AT_INDEX:
                updateClauseAndBoundValues = changeSet.generateUpdateForRemovedAtIndexElement(conditions);
                break;
            default:
                throw new AchillesException(String.format("Should not generate non-prepapred statement for collection/map change of type '%s'", operationType));
        }

        final Pair<Update.Where, Object[]> whereClauseAndBoundValues = generateWhereClauseForUpdate(entity, meta.getIdMeta(), updateClauseAndBoundValues.left);
        final Object[] boundValues = addAll(addAll(updateClauseAndBoundValues.right, whereClauseAndBoundValues.right), casEncodedValues.toArray());
        return Pair.create(whereClauseAndBoundValues.left, boundValues);
    }

    private List<Object> addAndEncodeCasConditions(EntityMeta entityMeta, List<CasCondition> casConditions, Update.Conditions conditions) {
        List<Object> casEncodedValues = new ArrayList<>();
        for (CasCondition casCondition : casConditions) {
            final Object encodedValue = entityMeta.encodeCasConditionValue(casCondition);
            casEncodedValues.add(encodedValue);
            conditions.and(casCondition.toClause());
        }
        return casEncodedValues;
    }

    private Pair<Update.Where, Object[]> generateWhereClauseForUpdate(Object entity, PropertyMeta idMeta,
            Assignments update) {
        Update.Where where = null;
        Object[] boundValues;
        Object primaryKey = idMeta.getPrimaryKey(entity);
        if (idMeta.isEmbeddedId()) {
            List<String> componentNames = idMeta.getComponentNames();
            List<Object> encodedComponents = idMeta.encodeToComponents(primaryKey);
            boundValues = new Object[encodedComponents.size()];
            for (int i = 0; i < encodedComponents.size(); i++) {
                String componentName = componentNames.get(i);
                Object componentValue = encodedComponents.get(i);
                if (i == 0) {
                    where = update.where(eq(componentName, componentValue));
                } else {
                    where.and(eq(componentName, componentValue));
                }
                boundValues[i] = componentValue;
            }
        } else {
            Object id = idMeta.encode(primaryKey);
            where = update.where(eq(idMeta.getPropertyName(), id));
            boundValues = new Object[] { id };
        }
        return Pair.create(where, boundValues);
    }


    private void generateSelectForPrimaryKey(PropertyMeta idMeta, Selection select) {
        if (idMeta.isEmbeddedId()) {
            for (String component : idMeta.getComponentNames()) {
                select.column(component);
            }
        } else {
            select.column(idMeta.getPropertyName());
        }
    }

}
