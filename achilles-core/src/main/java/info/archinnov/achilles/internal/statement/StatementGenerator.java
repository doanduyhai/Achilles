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
import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static info.archinnov.achilles.type.Options.CASCondition;
import static org.apache.commons.lang.ArrayUtils.addAll;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.base.Optional;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.context.facade.PersistentStateHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.type.Pair;

public class StatementGenerator {

    private static final Logger log = LoggerFactory.getLogger(StatementGenerator.class);

    public Pair<Update.Where, Object[]> generateCollectionAndMapUpdateOperation(PersistentStateHolder context, DirtyCheckChangeSet changeSet) {
        log.trace("Generate collection/map update operation for dirty change set {}", changeSet);

        final Object entity = context.getEntity();
        final EntityMeta meta = context.getEntityMeta();
        final Optional<Integer> ttlO = context.getTtl();
        final Optional<Long> timestampO = context.getTimestamp();
        final List<CASCondition> CASConditions = context.getCasConditions();

        final Update.Conditions conditions = update(meta.getTableName()).onlyIf();
        List<Object> casEncodedValues = addAndEncodeCasConditions(meta, CASConditions, conditions);

        Object[] boundValues = new Object[] { };
        if (ttlO.isPresent()) {
            conditions.using(ttl(ttlO.get()));
            boundValues = addAll(boundValues, new Object[] { ttlO.get() });
        }

        if (timestampO.isPresent()) {
            conditions.using(timestamp(timestampO.get()));
            boundValues = addAll(boundValues, new Object[] { timestampO.get() });
        }

        final CollectionAndMapChangeType operationType = changeSet.getChangeType();

        Pair<Assignments, Object[]> updateClauseAndBoundValues;
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

        final Pair<Update.Where, Object[]> whereClauseAndBoundValues = generateWhereClauseForUpdate(entity, meta.getIdMeta(),
                changeSet.getPropertyMeta(), updateClauseAndBoundValues.left);
        boundValues = addAll(addAll(boundValues, addAll(updateClauseAndBoundValues.right, whereClauseAndBoundValues.right)), casEncodedValues.toArray());
        return Pair.create(whereClauseAndBoundValues.left, boundValues);
    }

    private List<Object> addAndEncodeCasConditions(EntityMeta entityMeta, List<CASCondition> CASConditions, Update.Conditions conditions) {
        List<Object> casEncodedValues = new ArrayList<>();
        for (CASCondition CASCondition : CASConditions) {
            final Object encodedValue = entityMeta.encodeCasConditionValue(CASCondition);
            casEncodedValues.add(encodedValue);
            conditions.and(CASCondition.toClause());
        }
        return casEncodedValues;
    }

    private Pair<Update.Where, Object[]> generateWhereClauseForUpdate(Object entity, PropertyMeta idMeta, PropertyMeta pm, Assignments update) {
        Update.Where where = null;
        Object[] boundValues;
        Object primaryKey = idMeta.getPrimaryKey(entity);
        if (idMeta.isEmbeddedId()) {
            List<String> componentNames = idMeta.getComponentNames();
            List<Object> encodedComponents = idMeta.encodeToComponents(primaryKey, pm.isStaticColumn());
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

}
