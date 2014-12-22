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

import static com.datastax.driver.core.querybuilder.QueryBuilder.timestamp;
import static com.datastax.driver.core.querybuilder.QueryBuilder.ttl;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static info.archinnov.achilles.type.Options.LWTCondition;
import static org.apache.commons.lang3.ArrayUtils.addAll;
import java.util.ArrayList;
import java.util.List;

import info.archinnov.achilles.internal.metadata.holder.EntityMetaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.base.Optional;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.internal.context.facade.PersistentStateHolder;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.persistence.operations.CollectionAndMapChangeType;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyCheckChangeSet;
import info.archinnov.achilles.type.Pair;

public class StatementGenerator {

    private static final Logger log = LoggerFactory.getLogger(StatementGenerator.class);

    public Pair<Update.Where, Object[]> generateCollectionAndMapUpdateOperation(PersistentStateHolder context, DirtyCheckChangeSet changeSet) {
        log.trace("Generate collection/map update operation for dirty change set {}", changeSet);

        final Object entity = context.getEntity();
        final EntityMeta meta = context.getEntityMeta();
        final EntityMetaConfig metaConfig = meta.config();

        final Optional<Integer> ttlO = context.getTtl();
        final Optional<Long> timestampO = context.getTimestamp();
        final List<LWTCondition> LWTConditions = context.getLWTConditions();

        final Update.Conditions conditions = update(metaConfig.getKeyspaceName(), metaConfig.getTableName()).onlyIf();
        List<Object> LWTEncodedValues = addAndEncodeLWTConditions(meta, LWTConditions, conditions);

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
                throw new AchillesException(String.format("Should not generate non-prepared statement for collection/map change of type '%s'", operationType));
        }

        final Pair<Update.Where, Object[]> whereClauseAndBoundValues = meta.getIdMeta().forStatementGeneration().generateWhereClauseForUpdate(entity, changeSet.getPropertyMeta(), updateClauseAndBoundValues.left);
        boundValues = addAll(addAll(boundValues, addAll(updateClauseAndBoundValues.right, whereClauseAndBoundValues.right)), LWTEncodedValues.toArray());
        return Pair.create(whereClauseAndBoundValues.left, boundValues);
    }

    private List<Object> addAndEncodeLWTConditions(EntityMeta entityMeta, List<LWTCondition> LWTConditions, Update.Conditions conditions) {
        List<Object> LWTEncodedValues = new ArrayList<>();
        for (LWTCondition LWTCondition : LWTConditions) {
            final Object encodedValue = entityMeta.forTranscoding().encodeCasConditionValue(LWTCondition);
            LWTEncodedValues.add(encodedValue);
            conditions.and(LWTCondition.toClause());
        }
        return LWTEncodedValues;
    }

    public static enum Singleton {
        INSTANCE;

        private final StatementGenerator instance = new StatementGenerator();

        public StatementGenerator get() {
            return instance;
        }
    }
}
