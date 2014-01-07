/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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
package info.archinnov.achilles.internal.persistence.operations;

import static com.google.common.collect.Collections2.filter;
import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.*;
import static info.archinnov.achilles.type.CounterBuilder.initialValue;
import info.archinnov.achilles.internal.context.PersistenceContext;
import info.archinnov.achilles.internal.persistence.metadata.EntityMeta;
import info.archinnov.achilles.internal.persistence.metadata.PropertyMeta;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.CounterBuilder;
import info.archinnov.achilles.type.CounterImpl;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityPersister {
	private static final Logger log = LoggerFactory.getLogger(EntityPersister.class);

	public void persist(PersistenceContext context) {
		EntityMeta entityMeta = context.getEntityMeta();

		Object entity = context.getEntity();
		log.debug("Persisting transient entity {}", entity);

		if (entityMeta.isClusteredCounter()) {
			persistClusteredCounter(context);
		} else {
			persistEntity(context, entityMeta);
		}
	}

	private void persistEntity(PersistenceContext context, EntityMeta entityMeta) {
        context.pushInsertStatement();
		persistCounters(context, entityMeta.getAllCounterMetas());
	}

    public void remove(PersistenceContext context) {
        log.trace("Removing entity using PersistenceContext {}", context);
        EntityMeta entityMeta = context.getEntityMeta();
        if (entityMeta.isClusteredCounter()) {
            context.bindForClusteredCounterRemoval(entityMeta.getFirstMeta());
        } else {
            context.bindForRemoval(entityMeta.getTableName());
            removeRelatedCounters(context);
        }
    }

    protected void persistCounters(PersistenceContext context, List<PropertyMeta> counterMetas) {
        log.trace("Persisting counters using PersistenceContext {}",context);
        Object entity = context.getEntity();
        for (PropertyMeta counterMeta : counterMetas) {
            Object counter = counterMeta.getValueFromField(entity);
            Counter newCounter;
            if (counter != null) {
                Validator.validateTrue(CounterImpl.class.isAssignableFrom(counter.getClass()),
                                       "Counter property '%s' value from entity class '%s'  should be of type '%s'",
                                       counterMeta.getPropertyName(), counterMeta.getEntityClassName(),
                                       CounterImpl.class.getCanonicalName());
                CounterImpl counterValue = (CounterImpl) counter;
                context.bindForSimpleCounterIncrement(counterMeta, counterValue.getInternalCounterDelta());
                newCounter = initialValue(counterValue.getInternalCounterDelta());
            } else {
                newCounter = initialValue(0L);
            }
            counterMeta.setValueToField(entity,newCounter);
        }
    }

    void persistClusteredCounter(PersistenceContext context) {
        log.trace("Persisting clustered counter using PersistenceContext {}",context);
        Object entity = context.getEntity();

        int nullCount=0;
        final List<PropertyMeta> allCountersMeta = context.getAllCountersMeta();
        for(PropertyMeta counterMeta: allCountersMeta) {
            Object counter = counterMeta.getValueFromField(entity);
            if (counter != null) {
                Validator.validateTrue(CounterImpl.class.isAssignableFrom(counter.getClass()),
                                       "Counter property '%s' value from entity class '%s'  should be of type '%s'",
                                       counterMeta.getPropertyName(), counterMeta.getEntityClassName(),
                                       CounterImpl.class.getCanonicalName());
                CounterImpl counterValue = (CounterImpl) counter;
                context.pushClusteredCounterIncrementStatement(counterMeta, counterValue.getInternalCounterDelta());
                counterMeta.setValueToField(entity, initialValue(counterValue.getInternalCounterDelta()));
            } else {
                nullCount++;
            }
        }

        if(nullCount == allCountersMeta.size()) {
            throw new IllegalStateException("Cannot insert clustered counter entity '" + entity
                                                    + "' with null clustered counter value");
        }

    }

    protected void removeRelatedCounters(PersistenceContext context) {
        log.trace("Removing counter values related to entity using PersistenceContext {}", context);
        EntityMeta entityMeta = context.getEntityMeta();

        Collection<PropertyMeta> counterMetas = filter(entityMeta.getAllMetas(), counterType);
        for (PropertyMeta pm : counterMetas) {
            context.bindForSimpleCounterRemoval(pm);
        }
    }
}
