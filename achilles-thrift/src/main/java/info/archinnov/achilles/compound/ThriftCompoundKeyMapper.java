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
package info.archinnov.achilles.compound;

import static info.archinnov.achilles.logger.ThriftLoggerHelper.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.validation.Validator;

import java.util.ArrayList;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.Component;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.FluentIterable;

public class ThriftCompoundKeyMapper {

	private static final Logger log = LoggerFactory.getLogger(ThriftCompoundKeyMapper.class);
	private static final ClassToSerializerTransformer classToSerializer = new ClassToSerializerTransformer();

	private ThriftSliceQueryValidator validator = new ThriftSliceQueryValidator();

	public Object fromCompositeToEmbeddedId(PropertyMeta idMeta, List<Component<?>> components, Object primaryKey) {
		if (log.isTraceEnabled()) {
			log.trace("Build compound primary key {} from composite components {}", idMeta.getPropertyName(),
					format(components));
		}

		List<Object> partitionComponents = idMeta.extractPartitionComponents(primaryKey);

		Object compoundPrimaryKey;
		List<Class<?>> componentClasses = idMeta.getClusteringComponentClasses();
		List<Serializer<Object>> serializers = FluentIterable.from(componentClasses).transform(classToSerializer)
				.toImmutableList();

		List<Object> componentValues = new ArrayList<Object>(partitionComponents);
		for (int i = 0; i < components.size(); i++) {
			Component<?> comp = components.get(i);
			componentValues.add(serializers.get(i).fromByteBuffer(comp.getBytes()));
		}

		compoundPrimaryKey = idMeta.decodeFromComponents(componentValues);

		log.trace("Built compound primary key : {}", compoundPrimaryKey);

		return compoundPrimaryKey;
	}

	public Composite fromCompoundToCompositeForInsertOrGet(Object compoundPrimaryKey, PropertyMeta pm) {
		log.trace("Build composite from key {} to persist @EmbeddedId {} ", compoundPrimaryKey, pm.getPropertyName());

		List<Object> components = pm.encodeToComponents(compoundPrimaryKey);
		return fromComponentsToCompositeForInsertOrGet(components, pm, false);
	}

	public Object buildRowKey(ThriftPersistenceContext context) {
		Object rowKey;
		PropertyMeta idMeta = context.getIdMeta();
		Object primaryKey = context.getPrimaryKey();
		if (idMeta.isCompositePartitionKey()) {
			List<Object> components = idMeta.encodeToComponents(primaryKey);
			rowKey = fromComponentsToCompositeForInsertOrGet(components, idMeta, true);
		} else if (idMeta.isEmbeddedId()) {
			rowKey = idMeta.getPartitionKey(primaryKey);
		} else {
			rowKey = primaryKey;
		}
		return rowKey;
	}

	protected Composite fromComponentsToCompositeForInsertOrGet(List<Object> components, PropertyMeta pm,
			boolean partitionComponents) {
		String propertyName = pm.getPropertyName();

		Validator.validateNotNull(components, "The component values for the @EmbeddedId '%s' should not be null",
				propertyName);
		Validator.validateNotEmpty(components, "The component values for the @EmbeddedId '%s' should not be empty",
				propertyName);

		Composite composite = new Composite();
		List<Object> componentValues;
		List<Class<?>> componentClasses;

		if (partitionComponents) {
			componentValues = pm.extractPartitionComponents(components);
			componentClasses = pm.getPartitionComponentClasses();
		} else {
			for (Object value : components) {
				Validator.validateNotNull(value, "The component values for the @EmbeddedId '%s' should not be null",
						propertyName);
			}
			componentValues = pm.extractClusteringComponents(components);
			componentClasses = pm.getClusteringComponentClasses();
		}

		log.trace("Build composite from components {} to persist @EmbeddedId {} ", componentValues, propertyName);

		List<Serializer<Object>> serializers = FluentIterable.from(componentClasses).transform(classToSerializer)
				.toImmutableList();
		int srzCount = serializers.size();

		for (Object value : componentValues) {
			Validator.validateNotNull(value, "The values for the @EmbeddedId '%s' should not be null", propertyName);
		}

		for (int i = 0; i < srzCount; i++) {
			Serializer<Object> srz = serializers.get(i);
			composite.setComponent(i, componentValues.get(i), srz, srz.getComparatorType().getTypeName());
		}
		return composite;
	}

	public Composite fromComponentsToCompositeForQuery(List<Object> components, PropertyMeta idMeta,
			ComponentEquality equality) {
		String propertyName = idMeta.getPropertyName();

		List<Object> columnComponents = idMeta.extractClusteringComponents(components);
		List<Class<?>> columnClasses = idMeta.getClusteringComponentClasses();

		log.trace("Build composite from components {} to query @EmbeddedId {} ", columnComponents, propertyName);

		Composite composite = new Composite();

		List<Serializer<Object>> serializers = FluentIterable.from(columnClasses).transform(classToSerializer)
				.toImmutableList();

		int srzCount = serializers.size();

		Validator.validateTrue(srzCount >= columnComponents.size(),
				"There should be at most %s values for the @EmbeddedId '%s'", srzCount, propertyName);

		int lastNotNullIndex = validator.getLastNonNullIndex(columnComponents);

		for (int i = 0; i <= lastNotNullIndex; i++) {
			Serializer<Object> srz = serializers.get(i);
			Object value = columnComponents.get(i);

			if (i < lastNotNullIndex) {
				composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(), EQUAL);
			} else {
				composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(), equality);
			}
		}
		return composite;
	}
}
