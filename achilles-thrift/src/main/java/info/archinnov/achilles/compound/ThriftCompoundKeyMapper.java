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

import static info.archinnov.achilles.logger.ThriftLoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
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

	private static final Logger log = LoggerFactory
			.getLogger(ThriftCompoundKeyMapper.class);
	private static final ClassToSerializerTransformer classToSerializer = new ClassToSerializerTransformer();

	private ThriftCompoundKeyValidator validator = new ThriftCompoundKeyValidator();

	public <V> V fromCompositeToEmbeddedId(PropertyMeta pm,
			List<Component<?>> components, Object partitionKey) {
		if (log.isTraceEnabled()) {
			log.trace("Build compound key {} from composite components {}",
					pm.getPropertyName(), format(components));
		}

		V compoundKey;
		List<Class<?>> componentClasses = pm.getComponentClasses().subList(1,
				pm.getComponentClasses().size());

		List<Serializer<Object>> serializers = FluentIterable
				.from(componentClasses).transform(classToSerializer)
				.toImmutableList();

		int componentCount = components.size();

		List<Object> componentValues = new ArrayList<Object>();
		componentValues.add(partitionKey);
		for (int i = 0; i < componentCount; i++) {
			Component<?> comp = components.get(i);
			componentValues.add(serializers.get(i).fromByteBuffer(
					comp.getBytes()));
		}

		compoundKey = (V) pm.decodeFromComponents(componentValues);

		log.trace("Built compound key : {}", compoundKey);

		return compoundKey;
	}

	public Composite fromCompoundToCompositeForInsertOrGet(Object compoundKey,
			PropertyMeta pm) {
		log.trace("Build composite from key {} to persist @CompoundKey {} ",
				compoundKey, pm.getPropertyName());

		List<Object> components = pm.encodeToComponents(compoundKey);
		return fromComponentsToCompositeForInsertOrGet(components, pm);

	}

	protected Composite fromComponentsToCompositeForInsertOrGet(
			List<Object> components, PropertyMeta pm) {
		String propertyName = pm.getPropertyName();

		Validator
				.validateNotNull(
						components,
						"The component values for the @CompoundKey '%s' should not be null",
						propertyName);
		Validator
				.validateNotEmpty(
						components,
						"The component values for the @CompoundKey '%s' should not be empty",
						propertyName);
		for (Object value : components) {
			Validator
					.validateNotNull(
							value,
							"The component values for the @CompoundKey '%s' should not be null",
							propertyName);
		}

		Composite composite = new Composite();
		List<Object> columnComponents = components
				.subList(1, components.size());
		List<Class<?>> columnClasses = pm.getComponentClasses().subList(1,
				pm.getComponentClasses().size());

		log.trace(
				"Build composite from components {} to persist @CompoundKey {} ",
				columnComponents, propertyName);

		List<Serializer<Object>> serializers = FluentIterable
				.from(columnClasses).transform(classToSerializer)
				.toImmutableList();
		int srzCount = serializers.size();

		for (Object value : columnComponents) {
			Validator.validateNotNull(value,
					"The values for the @CompoundKey '%s' should not be null",
					propertyName);
		}

		for (int i = 0; i < srzCount; i++) {
			Serializer<Object> srz = serializers.get(i);
			composite.setComponent(i, columnComponents.get(i), srz, srz
					.getComparatorType().getTypeName());
		}
		return composite;
	}

	public Composite fromComponentsToCompositeForQuery(List<Object> components,
			PropertyMeta pm, ComponentEquality equality) {
		String propertyName = pm.getPropertyName();

		List<Object> columnComponents = components
				.subList(1, components.size());
		List<Class<?>> columnClasses = pm.getComponentClasses().subList(1,
				pm.getComponentClasses().size());

		log.trace(
				"Build composite from components {} to query @CompoundKey {} ",
				columnComponents, propertyName);

		Composite composite = new Composite();

		List<Serializer<Object>> serializers = FluentIterable
				.from(columnClasses).transform(classToSerializer)
				.toImmutableList();

		int srzCount = serializers.size();

		Validator.validateTrue(srzCount >= columnComponents.size(),
				"There should be at most %s values for the @CompoundKey '%s'",
				srzCount, propertyName);

		int lastNotNullIndex = validator
				.validateNoHoleAndReturnLastNonNullIndex(columnComponents);

		for (int i = 0; i <= lastNotNullIndex; i++) {
			Serializer<Object> srz = serializers.get(i);
			Object value = columnComponents.get(i);

			if (i < lastNotNullIndex) {
				composite.setComponent(i, value, srz, srz.getComparatorType()
						.getTypeName(), EQUAL);
			} else {
				composite.setComponent(i, value, srz, srz.getComparatorType()
						.getTypeName(), equality);
			}
		}
		return composite;
	}
}
