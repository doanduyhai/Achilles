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
package info.archinnov.achilles.entity.metadata.transcoding;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.map.ObjectMapper;

public class CompoundTranscoder extends AbstractTranscoder {

	public CompoundTranscoder(ObjectMapper objectMapper) {
		super(objectMapper);
	}

	@Override
	public List<Object> encodeToComponents(PropertyMeta pm, Object compoundKey) {
		List<Object> compoundComponents = new ArrayList<Object>();
		List<Method> componentGetters = pm.getComponentGetters();
		List<Class<?>> componentClasses = pm.getComponentClasses();
		if (compoundKey != null) {
			for (int i = 0; i < componentGetters.size(); i++) {
				Object component = invoker.getValueFromField(compoundKey,
						componentGetters.get(i));
				Object encoded = super.encodeIgnoreJoin(
						componentClasses.get(i), component);
				compoundComponents.add(encoded);
			}
		}
		return compoundComponents;
	}

	@Override
	public List<Object> encodeComponents(PropertyMeta pm, List<?> components) {
		List<Object> encodedComponents = new ArrayList<Object>();
		List<Class<?>> componentClasses = pm.getComponentClasses();
		for (Object component : components) {
			if (component != null) {
				Class<?> componentClass = component.getClass();
				Validator
						.validateTrue(
								componentClasses.contains(componentClass),
								"The component {} for embedded id {} has an unknown type. Valid types are {}",
								component, pm.getValueClass()
										.getCanonicalName(), componentClasses);
				Object encoded = super.encodeIgnoreJoin(componentClass,
						component);
				encodedComponents.add(encoded);
			}
		}
		return encodedComponents;
	}

	@Override
	public Object decodeFromComponents(PropertyMeta pm, List<?> components) {
		Constructor<?> constructor = pm.getEmbeddedIdConstructor();
		List<Method> componentSetters = pm.getComponentSetters();

		List<Object> decodedComponents = new ArrayList<Object>();
		List<Class<?>> componentClasses = pm.getComponentClasses();
		for (int i = 0; i < components.size(); i++) {
			Object decoded = super.decodeIgnoreJoin(componentClasses.get(i),
					components.get(i));
			decodedComponents.add(decoded);
		}

		Object compoundKey;
		if (pm.hasDefaultConstructorForEmbeddedId()) {
			compoundKey = injectValuesBySetter(decodedComponents, constructor,
					componentSetters);
		} else {
			compoundKey = injectValuesByConstructor(decodedComponents,
					constructor);
		}
		return compoundKey;
	}

	private Object injectValuesBySetter(List<?> components,
			Constructor<?> constructor, List<Method> componentSetters) {

		Object compoundKey = invoker.instanciate(constructor);

		for (int i = 0; i < components.size(); i++) {
			Object compValue = components.get(i);
			invoker.setValueToField(compoundKey, componentSetters.get(i),
					compValue);
		}
		return compoundKey;
	}

	private Object injectValuesByConstructor(List<?> components,
			Constructor<?> constructor) {
		Object compoundKey;
		int componentCount = components.size();
		Object[] constructorParams = new Object[componentCount];
		for (int i = 0; i < componentCount; i++) {
			constructorParams[i] = components.get(i);
		}
		compoundKey = invoker.instanciate(constructor, constructorParams);
		return compoundKey;
	}
}
