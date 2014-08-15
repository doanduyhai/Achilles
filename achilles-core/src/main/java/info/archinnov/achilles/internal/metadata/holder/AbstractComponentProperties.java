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

package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.internal.helper.LoggerHelper.fqcnToStringFn;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang.ClassUtils;
import org.apache.commons.lang.StringUtils;
import com.google.common.base.Objects;
import com.google.common.collect.Lists;

public abstract class AbstractComponentProperties {

	protected final List<Class<?>> componentClasses;
	protected final List<String> componentNames;
	protected final List<Field> componentFields;
	protected final List<Method> componentGetters;
	protected final List<Method> componentSetters;

	protected AbstractComponentProperties(List<Class<?>> componentClasses, List<String> componentNames,
			List<Field> componentFields,List<Method> componentGetters, List<Method> componentSetters) {
		this.componentClasses = componentClasses;
		this.componentNames = componentNames;
        this.componentFields = componentFields;
        this.componentGetters = componentGetters;
		this.componentSetters = componentSetters;
	}

	public List<Class<?>> getComponentClasses() {
		return componentClasses;
	}

    public List<String> getComponentNames() {
		return componentNames;
	}

    public List<Method> getComponentGetters() {
		return componentGetters;
	}

    public List<Method> getComponentSetters() {
		return componentSetters;
	}

    public List<Field> getComponentFields() {
        return componentFields;
    }

    protected static boolean isCompatibleClass(Class<?> expected, Class<?> given)
    {
        expected= ClassUtils.primitiveToWrapper(expected);
        return (expected==given || expected.isAssignableFrom(given));
    }

    @Override
	public String toString() {

		return Objects.toStringHelper(this.getClass())
				.add("componentClasses", StringUtils.join(Lists.transform(componentClasses, fqcnToStringFn), ","))
				.add("componentNames", componentNames).toString();

	}
}
