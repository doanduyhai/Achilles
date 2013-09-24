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
package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.helper.LoggerHelper.fqcnToStringFn;

import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Objects;
import com.google.common.collect.Lists;

public class EmbeddedIdProperties {
	private List<Class<?>> componentClasses;
	private List<String> componentNames;
	private List<Method> componentGetters;
	private List<Method> componentSetters;
	private List<String> timeUUIDComponents;

	public String getOrderingComponent() {
		String component = null;
		if (componentNames.size() > 1) {
			return componentNames.get(1);
		}
		return component;
	}

	public List<String> getClusteringComponentNames() {
		return componentNames.subList(1, componentNames.size());
	}

	public List<Class<?>> getClusteringComponentClasses() {
		return componentClasses.subList(1, componentClasses.size());
	}

	public List<Class<?>> getComponentClasses() {
		return componentClasses;
	}

	public void setComponentClasses(List<Class<?>> componentClasses) {
		this.componentClasses = componentClasses;
	}

	public List<Method> getComponentGetters() {
		return componentGetters;
	}

	public void setComponentGetters(List<Method> componentGetters) {
		this.componentGetters = componentGetters;
	}

	public List<Method> getComponentSetters() {
		return componentSetters;
	}

	public void setComponentSetters(List<Method> componentSetters) {
		this.componentSetters = componentSetters;
	}

	public List<String> getComponentNames() {
		return componentNames;
	}

	public void setComponentNames(List<String> componentNames) {
		this.componentNames = componentNames;
	}

	public List<String> getTimeUUIDComponents() {
		return timeUUIDComponents;
	}

	public void setTimeUUIDComponents(List<String> timeUUIDComponents) {
		this.timeUUIDComponents = timeUUIDComponents;
	}

	@Override
	public String toString() {

		return Objects
				.toStringHelper(this.getClass())
				.add("componentClasses",
						StringUtils.join(Lists.transform(componentClasses,
								fqcnToStringFn), ","))
				.add("componentNames", componentNames).toString();

	}

}
