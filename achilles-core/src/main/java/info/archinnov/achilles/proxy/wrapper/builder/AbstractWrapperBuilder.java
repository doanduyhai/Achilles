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
package info.archinnov.achilles.proxy.wrapper.builder;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.proxy.wrapper.AbstractWrapper;

import java.lang.reflect.Method;
import java.util.Map;

@SuppressWarnings("unchecked")
public abstract class AbstractWrapperBuilder<T extends AbstractWrapperBuilder<T>> {
	private Map<Method, PropertyMeta> dirtyMap;
	private Method setter;
	private PropertyMeta propertyMeta;
	private EntityProxifier<PersistenceContext> proxifier;
	protected PersistenceContext context;

	public T dirtyMap(Map<Method, PropertyMeta> dirtyMap) {
		this.dirtyMap = dirtyMap;
		return (T) this;
	}

	public T setter(Method setter) {
		this.setter = setter;
		return (T) this;
	}

	public T propertyMeta(PropertyMeta propertyMeta) {
		this.propertyMeta = propertyMeta;
		return (T) this;
	}

	@SuppressWarnings("rawtypes")
	public T proxifier(EntityProxifier proxifier) {
		this.proxifier = proxifier;
		return (T) this;
	}

	public T context(PersistenceContext context) {
		this.context = context;
		return (T) this;
	}

	public void build(AbstractWrapper wrapper) {
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setProxifier(proxifier);
		wrapper.setContext(context);
	}
}
