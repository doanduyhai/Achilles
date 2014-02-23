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
package info.archinnov.achilles.internal.proxy.wrapper.builder;

import java.lang.reflect.Method;
import java.util.Map;

import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.internal.proxy.wrapper.AbstractWrapper;

@SuppressWarnings("unchecked")
public abstract class AbstractWrapperBuilder<T extends AbstractWrapperBuilder<T>> {
	private Map<Method, DirtyChecker> dirtyMap;
	private Method setter;
	private PropertyMeta propertyMeta;

	public T dirtyMap(Map<Method, DirtyChecker> dirtyMap) {
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

	public void build(AbstractWrapper wrapper) {
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
	}
}
