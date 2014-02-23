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
package info.archinnov.achilles.internal.proxy.wrapper;

import java.lang.reflect.Method;
import java.util.Map;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;

public abstract class AbstractWrapper {
	protected Map<Method, DirtyChecker> dirtyMap;
	protected Method setter;
	protected PropertyMeta propertyMeta;
	protected EntityProxifier proxifier = new EntityProxifier();


	public Map<Method, DirtyChecker> getDirtyMap() {
		return dirtyMap;
	}

	public void setDirtyMap(Map<Method, DirtyChecker> dirtyMap) {
		this.dirtyMap = dirtyMap;
	}

	public void setSetter(Method setter) {
		this.setter = setter;
	}

	public void setPropertyMeta(PropertyMeta propertyMeta) {
		this.propertyMeta = propertyMeta;
	}

    void setProxifier(EntityProxifier proxifier) {
        this.proxifier = proxifier;
    }

    protected abstract DirtyChecker getDirtyChecker();
}
