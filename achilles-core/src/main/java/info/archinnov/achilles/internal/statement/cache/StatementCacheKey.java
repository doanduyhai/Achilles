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
package info.archinnov.achilles.internal.statement.cache;

import java.util.Set;

public class StatementCacheKey {
	private CacheType type;

	private Set<String> fields;

	private Class<?> entityClass;

	public StatementCacheKey(CacheType type, Set<String> fields, Class<?> entityClass) {
		this.type = type;
		this.entityClass = entityClass;
		this.fields = fields;
	}

	public CacheType getType() {
		return type;
	}

	public Set<String> getFields() {
		return fields;
	}

	@SuppressWarnings("unchecked")
	public <T> Class<T> getEntityClass() {
		return (Class<T>) entityClass;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((fields == null) ? 0 : fields.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatementCacheKey that = (StatementCacheKey) o;

        return entityClass.equals(that.entityClass)
                && fields.equals(that.fields)
                && type == that.type;

    }
}
