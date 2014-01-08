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
package info.archinnov.achilles.internal.persistence.metadata;

import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.counterType;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.reflection.ReflectionInvoker;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;

public class EntityMeta {

	public static final Predicate<EntityMeta> CLUSTERED_COUNTER_FILTER = new Predicate<EntityMeta>() {
		@Override
		public boolean apply(EntityMeta meta) {
			return meta.isClusteredCounter();
		}
	};

	public static final Predicate<EntityMeta> EXCLUDE_CLUSTERED_COUNTER_FILTER = new Predicate<EntityMeta>() {
		@Override
		public boolean apply(EntityMeta meta) {
			return !meta.isClusteredCounter();
		}
	};

	private ReflectionInvoker invoker = new ReflectionInvoker();

	private Class<?> entityClass;
	private String className;
	private String tableName;
	private Class<?> idClass;
	private Map<String, PropertyMeta> propertyMetas;
    private List<PropertyMeta> allMetasExceptCounters;
    private List<PropertyMeta> allMetasExceptIdAndCounters;
	private PropertyMeta idMeta;
	private Map<Method, PropertyMeta> getterMetas;
	private Map<Method, PropertyMeta> setterMetas;
	private boolean clusteredEntity = false;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;
	private PropertyMeta firstMeta;
	private List<PropertyMeta> allMetasExceptId;
	private boolean clusteredCounter = false;
	private List<Interceptor<?>> interceptors = new ArrayList<>();

	public Object getPrimaryKey(Object entity) {
		return idMeta.getPrimaryKey(entity);
	}

	public void addInterceptor(Interceptor<?> interceptor) {
		interceptors.add(interceptor);
	}

	public List<Interceptor<?>> getInterceptors() {
		return interceptors;
	}


    public void intercept(Object entity, Event event) {
        List<Interceptor<?>> interceptors = getInterceptorsForEvent(event);
        if (interceptors.size() > 0) {
            for (Interceptor interceptor : interceptors) {
                interceptor.onEvent(entity);
            }
            Validator.validateNotNull(getPrimaryKey(entity),"The primary key should not be null after intercepting the event '%s'",event);
        }
    }

	protected List<Interceptor<?>> getInterceptorsForEvent(final Event event) {
		return FluentIterable.from(interceptors).filter(getFilterForEvent(event)).toImmutableList();

	}

	private Predicate<? super Interceptor<?>> getFilterForEvent(final Event event) {
		return new Predicate<Interceptor<?>>() {
			public boolean apply(Interceptor<?> p) {
				return p != null && p.events() != null && p.events().contains(event);
			}
		};
	}


	public Object getPartitionKey(Object compoundKey) {
		return idMeta.getPartitionKey(compoundKey);
	}

	@SuppressWarnings("unchecked")
	public <T> T instanciate() {
		return (T) invoker.instantiate(entityClass);
	}

	public boolean hasEmbeddedId() {
		return idMeta.isEmbeddedId();
	}



	// ////////// Getters & Setters
	@SuppressWarnings("unchecked")
	public <T> Class<T> getEntityClass() {
		return (Class<T>) entityClass;
	}

	public void setEntityClass(Class<?> entityClass) {
		this.entityClass = entityClass;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public Map<String, PropertyMeta> getPropertyMetas() {
		return propertyMetas;
	}

	public void setPropertyMetas(Map<String, PropertyMeta> propertyMetas) {
		this.propertyMetas = propertyMetas;
	}

	public PropertyMeta getIdMeta() {
		return idMeta;
	}

	public void setIdMeta(PropertyMeta idMeta) {
		this.idMeta = idMeta;
	}

	public Map<Method, PropertyMeta> getGetterMetas() {
		return getterMetas;
	}

	public void setGetterMetas(Map<Method, PropertyMeta> getterMetas) {
		this.getterMetas = getterMetas;
	}

	public Map<Method, PropertyMeta> getSetterMetas() {
		return setterMetas;
	}

	public void setSetterMetas(Map<Method, PropertyMeta> setterMetas) {
		this.setterMetas = setterMetas;
	}

	public boolean isClusteredEntity() {
		return clusteredEntity;
	}

	public void setClusteredEntity(boolean clusteredEntity) {
		this.clusteredEntity = clusteredEntity;
	}

	public ConsistencyLevel getReadConsistencyLevel() {
		return this.consistencyLevels.left;
	}

	public ConsistencyLevel getWriteConsistencyLevel() {
		return this.consistencyLevels.right;
	}

	public Pair<ConsistencyLevel, ConsistencyLevel> getConsistencyLevels() {
		return this.consistencyLevels;
	}

	public void setConsistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
		this.consistencyLevels = consistencyLevels;
	}

	@SuppressWarnings("unchecked")
	public <T> Class<T> getIdClass() {
		return (Class<T>) idClass;
	}

	public void setIdClass(Class<?> idClass) {
		this.idClass = idClass;
	}

	public List<PropertyMeta> getAllMetas() {
		return new ArrayList(propertyMetas.values());
	}


    public List<PropertyMeta> getAllCounterMetas() {
        return from(propertyMetas.values()).filter(counterType)
                .toImmutableList();
    }

	public List<PropertyMeta> getAllMetasExceptId() {
		return this.allMetasExceptId;
	}

	public void setAllMetasExceptId(List<PropertyMeta> allMetasExceptIdMeta) {
		this.allMetasExceptId = allMetasExceptIdMeta;
	}

	public boolean isClusteredCounter() {
		return this.clusteredCounter;
	}

	public void setClusteredCounter(boolean clusteredCounter) {
		this.clusteredCounter = clusteredCounter;
	}

	public boolean isValueless() {
		return propertyMetas.size() == 1;
	}

    public List<PropertyMeta> getAllMetasExceptIdAndCounters() {
        return allMetasExceptIdAndCounters;
    }

    public void setAllMetasExceptIdAndCounters(List<PropertyMeta> allMetasExceptIdAndCounters) {
        this.allMetasExceptIdAndCounters = allMetasExceptIdAndCounters;
    }

    public List<PropertyMeta> getAllMetasExceptCounters() {
        return allMetasExceptCounters;
    }

    public void setAllMetasExceptCounters(List<PropertyMeta> allMetasExceptCounters) {
        this.allMetasExceptCounters = allMetasExceptCounters;
    }

    public List<PropertyMeta> getColumnsMetaToInsert() {
        if(clusteredCounter) {
           return allMetasExceptId;
        } else {
           return allMetasExceptIdAndCounters;
        }
    }

    public List<PropertyMeta> getColumnsMetaToLoad() {
        if(clusteredCounter) {
            return new ArrayList(propertyMetas.values());
        } else {
            return allMetasExceptCounters;
        }
    }

    @Override
	public String toString() {

		return Objects.toStringHelper(this.getClass()).add("className", className)
				.add("tableName/columnFamilyName", tableName)
				.add("propertyMetas", StringUtils.join(propertyMetas.keySet(), ",")).add("idMeta", idMeta)
				.add("clusteredEntity", clusteredEntity).add("consistencyLevels", consistencyLevels).toString();
	}
}
