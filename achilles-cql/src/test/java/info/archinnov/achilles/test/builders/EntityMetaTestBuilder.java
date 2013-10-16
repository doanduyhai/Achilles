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
package info.archinnov.achilles.test.builders;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.apache.cassandra.utils.Pair;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

public class EntityMetaTestBuilder<ID> {

	private PropertyMeta idMeta;
	private String classname;
	private String columnFamilyName;
	private Map<String, PropertyMeta> propertyMetas = new HashMap<String, PropertyMeta>();
	private Map<Method, PropertyMeta> getterMetas = new HashMap<Method, PropertyMeta>();
	private Map<Method, PropertyMeta> setterMetas = new HashMap<Method, PropertyMeta>();
	private boolean columnFamilyDirectMapping = false;
	private Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels;

	public static <ID> EntityMetaTestBuilder<ID> builder(PropertyMeta idMeta) {
		return new EntityMetaTestBuilder<ID>(idMeta);
	}

	public EntityMeta build() {
		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(idMeta);
		meta.setClassName(classname);
		meta.setTableName(columnFamilyName);
		meta.setIdClass(idMeta.getValueClass());
		meta.setPropertyMetas(propertyMetas);
		meta.setGetterMetas(getterMetas);
		meta.setClusteredEntity(columnFamilyDirectMapping);
		meta.setConsistencyLevels(consistencyLevels);

		return meta;
	}

	public EntityMetaTestBuilder(PropertyMeta idMeta) {
		this.idMeta = idMeta;
	}

	public EntityMetaTestBuilder<ID> classname(String classname) {
		this.classname = classname;
		return this;
	}

	public EntityMetaTestBuilder<ID> columnFamilyName(String columnFamilyName) {
		this.columnFamilyName = columnFamilyName;
		return this;
	}

	public EntityMetaTestBuilder<ID> propertyMetas(Map<String, PropertyMeta> propertyMetas) {
		this.propertyMetas = propertyMetas;
		return this;
	}

	public EntityMetaTestBuilder<ID> columnFamilyDirectMapping(boolean columnFamilyDirectMapping) {
		this.columnFamilyDirectMapping = columnFamilyDirectMapping;
		return this;
	}

	public EntityMetaTestBuilder<ID> consistencyLevels(Pair<ConsistencyLevel, ConsistencyLevel> consistencyLevels) {
		this.consistencyLevels = consistencyLevels;
		return this;
	}

	public <K, V> EntityMetaTestBuilder<ID> addPropertyMeta(PropertyMeta propertyMeta) {
		this.propertyMetas.put(propertyMeta.getPropertyName(), propertyMeta);
		return this;
	}

	public <T, K, V> EntityMetaTestBuilder<ID> addGetter(Class<T> targetClass, String getter, PropertyMeta propertyMeta)
			throws SecurityException, NoSuchMethodException {
		Method getterMethod = targetClass.getDeclaredMethod(getter);
		getterMetas.put(getterMethod, propertyMeta);
		return this;
	}

	public <T, S, K, V> EntityMetaTestBuilder<ID> addSetter(Class<T> targetClass, String setter, Class<S> type,
			PropertyMeta propertyMeta) throws SecurityException, NoSuchMethodException {
		Method setterMethod = targetClass.getDeclaredMethod(setter, type);
		setterMetas.put(setterMethod, propertyMeta);
		return this;
	}

}
