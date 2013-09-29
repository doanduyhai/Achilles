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
package info.archinnov.achilles.entity.context;

import static org.mockito.Mockito.*;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.context.ThriftImmediateFlushContext;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftCounterDao;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.dao.ThriftGenericWideRowDao;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.type.OptionsBuilder;

import java.util.HashMap;
import java.util.Map;

import org.powermock.reflect.Whitebox;

public class ThriftPersistenceContextTestBuilder {
	private EntityMeta entityMeta;
	private Map<String, ThriftGenericEntityDao> entityDaosMap = new HashMap<String, ThriftGenericEntityDao>();
	private Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap = new HashMap<String, ThriftGenericWideRowDao>();
	private ThriftCounterDao counterDao;
	private ThriftConsistencyLevelPolicy policy;
	private Object entity;
	private Class<?> entityClass;
	private Object primaryKey;
	private ThriftGenericEntityDao entityDao;
	private ThriftGenericWideRowDao wideRowDao;

	private ThriftImmediateFlushContext thriftImmediateFlushContext;

	public static ThriftPersistenceContextTestBuilder context(EntityMeta entityMeta,//
			ThriftCounterDao thriftCounterDao, //
			ThriftConsistencyLevelPolicy policy, //
			Class<?> entityClass, Object primaryKey) {
		return new ThriftPersistenceContextTestBuilder(entityMeta, thriftCounterDao, policy, entityClass, primaryKey);
	}

	public static ThriftPersistenceContextTestBuilder mockAll(EntityMeta entityMeta, Class<?> entityClass,
			Object primaryKey) {
		return new ThriftPersistenceContextTestBuilder(entityMeta, mock(ThriftCounterDao.class),
				mock(ThriftConsistencyLevelPolicy.class), entityClass, primaryKey);
	}

	public ThriftPersistenceContextTestBuilder(EntityMeta entityMeta, ThriftCounterDao counterDao,
			ThriftConsistencyLevelPolicy policy, Class<?> entityClass, Object primaryKey) {
		this.entityMeta = entityMeta;
		this.counterDao = counterDao;
		this.policy = policy;
		this.entityClass = entityClass;
		this.primaryKey = primaryKey;
	}

	public ThriftPersistenceContext build() {
		ThriftDaoContext thriftDaoContext = new ThriftDaoContext(entityDaosMap, columnFamilyDaosMap, counterDao);
		ConfigurationContext configContext = new ConfigurationContext();
		configContext.setConsistencyPolicy(policy);
		ThriftPersistenceContext context = new ThriftPersistenceContext(entityMeta, //
				configContext, //
				thriftDaoContext, //
				thriftImmediateFlushContext, //
				entityClass, primaryKey, OptionsBuilder.noOptions());

		context.setEntity(entity);
		Whitebox.setInternalState(context, ThriftGenericEntityDao.class, entityDao);
		Whitebox.setInternalState(context, ThriftGenericWideRowDao.class, wideRowDao);
		return context;
	}

	public ThriftPersistenceContextTestBuilder entityDaosMap(Map<String, ThriftGenericEntityDao> entityDaosMap) {
		this.entityDaosMap = entityDaosMap;
		return this;
	}

	public ThriftPersistenceContextTestBuilder wideRowDaosMap(Map<String, ThriftGenericWideRowDao> columnFamilyDaosMap) {
		this.columnFamilyDaosMap = columnFamilyDaosMap;
		return this;
	}

	public ThriftPersistenceContextTestBuilder entity(Object entity) {
		this.entity = entity;
		return this;
	}

	public ThriftPersistenceContextTestBuilder entityDao(ThriftGenericEntityDao entityDao) {
		this.entityDao = entityDao;
		return this;
	}

	public ThriftPersistenceContextTestBuilder wideRowDao(ThriftGenericWideRowDao columnFamilyDao) {
		this.wideRowDao = columnFamilyDao;
		return this;
	}

	public ThriftPersistenceContextTestBuilder thriftImmediateFlushContext(
			ThriftImmediateFlushContext thriftImmediateFlushContext) {
		this.thriftImmediateFlushContext = thriftImmediateFlushContext;
		return this;
	}
}
