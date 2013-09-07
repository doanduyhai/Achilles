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
package info.archinnov.achilles.proxy;

import info.archinnov.achilles.composite.ThriftCompositeFactory;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.dao.ThriftAbstractDao;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.operations.ThriftEntityPersister;
import info.archinnov.achilles.entity.operations.ThriftEntityProxifier;
import info.archinnov.achilles.proxy.wrapper.builder.ThriftCounterWrapperBuilder;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Counter;
import me.prettyprint.hector.api.beans.Composite;

public class ThriftEntityInterceptor<T> extends
		EntityInterceptor<ThriftPersistenceContext, T> {

	private ThriftCompositeFactory thriftCompositeFactory = new ThriftCompositeFactory();
	private ReflectionInvoker invoker = new ReflectionInvoker();

	public ThriftEntityInterceptor() {
		super.loader = new ThriftEntityLoader();
		super.persister = new ThriftEntityPersister();
		super.proxifier = new ThriftEntityProxifier();
	}

	@Override
	protected Counter buildCounterWrapper(PropertyMeta propertyMeta) {
		Counter result;
		Object rowKey;
		Composite comp;
		ThriftAbstractDao counterDao;
		CounterProperties counterProperties = propertyMeta
				.getCounterProperties();
		PropertyMeta idMeta = counterProperties.getIdMeta();
		if (context.isClusteredEntity()) {
			rowKey = invoker.getPartitionKey(primaryKey, idMeta);
			comp = thriftCompositeFactory.createBaseForClusteredGet(primaryKey,
					idMeta);
			counterDao = context.getWideRowDao();
		} else {
			rowKey = thriftCompositeFactory.createKeyForCounter(
					counterProperties.getFqcn(), primaryKey, idMeta);
			comp = thriftCompositeFactory.createBaseForCounterGet(propertyMeta);
			counterDao = context.getCounterDao();
		}

		ConsistencyLevel consistencyLevel = context.getConsistencyLevel()
				.isPresent() ? context.getConsistencyLevel().get()
				: propertyMeta.getReadConsistencyLevel();

		result = ThriftCounterWrapperBuilder.builder(context)
				//
				.counterDao(counterDao).columnName(comp)
				.consistencyLevel(consistencyLevel).key(rowKey).build();
		return result;
	}
}
