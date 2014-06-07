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

package info.archinnov.achilles.internal.context.facade;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.context.ConfigurationContext;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.type.ConsistencyLevel;
import info.archinnov.achilles.type.Options;

public interface PersistentStateHolder {

    public boolean isClusteredCounter();

    public EntityMeta getEntityMeta();

    public PropertyMeta getIdMeta();

    public Object getEntity();

    public void setEntity(Object entity);

    public <T> Class<T> getEntityClass();

    public Object getPrimaryKey();

    public Options getOptions();

    public Optional<Integer> getTtl();

    public Optional<Long> getTimestamp();

    public Optional<ConsistencyLevel> getConsistencyLevel();

    public Optional<com.datastax.driver.core.ConsistencyLevel> getSerialConsistencyLevel();

    public List<Options.CASCondition> getCasConditions();

    public boolean hasCasConditions();

    public Optional getCASResultListener();

    public Set<Method> getAllGettersExceptCounters();

    public List<PropertyMeta> getAllCountersMeta();

    public ConfigurationContext getConfigContext();

	public ExecutorService getExecutorService();
}
