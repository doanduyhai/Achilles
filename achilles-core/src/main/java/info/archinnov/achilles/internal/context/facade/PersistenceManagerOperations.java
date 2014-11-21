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

import java.util.List;
import java.util.Set;
import info.archinnov.achilles.async.AchillesFuture;
import info.archinnov.achilles.exception.AchillesStaleObjectStateException;
import info.archinnov.achilles.type.Empty;

public interface PersistenceManagerOperations extends PersistentStateHolder {

    public <T> AchillesFuture<T> persist(T rawEntity);

    public <T> AchillesFuture<T> batchPersist(T rawEntity);

    public <T> AchillesFuture<T> update(T proxy);

    public <T> AchillesFuture<T> delete();

    public AchillesFuture<Empty> deleteById();

    public <T> AchillesFuture<T> find(Class<T> entityClass);

    public <T> T getProxy(Class<T> entityClass);

    public <T> AchillesFuture<T> refresh(T proxy) throws AchillesStaleObjectStateException;

    public <T> T initialize(T proxy);

    public <T> List<T> initialize(List<T> proxies);

    public <T> Set<T> initialize(Set<T> proxies);

}
