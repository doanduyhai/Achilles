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
package info.archinnov.achilles.context;

import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.json.ObjectMapperFactory;

public class ConfigurationContext {
	private boolean forceColumnFamilyCreation;

	private boolean ensureJoinConsistency;

	private AchillesConsistencyLevelPolicy consistencyPolicy;

	private ObjectMapperFactory objectMapperFactory;

	private Impl impl;

	public boolean isForceColumnFamilyCreation() {
		return forceColumnFamilyCreation;
	}

	public void setForceColumnFamilyCreation(boolean forceColumnFamilyCreation) {
		this.forceColumnFamilyCreation = forceColumnFamilyCreation;
	}

	public boolean isEnsureJoinConsistency() {
		return ensureJoinConsistency;
	}

	public void setEnsureJoinConsistency(boolean ensureJoinConsistency) {
		this.ensureJoinConsistency = ensureJoinConsistency;
	}

	public AchillesConsistencyLevelPolicy getConsistencyPolicy() {
		return consistencyPolicy;
	}

	public void setConsistencyPolicy(
			AchillesConsistencyLevelPolicy consistencyPolicy) {
		this.consistencyPolicy = consistencyPolicy;
	}

	public ObjectMapperFactory getObjectMapperFactory() {
		return objectMapperFactory;
	}

	public void setObjectMapperFactory(ObjectMapperFactory objectMapperFactory) {
		this.objectMapperFactory = objectMapperFactory;
	}

	public Impl getImpl() {
		return impl;
	}

	public void setImpl(Impl impl) {
		this.impl = impl;
	}

	public static enum Impl {
		THRIFT, CQL;
	}
}
