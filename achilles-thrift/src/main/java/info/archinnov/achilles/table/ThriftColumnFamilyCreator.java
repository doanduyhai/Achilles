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
package info.archinnov.achilles.table;

import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.validation.Validator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftColumnFamilyCreator extends TableCreator {
	private static final Logger log = LoggerFactory.getLogger(ThriftColumnFamilyCreator.class);
	private Cluster cluster;
	private Keyspace keyspace;
	private ThriftColumnFamilyFactory columnFamilyFactory = new ThriftColumnFamilyFactory();
	private ThriftColumnFamilyValidator columnFamilyValidator = new ThriftColumnFamilyValidator();

	private List<ColumnFamilyDefinition> cfDefs;
	private Set<String> columnFamilyNames = new HashSet<String>();

	public ThriftColumnFamilyCreator(Cluster cluster, Keyspace keyspace) {
		this.cluster = cluster;
		this.keyspace = keyspace;
		KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspace.getKeyspaceName());

		Validator.validateNotNull(keyspaceDef, "The keyspace '%s' provided by configuration does not exist",
				keyspace.getKeyspaceName());

		if (keyspaceDef != null && keyspaceDef.getCfDefs() != null) {
			cfDefs = keyspaceDef.getCfDefs();
		}

	}

	@Override
	protected void validateOrCreateTableForEntity(EntityMeta entityMeta, boolean forceTableCreation) {
		String tableName = entityMeta.getTableName();
		ColumnFamilyDefinition cfDef = this.discoverTable(tableName);
		if (cfDef == null) {
			if (forceTableCreation) {
				log.debug("Force creation of column family for entityMeta {}", entityMeta.getClassName());

				createTable(entityMeta);
			} else {
				throw new AchillesInvalidTableException("The required column family '" + tableName
						+ "' does not exist for entity '" + entityMeta.getClassName() + "'");
			}
		} else {
			if (entityMeta.isClusteredEntity()) {
				columnFamilyValidator.validateCFForClusteredEntity(cfDef, entityMeta);
			} else {
				columnFamilyValidator.validateCFForEntity(cfDef, entityMeta);
			}
		}
	}

	@Override
	protected void validateOrCreateTableForCounter(boolean forceColumnFamilyCreation) {
		ColumnFamilyDefinition cfDef = this.discoverTable(AchillesCounter.THRIFT_COUNTER_CF);
		if (cfDef == null) {
			if (forceColumnFamilyCreation) {
				log.debug("Force creation of column family for counters");

				this.createCounterColumnFamily();
			} else {
				throw new AchillesInvalidTableException("The required column family '"
						+ AchillesCounter.THRIFT_COUNTER_CF + "' does not exist");
			}
		} else {
			columnFamilyValidator.validateCounterCF(cfDef);
		}

	}

	protected ColumnFamilyDefinition discoverTable(String columnFamilyName) {
		log.debug("Start discovery of column family {}", columnFamilyName);
		for (ColumnFamilyDefinition cfDef : this.cfDefs) {
			if (StringUtils.equals(cfDef.getName(), columnFamilyName)) {
				log.debug("Existing column family {} found", columnFamilyName);
				return cfDef;
			}
		}
		return null;
	}

	protected void addTable(ColumnFamilyDefinition cfDef) {
		if (!columnFamilyNames.contains(cfDef.getName())) {
			columnFamilyNames.add(cfDef.getName());
			cluster.addColumnFamily(cfDef, true);
		}

	}

	protected void createTable(EntityMeta entityMeta) {
		log.debug("Creating column family for entityMeta {}", entityMeta.getClassName());
		String columnFamilyName = entityMeta.getTableName();
		if (!columnFamilyNames.contains(columnFamilyName)) {
			ColumnFamilyDefinition cfDef;
			if (entityMeta.isClusteredEntity()) {
				cfDef = columnFamilyFactory.createClusteredEntityCF(this.keyspace.getKeyspaceName(), entityMeta);
			} else {
				cfDef = columnFamilyFactory.createEntityCF(entityMeta, this.keyspace.getKeyspaceName());

			}
			this.addTable(cfDef);
		}

	}

	private void createCounterColumnFamily() {
		log.debug("Creating generic counter column family");
		if (!columnFamilyNames.contains(AchillesCounter.THRIFT_COUNTER_CF)) {
			ColumnFamilyDefinition cfDef = columnFamilyFactory.createCounterCF(this.keyspace.getKeyspaceName());
			this.addTable(cfDef);
		}
	}
}
