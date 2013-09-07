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
package info.archinnov.achilles.entity.manager;

import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.configuration.ThriftArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.consistency.ThriftConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.context.ThriftDaoContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.HashMap;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class ThriftEntityManagerFactoryTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private ThriftEntityManagerFactory factory;

	@Test
	public void should_create_entity_manager() throws Exception {
		ThriftDaoContext daoContext = mock(ThriftDaoContext.class);
		ConfigurationContext configContext = mock(ConfigurationContext.class);
		AchillesConsistencyLevelPolicy consistencyPolicy = mock(AchillesConsistencyLevelPolicy.class);

		when(configContext.getConsistencyPolicy())
				.thenReturn(consistencyPolicy);
		when(consistencyPolicy.getDefaultGlobalReadConsistencyLevel())
				.thenReturn(EACH_QUORUM);
		Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();

		doCallRealMethod().when(factory).setThriftDaoContext(
				any(ThriftDaoContext.class));
		doCallRealMethod().when(factory).setConfigContext(
				any(ConfigurationContext.class));
		doCallRealMethod().when(factory).setEntityMetaMap(
				(Map<Class<?>, EntityMeta>) any(Map.class));

		factory.setThriftDaoContext(daoContext);
		factory.setConfigContext(configContext);
		factory.setEntityMetaMap(entityMetaMap);

		doCallRealMethod().when(factory).createEntityManager();

		ThriftEntityManager em = (ThriftEntityManager) factory
				.createEntityManager();

		assertThat(Whitebox.getInternalState(em, ThriftDaoContext.class))
				.isSameAs(daoContext);
		assertThat(Whitebox.getInternalState(em, "configContext")).isSameAs(
				configContext);
		Map<Class<?>, EntityMeta> builtEntityMetaMap = Whitebox
				.getInternalState(em, "entityMetaMap");
		assertThat(builtEntityMetaMap).isNotNull();
		assertThat(builtEntityMetaMap).isEmpty();

	}

	@Test
	public void should_init_consistency_level_policy() throws Exception {
		Map<String, Object> configMap = new HashMap<String, Object>();
		ThriftArgumentExtractor argumentExtractor = mock(ThriftArgumentExtractor.class);
		Map<String, ConsistencyLevel> readLevels = new HashMap<String, ConsistencyLevel>();
		Map<String, ConsistencyLevel> writeLevels = new HashMap<String, ConsistencyLevel>();
		readLevels.put("cf", THREE);
		writeLevels.put("cf", QUORUM);

		when(argumentExtractor.initDefaultReadConsistencyLevel(configMap))
				.thenReturn(ONE);
		when(argumentExtractor.initDefaultWriteConsistencyLevel(configMap))
				.thenReturn(TWO);
		when(argumentExtractor.initReadConsistencyMap(configMap)).thenReturn(
				readLevels);
		when(argumentExtractor.initWriteConsistencyMap(configMap)).thenReturn(
				writeLevels);

		doCallRealMethod().when(factory).initConsistencyLevelPolicy(configMap,
				argumentExtractor);

		ThriftConsistencyLevelPolicy policy = (ThriftConsistencyLevelPolicy) factory
				.initConsistencyLevelPolicy(configMap, argumentExtractor);

		assertThat(policy.getDefaultGlobalReadConsistencyLevel())
				.isEqualTo(ONE);
		assertThat(policy.getDefaultGlobalWriteConsistencyLevel()).isEqualTo(
				TWO);
		assertThat(policy.getConsistencyLevelForRead("cf")).isEqualTo(THREE);
		assertThat(policy.getConsistencyLevelForWrite("cf")).isEqualTo(QUORUM);
	}

}
