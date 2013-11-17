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

import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parsing.EntityExplorer;
import info.archinnov.achilles.entity.parsing.EntityParser;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.table.TableCreator;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

@RunWith(MockitoJUnitRunner.class)
public class CQLPersistenceManagerFactoryTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private CQLPersistenceManagerFactory pmf;

	@Mock
	private TableCreator tableCreator;

	@Mock
	private EntityExplorer entityExplorer;

	@Mock
	private EntityParser entityParser;

	@Mock
	private ArgumentExtractor extractor;

	@Mock
	private ConfigurationContext configContext;

	private Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();

	private List<String> entityPackages = new ArrayList<String>();

	private CQLPersistenceManagerFactory build(Map<String, Object> configMap) {
		CQLPersistenceManagerFactory pmf = new CQLPersistenceManagerFactory(configMap);
		pmf.setEntityMetaMap(entityMetaMap);
		pmf.setEntityPackages(entityPackages);
		pmf.setEntityParser(entityParser);
		pmf.setEntityExplorer(entityExplorer);
		pmf.setConfigContext(configContext);
		Whitebox.setInternalState(pmf, ArgumentExtractor.class, extractor);
		return pmf;
	}

	@Test
	public void should_bootstrap() throws Exception {
		pmf = build(new HashMap<String, Object>());
		when(pmf.discoverEntities()).thenReturn(true);
		boolean hasSimpleCounter = pmf.bootstrap();
		assertThat(hasSimpleCounter).isTrue();
	}

	@Test
	public void should_exception_during_boostrap() throws Exception {

		pmf = build(new HashMap<String, Object>());
		pmf = spy(pmf);

		doThrow(new RuntimeException("test")).when(pmf).discoverEntities();

		exception.expect(AchillesException.class);
		exception.expectMessage("Exception during entity parsing : test");
		pmf.bootstrap();

	}

	@Test
	public void should_discover_entities() throws Exception {
		List<Class<?>> entities = new ArrayList<Class<?>>();
		entities.add(Long.class);

		EntityMeta entityMeta = new EntityMeta();

		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(entities);
		when(entityParser.parseEntity(any(EntityParsingContext.class))).thenReturn(entityMeta);

		pmf.discoverEntities();

		assertThat(entityMetaMap).containsKey(Long.class);
		assertThat(entityMetaMap).containsValue(entityMeta);
	}

	@Test
	public void should_discover_package_without_entities() throws Exception {
		List<Class<?>> entities = new ArrayList<Class<?>>();

		EntityMeta entityMeta = new EntityMeta();

		when(entityExplorer.discoverEntities(entityPackages)).thenReturn(entities);
		when(entityParser.parseEntity(any(EntityParsingContext.class))).thenReturn(entityMeta);

		pmf.discoverEntities();

		assertThat(entityMetaMap).isEmpty();

		verify(entityParser, never()).parseEntity(any(EntityParsingContext.class));
	}

	@Test
	public void should_parse_configuration() throws Exception {
		ObjectMapperFactory mapperFactory = mock(ObjectMapperFactory.class);
		Map<String, Object> configurationMap = new HashMap<String, Object>();
		when(extractor.initForceCFCreation(configurationMap)).thenReturn(true);
		when(extractor.initObjectMapperFactory(configurationMap)).thenReturn(mapperFactory);
		when(extractor.initDefaultReadConsistencyLevel(configurationMap)).thenReturn(ConsistencyLevel.ANY);
		when(extractor.initDefaultWriteConsistencyLevel(configurationMap)).thenReturn(ConsistencyLevel.ALL);

		ConfigurationContext builtContext = pmf.parseConfiguration(configurationMap);

		assertThat(builtContext).isNotNull();
		assertThat(builtContext.isForceColumnFamilyCreation()).isTrue();
		assertThat(builtContext.getObjectMapperFactory()).isSameAs(mapperFactory);
	}

	@Test
	public void should_create_entity_manager() throws Exception {

		CQLPersistenceManager manager = pmf.createPersistenceManager();
		assertThat(manager).isNotNull();
	}
}
