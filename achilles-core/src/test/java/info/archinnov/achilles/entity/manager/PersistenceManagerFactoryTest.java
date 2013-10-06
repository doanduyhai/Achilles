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
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parsing.EntityExplorer;
import info.archinnov.achilles.entity.parsing.EntityParser;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.table.TableCreator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PersistenceManagerFactoryTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private PersistenceManagerFactory pmf;

	@Mock
	private TableCreator tableCreator;

	@Mock
	private EntityExplorer achillesEntityExplorer;

	@Mock
	private EntityParser achillesEntityParser;

	@Mock
	private ArgumentExtractor extractor;

	@Mock
	private EntityParser entityParser;

	private Map<Class<?>, EntityMeta> entityMetaMap = new HashMap<Class<?>, EntityMeta>();

	private List<String> entityPackages = new ArrayList<String>();

	@Before
	public void setUp() {
		doCallRealMethod().when(pmf).setEntityMetaMap(Mockito.<Map<Class<?>, EntityMeta>> any());
		doCallRealMethod().when(pmf).setEntityPackages(Mockito.<List<String>> any());
		doCallRealMethod().when(pmf).setEntityParser(any(EntityParser.class));
		doCallRealMethod().when(pmf).setEntityExplorer(any(EntityExplorer.class));

		pmf.setEntityMetaMap(entityMetaMap);
		pmf.setEntityPackages(entityPackages);
		pmf.setEntityParser(achillesEntityParser);
		pmf.setEntityExplorer(achillesEntityExplorer);
	}

	@Test
	public void should_bootstrap() throws Exception {

		when(pmf.discoverEntities()).thenReturn(true);
		doCallRealMethod().when(pmf).bootstrap();
		boolean hasSimpleCounter = pmf.bootstrap();
		assertThat(hasSimpleCounter).isTrue();
	}

	@Test
	public void should_exception_during_boostrap() throws Exception {
		when(pmf.discoverEntities()).thenThrow(new RuntimeException("test"));
		doCallRealMethod().when(pmf).bootstrap();

		exception.expect(AchillesException.class);
		exception.expectMessage("Exception during entity parsing : test");
		pmf.bootstrap();

	}

	@Test
	public void should_discover_entities() throws Exception {
		List<Class<?>> entities = new ArrayList<Class<?>>();
		entities.add(Long.class);

		EntityMeta entityMeta = new EntityMeta();

		when(achillesEntityExplorer.discoverEntities(entityPackages)).thenReturn(entities);
		when(achillesEntityParser.parseEntity(any(EntityParsingContext.class))).thenReturn(entityMeta);

		doCallRealMethod().when(pmf).discoverEntities();
		pmf.discoverEntities();

		assertThat(entityMetaMap).containsKey(Long.class);
		assertThat(entityMetaMap).containsValue(entityMeta);
	}

	@Test
	public void should_discover_package_without_entities() throws Exception {
		List<Class<?>> entities = new ArrayList<Class<?>>();

		EntityMeta entityMeta = new EntityMeta();

		when(achillesEntityExplorer.discoverEntities(entityPackages)).thenReturn(entities);
		when(achillesEntityParser.parseEntity(any(EntityParsingContext.class))).thenReturn(entityMeta);

		doCallRealMethod().when(pmf).discoverEntities();
		pmf.discoverEntities();

		assertThat(entityMetaMap).isEmpty();

		verify(entityParser, never()).parseEntity(any(EntityParsingContext.class));
	}

	@Test
	public void should_parse_configuration() throws Exception {
		AchillesConsistencyLevelPolicy policy = mock(AchillesConsistencyLevelPolicy.class);
		ObjectMapperFactory mapperFactory = mock(ObjectMapperFactory.class);
		Map<String, Object> configurationMap = new HashMap<String, Object>();
		when(extractor.initForceCFCreation(configurationMap)).thenReturn(true);
		when(pmf.initConsistencyLevelPolicy(configurationMap, extractor)).thenReturn(policy);
		when(extractor.initObjectMapperFactory(configurationMap)).thenReturn(mapperFactory);

		doCallRealMethod().when(pmf).parseConfiguration(configurationMap, extractor);

		ConfigurationContext builtContext = pmf.parseConfiguration(configurationMap, extractor);

		assertThat(builtContext).isNotNull();
		assertThat(builtContext.isForceColumnFamilyCreation()).isTrue();
		assertThat(builtContext.getConsistencyPolicy()).isSameAs(policy);
		assertThat(builtContext.getObjectMapperFactory()).isSameAs(mapperFactory);
	}

}
