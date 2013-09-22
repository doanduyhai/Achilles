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

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import info.archinnov.achilles.configuration.ArgumentExtractor;
import info.archinnov.achilles.consistency.AchillesConsistencyLevelPolicy;
import info.archinnov.achilles.context.ConfigurationContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.parsing.EntityExplorer;
import info.archinnov.achilles.entity.parsing.EntityParser;
import info.archinnov.achilles.entity.parsing.context.EntityParsingContext;
import info.archinnov.achilles.entity.parsing.validator.EntityParsingValidator;
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
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class EntityManagerFactoryTest {
	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private EntityManagerFactory factory;

	@Mock
	private TableCreator tableCreator;

	@Mock
	private EntityExplorer achillesEntityExplorer;

	@Mock
	private EntityParsingValidator validator;

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
		doCallRealMethod().when(factory).setEntityMetaMap(any(Map.class));
		doCallRealMethod().when(factory).setEntityPackages(any(List.class));
		doCallRealMethod().when(factory).setEntityParser(
				any(EntityParser.class));
		doCallRealMethod().when(factory).setEntityExplorer(
				any(EntityExplorer.class));
		doCallRealMethod().when(factory).setValidator(
				any(EntityParsingValidator.class));

		factory.setEntityMetaMap(entityMetaMap);
		factory.setEntityPackages(entityPackages);
		factory.setEntityParser(achillesEntityParser);
		factory.setEntityExplorer(achillesEntityExplorer);
		factory.setValidator(validator);
	}

	@Test
	public void should_bootstrap() throws Exception {

		when(factory.discoverEntities()).thenReturn(true);
		doCallRealMethod().when(factory).bootstrap();
		boolean hasSimpleCounter = factory.bootstrap();
		assertThat(hasSimpleCounter).isTrue();
	}

	@Test
	public void should_exception_during_boostrap() throws Exception {
		when(factory.discoverEntities())
				.thenThrow(new RuntimeException("test"));
		doCallRealMethod().when(factory).bootstrap();

		exception.expect(AchillesException.class);
		exception.expectMessage("Exception during entity parsing : test");
		factory.bootstrap();

	}

	@Test
	public void should_discover_package_without_entities() throws Exception {
		List<Class<?>> entities = new ArrayList<Class<?>>();

		EntityMeta entityMeta = new EntityMeta();

		when(achillesEntityExplorer.discoverEntities(entityPackages))
				.thenReturn(entities);
		when(achillesEntityParser.parseEntity(any(EntityParsingContext.class)))
				.thenReturn(entityMeta);

		doCallRealMethod().when(factory).discoverEntities();
		factory.discoverEntities();

		assertThat(entityMetaMap).isEmpty();

		verify(entityParser, never()).parseEntity(
				any(EntityParsingContext.class));
		verify(achillesEntityParser).fillJoinEntityMeta(
				any(EntityParsingContext.class), eq(entityMetaMap));

	}

	@Test
	public void should_parse_configuration() throws Exception {
		AchillesConsistencyLevelPolicy policy = mock(AchillesConsistencyLevelPolicy.class);
		ObjectMapperFactory mapperFactory = mock(ObjectMapperFactory.class);
		Map<String, Object> configurationMap = new HashMap<String, Object>();

		when(extractor.ensureConsistencyOnJoin(configurationMap)).thenReturn(
				true);
		when(extractor.initForceCFCreation(configurationMap)).thenReturn(true);
		when(factory.initConsistencyLevelPolicy(configurationMap, extractor))
				.thenReturn(policy);
		when(extractor.initObjectMapperFactory(configurationMap)).thenReturn(
				mapperFactory);

		doCallRealMethod().when(factory).parseConfiguration(configurationMap,
				extractor);

		ConfigurationContext builtContext = factory.parseConfiguration(
				configurationMap, extractor);

		assertThat(builtContext).isNotNull();
		assertThat(builtContext.isEnsureJoinConsistency()).isTrue();
		assertThat(builtContext.isForceColumnFamilyCreation()).isTrue();
		assertThat(builtContext.getConsistencyPolicy()).isSameAs(policy);
		assertThat(builtContext.getObjectMapperFactory()).isSameAs(
				mapperFactory);
	}

}
