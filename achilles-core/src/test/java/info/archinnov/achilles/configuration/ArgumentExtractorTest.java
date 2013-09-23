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
package info.archinnov.achilles.configuration;

import static info.archinnov.achilles.configuration.ConfigurationParameters.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.*;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.AnnotationIntrospector;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.codehaus.jackson.map.introspect.JacksonAnnotationIntrospector;
import org.codehaus.jackson.xc.JaxbAnnotationIntrospector;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableMap;

@RunWith(MockitoJUnitRunner.class)
public class ArgumentExtractorTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock
	private ArgumentExtractor extractor;

	@Mock
	private ObjectMapper mapper;

	@Mock
	private ObjectMapperFactory factory;

	private Map<String, Object> configMap = new HashMap<String, Object>();

	@Before
	public void setUp() {
		configMap.clear();
	}

	@Test
	public void should_init_entity_packages() throws Exception {
		configMap.put(ENTITY_PACKAGES_PARAM,
				"my.package.entity,another.package.entity,third.package");

		doCallRealMethod().when(extractor).initEntityPackages(configMap);
		List<String> actual = extractor.initEntityPackages(configMap);

		assertThat(actual).containsExactly("my.package.entity",
				"another.package.entity", "third.package");
	}

	@Test
	public void should_init_empty_entity_packages() throws Exception {
		doCallRealMethod().when(extractor).initEntityPackages(configMap);
		List<String> actual = extractor.initEntityPackages(configMap);

		assertThat(actual).isEmpty();
	}

	@Test
	public void should_init_forceCFCreation_to_default_value() throws Exception {
		doCallRealMethod().when(extractor).initForceCFCreation(configMap);
		boolean actual = extractor.initForceCFCreation(configMap);

		assertThat(actual).isFalse();
	}

	@Test
	public void should_init_forceCFCreation() throws Exception {
		configMap.put(FORCE_CF_CREATION_PARAM, true);

		doCallRealMethod().when(extractor).initForceCFCreation(configMap);
		boolean actual = extractor.initForceCFCreation(configMap);

		assertThat(actual).isTrue();
	}

	@Test
	public void should_ensure_join_consistency() throws Exception {
		configMap.put(ENSURE_CONSISTENCY_ON_JOIN_PARAM, true);
		doCallRealMethod().when(extractor).ensureConsistencyOnJoin(configMap);
		assertThat(extractor.ensureConsistencyOnJoin(configMap)).isTrue();
	}

	@Test
	public void should_not_ensure_join_consistency_by_default()
			throws Exception {
		doCallRealMethod().when(extractor).ensureConsistencyOnJoin(configMap);
		assertThat(extractor.ensureConsistencyOnJoin(configMap)).isFalse();
	}

	@Test
	public void should_init_default_object_factory_mapper() throws Exception {
		doCallRealMethod().when(extractor).initObjectMapperFactory(configMap);
		ObjectMapperFactory actual = extractor
				.initObjectMapperFactory(configMap);

		assertThat(actual).isNotNull();

		ObjectMapper mapper = actual.getMapper(Integer.class);

		assertThat(mapper).isNotNull();
		assertThat(mapper.getSerializationConfig().getSerializationInclusion())
				.isEqualTo(Inclusion.NON_NULL);
		assertThat(
				mapper.getDeserializationConfig()
						.isEnabled(
								DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES))
				.isFalse();
		Collection<AnnotationIntrospector> ais = mapper
				.getSerializationConfig().getAnnotationIntrospector()
				.allIntrospectors();

		assertThat(ais).hasSize(2);
		Iterator<AnnotationIntrospector> iterator = ais.iterator();

		assertThat(iterator.next()).isInstanceOfAny(
				JacksonAnnotationIntrospector.class,
				JaxbAnnotationIntrospector.class);
		assertThat(iterator.next()).isInstanceOfAny(
				JacksonAnnotationIntrospector.class,
				JaxbAnnotationIntrospector.class);
	}

	@Test
	public void should_init_object_mapper_factory_from_mapper()
			throws Exception {
		configMap.put(OBJECT_MAPPER_PARAM, mapper);

		doCallRealMethod().when(extractor).initObjectMapperFactory(configMap);
		ObjectMapperFactory actual = extractor
				.initObjectMapperFactory(configMap);

		assertThat(actual).isNotNull();
		assertThat(actual.getMapper(Long.class)).isSameAs(mapper);
	}

	@Test
	public void should_init_object_mapper_factory() throws Exception {
		configMap.put(OBJECT_MAPPER_FACTORY_PARAM, factory);

		doCallRealMethod().when(extractor).initObjectMapperFactory(configMap);
		ObjectMapperFactory actual = extractor
				.initObjectMapperFactory(configMap);

		assertThat(actual).isSameAs(factory);
	}

	@Test
	public void should_init_default_read_consistency_level() throws Exception {
		configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM, "ONE");

		doCallRealMethod().when(extractor).initDefaultReadConsistencyLevel(
				configMap);
		assertThat(extractor.initDefaultReadConsistencyLevel(configMap))
				.isEqualTo(ONE);
	}

	@Test
	public void should_exception_when_invalid_consistency_level()
			throws Exception {
		configMap.put(CONSISTENCY_LEVEL_READ_DEFAULT_PARAM, "wrong_value");

		exception.expect(IllegalArgumentException.class);
		exception
				.expectMessage("'wrong_value' is not a valid Consistency Level");

		doCallRealMethod().when(extractor).initDefaultReadConsistencyLevel(
				configMap);
		extractor.initDefaultReadConsistencyLevel(configMap);
	}

	@Test
	public void should_init_default_write_consistency_level() throws Exception {
		configMap.put(CONSISTENCY_LEVEL_WRITE_DEFAULT_PARAM, "LOCAL_QUORUM");

		doCallRealMethod().when(extractor).initDefaultWriteConsistencyLevel(
				configMap);
		assertThat(extractor.initDefaultWriteConsistencyLevel(configMap))
				.isEqualTo(LOCAL_QUORUM);
	}

	@Test
	public void should_return_default_one_level_when_no_parameter()
			throws Exception {
		doCallRealMethod().when(extractor).initDefaultReadConsistencyLevel(
				configMap);
		assertThat(extractor.initDefaultReadConsistencyLevel(configMap))
				.isEqualTo(ONE);
	}

	@Test
	public void should_init_read_consistency_level_map() throws Exception {
		configMap.put(CONSISTENCY_LEVEL_READ_MAP_PARAM,
				ImmutableMap.of("cf1", "ONE", "cf2", "LOCAL_QUORUM"));

		doCallRealMethod().when(extractor).initReadConsistencyMap(configMap);
		Map<String, ConsistencyLevel> consistencyMap = extractor
				.initReadConsistencyMap(configMap);

		assertThat(consistencyMap.get("cf1")).isEqualTo(ConsistencyLevel.ONE);
		assertThat(consistencyMap.get("cf2")).isEqualTo(
				ConsistencyLevel.LOCAL_QUORUM);
	}

	@Test
	public void should_init_write_consistency_level_map() throws Exception {
		configMap.put(CONSISTENCY_LEVEL_WRITE_MAP_PARAM,
				ImmutableMap.of("cf1", "THREE", "cf2", "EACH_QUORUM"));

		doCallRealMethod().when(extractor).initWriteConsistencyMap(configMap);
		Map<String, ConsistencyLevel> consistencyMap = extractor
				.initWriteConsistencyMap(configMap);

		assertThat(consistencyMap.get("cf1")).isEqualTo(ConsistencyLevel.THREE);
		assertThat(consistencyMap.get("cf2")).isEqualTo(
				ConsistencyLevel.EACH_QUORUM);
	}

	@Test
	public void should_return_empty_consistency_map_when_no_parameter()
			throws Exception {
		doCallRealMethod().when(extractor).initWriteConsistencyMap(configMap);

		Map<String, ConsistencyLevel> consistencyMap = extractor
				.initWriteConsistencyMap(configMap);

		assertThat(consistencyMap).isEmpty();
	}

	@Test
	public void should_return_empty_consistency_map_when_empty_map_parameter()
			throws Exception {
		configMap.put(CONSISTENCY_LEVEL_WRITE_MAP_PARAM,
				new HashMap<String, String>());

		doCallRealMethod().when(extractor).initWriteConsistencyMap(configMap);

		Map<String, ConsistencyLevel> consistencyMap = extractor
				.initWriteConsistencyMap(configMap);

		assertThat(consistencyMap).isEmpty();
	}
}
