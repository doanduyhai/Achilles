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
package info.archinnov.achilles.entity.metadata;

import static info.archinnov.achilles.entity.metadata.PropertyType.EMBEDDED_ID;
import static info.archinnov.achilles.entity.metadata.PropertyType.ID;
import static info.archinnov.achilles.entity.metadata.PropertyType.LIST;
import static info.archinnov.achilles.entity.metadata.PropertyType.MAP;
import static info.archinnov.achilles.entity.metadata.PropertyType.SET;
import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static com.datastax.driver.core.ConsistencyLevel.QUORUM;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import info.archinnov.achilles.type.Pair;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SimpleTranscoder;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import com.datastax.driver.core.ConsistencyLevel;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	private ObjectMapper objectMapper = new ObjectMapper();

	@Mock
	private DataTranscoder transcoder;

	@Mock
	private ReflectionInvoker invoker;

	@Test
	public void should_get_counter_id_meta() throws Exception {
		PropertyMeta idMeta = new PropertyMeta();

		PropertyMeta propertyMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(PropertyType.COUNTER)
				.counterIdMeta(idMeta).build();

		assertThat(propertyMeta.counterIdMeta()).isSameAs(idMeta);
	}

	@Test
	public void should_return_null_if_no_counter_id_meta() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(PropertyType.COUNTER).build();

		assertThat(propertyMeta.counterIdMeta()).isNull();
	}

	@Test
	public void should_get_fqcn() throws Exception {

		PropertyMeta propertyMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(PropertyType.COUNTER)
				.fqcn("fqcn").build();

		assertThat(propertyMeta.fqcn()).isEqualTo("fqcn");
	}

	@Test
	public void should_return_null_if_no_fqcn() throws Exception {

		PropertyMeta propertyMeta = PropertyMetaTestBuilder.valueClass(Long.class).type(PropertyType.COUNTER).build();

		assertThat(propertyMeta.fqcn()).isNull();
	}

	@Test
	public void should_return_true_for_isLazy() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder.keyValueClass(Void.class, String.class)
				.type(PropertyType.LAZY_LIST).build();

		assertThat(propertyMeta.isLazy()).isTrue();
	}

	@Test
	public void should_return_true_for_isCounter_when_type_is_counter() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder.keyValueClass(Void.class, String.class)
				.type(PropertyType.COUNTER).build();

		assertThat(propertyMeta.isCounter()).isTrue();
	}

	@Test
	public void should_use_equals_and_hashcode() throws Exception {
		PropertyMeta meta1 = new PropertyMeta();
		meta1.setEntityClassName("entity");
		meta1.setPropertyName("field1");
		meta1.setType(PropertyType.SIMPLE);

		PropertyMeta meta2 = new PropertyMeta();
		meta2.setEntityClassName("entity");
		meta2.setPropertyName("field2");
		meta2.setType(PropertyType.SIMPLE);

		PropertyMeta meta3 = new PropertyMeta();
		meta3.setEntityClassName("entity");
		meta3.setPropertyName("field1");
		meta3.setType(PropertyType.LIST);

		PropertyMeta meta4 = new PropertyMeta();
		meta4.setEntityClassName("entity1");
		meta4.setPropertyName("field1");
		meta4.setType(PropertyType.SIMPLE);

		PropertyMeta meta5 = new PropertyMeta();
		meta5.setEntityClassName("entity");
		meta5.setPropertyName("field1");
		meta5.setType(PropertyType.SIMPLE);

		assertThat(meta1).isNotEqualTo(meta2);
		assertThat(meta1).isNotEqualTo(meta3);
		assertThat(meta1).isNotEqualTo(meta4);
		assertThat(meta1).isEqualTo(meta5);

		assertThat(meta1.hashCode()).isNotEqualTo(meta2.hashCode());
		assertThat(meta1.hashCode()).isNotEqualTo(meta3.hashCode());
		assertThat(meta1.hashCode()).isNotEqualTo(meta4.hashCode());
		assertThat(meta1.hashCode()).isEqualTo(meta5.hashCode());

		Set<PropertyMeta> pms = Sets.newHashSet(meta1, meta2, meta3, meta4, meta5);

		assertThat(pms).containsOnly(meta1, meta2, meta3, meta4);
	}

	@Test
	public void should_serialize_as_json() throws Exception {
		SimpleTranscoder transcoder = new SimpleTranscoder(objectMapper);
		PropertyMeta pm = new PropertyMeta();
		pm.setType(SIMPLE);
		pm.setTranscoder(transcoder);

		assertThat(pm.forceEncodeToJSON(new UUID(10, 10))).isEqualTo("\"00000000-0000-000a-0000-00000000000a\"");
	}

	@Test
	public void should_get_ordering_component() throws Exception {
		PropertyMeta meta = new PropertyMeta();

		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(Long.class, String.class),
				Arrays.asList("age", "name"), null, null);

		EmbeddedIdProperties props = new EmbeddedIdProperties(null, clusteringComponents, null, Arrays.asList("a", "b", "c"),
				null, null, null);
		meta.setEmbeddedIdProperties(props);

		assertThat(meta.getOrderingComponent()).isEqualTo("age");
	}
	
	@Test
	public void should_get_reversed_component() throws Exception {
		PropertyMeta meta = new PropertyMeta();

		ClusteringComponents clusteringComponents = new ClusteringComponents(Arrays.<Class<?>> asList(Long.class, String.class),
				Arrays.asList("age", "name"), "name", null, null);

		EmbeddedIdProperties props = new EmbeddedIdProperties(null, clusteringComponents, null, Arrays.asList("a", "b", "c"),
				null, null, null);
		meta.setEmbeddedIdProperties(props);

		assertThat(meta.getReversedComponent()).isEqualTo("name");
	}

	@Test
	public void should_return_null_for_cql_ordering_component_if_no_multikey() throws Exception {
		PropertyMeta meta = new PropertyMeta();

		assertThat(meta.getOrderingComponent()).isNull();

	}

	@Test
	public void should_get_component_getters() throws Exception {
		Method idGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = EmbeddedKey.class.getDeclaredMethod("getName");

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compGetters(idGetter, nameGetter)
				.build();

		assertThat(idMeta.getComponentGetters()).containsExactly(idGetter, nameGetter);
	}

	@Test
	public void should_return_empty_list_when_no_component_getters() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).build();

		assertThat(idMeta.getComponentGetters()).isEmpty();
	}

	@Test
	public void should_get_partition_key_getter() throws Exception {
		Method idGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = EmbeddedKey.class.getDeclaredMethod("getName");

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compGetters(idGetter, nameGetter)
				.build();

		assertThat(idMeta.getPartitionKeyGetter()).isEqualTo(idGetter);
	}

	@Test
	public void should_return_null_when_no_partition_key_getter() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).build();

		assertThat(idMeta.getPartitionKeyGetter()).isNull();
	}

	@Test
	public void should_get_component_names() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compNames("a", "b").build();

		assertThat(idMeta.getComponentNames()).containsExactly("a", "b");
	}

	@Test
	public void should_get_empty_component_names() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).build();

		assertThat(idMeta.getComponentNames()).isEmpty();
	}

	@Test
	public void should_get_component_setters() throws Exception {
		Method idSetter = EmbeddedKey.class.getDeclaredMethod("setUserId", Long.class);
		Method nameSetter = EmbeddedKey.class.getDeclaredMethod("setName", String.class);

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).compSetters(idSetter, nameSetter)
				.build();

		assertThat(idMeta.getComponentSetters()).containsExactly(idSetter, nameSetter);
	}

	@Test
	public void should_return_empty_list_when_no_component_setters() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).build();

		assertThat(idMeta.getComponentSetters()).isEmpty();
	}

	@Test
	public void should_get_component_classes() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class)
				.compClasses(Long.class, String.class).build();

		assertThat(idMeta.getComponentClasses()).containsExactly(Long.class, String.class);
	}

	@Test
	public void should_return_empty_list_when_no_component_classes() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).build();

		assertThat(idMeta.getComponentClasses()).isEmpty();
	}

	@Test
	public void should_return_true_for_is_embedded_id() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID).build();

		assertThat(idMeta.isEmbeddedId()).isTrue();
	}

	@Test
	public void should_return_false_for_is_embedded_id() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(ID).build();

		assertThat(idMeta.isEmbeddedId()).isFalse();
	}

	@Test
	public void should_get_read_consistency() throws Exception {
		PropertyMeta pm = new PropertyMeta();

		assertThat(pm.getReadConsistencyLevel()).isNull();
		assertThat(pm.getWriteConsistencyLevel()).isNull();

		pm.setConsistencyLevels(Pair.<ConsistencyLevel, ConsistencyLevel> create(QUORUM, QUORUM));

		assertThat(pm.getReadConsistencyLevel()).isEqualTo(QUORUM);
		assertThat(pm.getWriteConsistencyLevel()).isEqualTo(QUORUM);
	}

	@Test
	public void should_decode() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		pm.setTranscoder(transcoder);

		assertThat(pm.decode((Object) null)).isNull();
		assertThat(pm.decodeKey((Object) null)).isNull();
		assertThat(pm.decode((List<?>) null)).isNull();
		assertThat(pm.decode((Set<?>) null)).isNull();
		assertThat(pm.decode((Map<?, ?>) null)).isNull();
		assertThat(pm.decodeFromComponents((List<?>) null)).isNull();

		Object value = "";
		List<Object> list = new ArrayList<Object>();
		Set<Object> set = new HashSet<Object>();
		Map<Object, Object> map = new HashMap<Object, Object>();

		when(transcoder.decode(pm, value)).thenReturn(value);
		when(transcoder.decodeKey(pm, value)).thenReturn(value);
		when(transcoder.decode(pm, list)).thenReturn(list);
		when(transcoder.decode(pm, set)).thenReturn(set);
		when(transcoder.decode(pm, map)).thenReturn(map);
		when(transcoder.decodeFromComponents(pm, list)).thenReturn(list);

		assertThat(pm.decode(value)).isEqualTo(value);
		assertThat(pm.decodeKey(value)).isEqualTo(value);
		assertThat(pm.decode(list)).isEqualTo(list);
		assertThat(pm.decode(set)).isEqualTo(set);
		assertThat(pm.decode(map)).isEqualTo(map);
		assertThat(pm.decodeFromComponents(list)).isEqualTo(list);
	}

	@Test
	public void should_encode() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		pm.setTranscoder(transcoder);

		assertThat(pm.encode((Object) null)).isNull();
		assertThat(pm.encodeKey((Object) null)).isNull();
		assertThat(pm.encode((List<?>) null)).isNull();
		assertThat(pm.encode((Set<?>) null)).isNull();
		assertThat(pm.encode((Map<?, ?>) null)).isNull();
		assertThat(pm.encodeToComponents((List<?>) null)).isNull();

		Object value = "";
		List<Object> list = new ArrayList<Object>();
		Set<Object> set = new HashSet<Object>();
		Map<Object, Object> map = new HashMap<Object, Object>();

		when(transcoder.encode(pm, value)).thenReturn(value);
		when(transcoder.encodeKey(pm, value)).thenReturn(value);
		when(transcoder.encode(pm, list)).thenReturn(list);
		when(transcoder.encode(pm, set)).thenReturn(set);
		when(transcoder.encode(pm, map)).thenReturn(map);
		when(transcoder.encodeToComponents(pm, list)).thenReturn(list);
		when(transcoder.encodeToComponents(pm, list)).thenReturn(list);

		assertThat(pm.encode(value)).isEqualTo(value);
		assertThat(pm.encodeKey(value)).isEqualTo(value);
		assertThat(pm.encode(list)).isEqualTo(list);
		assertThat(pm.encode(set)).isEqualTo(set);
		assertThat(pm.encode(map)).isEqualTo(map);
		assertThat(pm.encodeToComponents(list)).isEqualTo(list);
		assertThat(pm.encodeToComponents(list)).isEqualTo(list);
	}

	@Test
	public void should_force_encode_to_json() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		pm.setTranscoder(transcoder);

		pm.forceEncodeToJSON("value");

		verify(transcoder).forceEncodeToJSON("value");
	}

	@Test
	public void should_force_decode_from_json_simple_value() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		pm.setTranscoder(transcoder);
		pm.setType(SIMPLE);
		pm.setValueClass(String.class);

		when(transcoder.forceDecodeFromJSON("value", String.class)).thenReturn("decoded");

		Object decoded = pm.forceDecodeFromJSON("value");

		assertThat(decoded).isEqualTo("decoded");
	}

	@Test
	public void should_force_decode_from_json_to_type() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		pm.setTranscoder(transcoder);

		when(transcoder.forceDecodeFromJSON("test", String.class)).thenReturn("test");

		assertThat(pm.forceDecodeFromJSON("test", String.class)).isEqualTo("test");
	}

	@Test
	public void should_get_primary_key() throws Exception {

		long id = RandomUtils.nextLong();
		CompleteBean entity = new CompleteBean(id);

		PropertyMeta pm = new PropertyMeta();
		pm.setType(ID);
		pm.setInvoker(invoker);

		when(invoker.getPrimaryKey(entity, pm)).thenReturn(id);

		assertThat(pm.getPrimaryKey(entity)).isEqualTo(id);
	}

	@Test
	public void should_exception_when_asking_primary_key_on_non_id_field() throws Exception {

		CompleteBean entity = new CompleteBean();

		PropertyMeta pm = new PropertyMeta();
		pm.setPropertyName("property");
		pm.setType(SIMPLE);

		exception.expect(IllegalStateException.class);
		exception.expectMessage("Cannot get primary key on a non id field 'property'");

		pm.getPrimaryKey(entity);
	}

	@Test
	public void should_get_partition_key() throws Exception {

		long id = RandomUtils.nextLong();
		EmbeddedKey embeddedKey = new EmbeddedKey(id, "name");

		PropertyMeta pm = new PropertyMeta();
		pm.setType(EMBEDDED_ID);
		pm.setInvoker(invoker);

		when(invoker.getPartitionKey(embeddedKey, pm)).thenReturn(id);

		assertThat(pm.getPartitionKey(embeddedKey)).isEqualTo(id);
	}

	@Test
	public void should_exception_when_asking_partition_key_on_non_embedded_id_field() throws Exception {

		EmbeddedKey embeddedKey = new EmbeddedKey();

		PropertyMeta pm = new PropertyMeta();
		pm.setPropertyName("property");
		pm.setType(SIMPLE);

		exception.expect(IllegalStateException.class);
		exception.expectMessage("Cannot get partition key on a non embedded id field 'property'");

		pm.getPartitionKey(embeddedKey);
	}

	@Test
	public void should_get_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		entity.setName("name");

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn("name");

		assertThat(pm.getValueFromField(entity)).isEqualTo("name");
	}

	@Test
	public void should_get_list_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		List<String> friends = Arrays.asList("foo", "bar");
		entity.setFriends(friends);

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("friends").accessors()
				.type(LIST).invoker(invoker).build();

		when((List<String>) invoker.getListValueFromField(entity, pm.getGetter())).thenReturn(friends);

		assertThat((List<String>) pm.getListValueFromField(entity)).containsExactly("foo", "bar");
	}

	@Test
	public void should_get_set_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		Set<String> followers = Sets.newHashSet("George", "Paul");
		entity.setFollowers(followers);

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("followers").accessors()
				.type(SET).invoker(invoker).build();

		when((Set<String>) invoker.getSetValueFromField(entity, pm.getGetter())).thenReturn(followers);

		assertThat((Set<String>) pm.getSetValueFromField(entity)).containsOnly("George", "Paul");
	}

	@Test
	public void should_get_map_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		Map<Integer, String> preferences = ImmutableMap.of(1, "FR", 2, "Paris");
		entity.setPreferences(preferences);

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Integer.class, String.class).field("preferences")
				.accessors().type(MAP).invoker(invoker).build();

		when((Map<Integer, String>) invoker.getMapValueFromField(entity, pm.getGetter())).thenReturn(preferences);

		Map<Integer, String> actual = (Map<Integer, String>) pm.getMapValueFromField(entity);

		assertThat(actual).containsKey(1).containsKey(2).containsValue("FR").containsValue("Paris");
	}

	@Test
	public void should_set_value_to_field() throws Exception {

		CompleteBean entity = new CompleteBean();

		PropertyMeta pm = PropertyMetaTestBuilder.completeBean(Void.class, String.class).field("name").accessors()
				.type(SIMPLE).invoker(invoker).build();

		pm.setValueToField(entity, "name");

		verify(invoker).setValueToField(entity, pm.getSetter(), "name");
	}

	@Test
	public void should_get_clustering_component_names() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).compNames("id", "comp1", "comp2").build();

		assertThat(pm.getClusteringComponentNames()).containsExactly("comp1", "comp2");
	}

	@Test
	public void should_get_empty_clustering_component_names() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).build();

		assertThat(pm.getClusteringComponentNames()).isEmpty();
	}

	@Test
	public void should_get_clustering_component_classes() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.compClasses(Long.class, UUID.class, String.class).build();

		assertThat(pm.getClusteringComponentClasses()).containsExactly(UUID.class, String.class);
	}

	@Test
	public void should_get_empty_clustering_component_classes() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).build();

		assertThat(pm.getClusteringComponentClasses()).isEmpty();
	}

	@Test
	public void should_return_true_for_is_component_time_uuid() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).compNames("id", "comp1", "comp2")
				.compTimeUUID("comp1").build();

		assertThat(pm.isComponentTimeUUID("comp1")).isTrue();
		assertThat(pm.isComponentTimeUUID("comp2")).isFalse();
	}

	@Test
	public void should_return_false_for_is_component_time_uuid_if_not_embedded_id() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).build();

		assertThat(pm.isComponentTimeUUID("comp1")).isFalse();
	}
}
