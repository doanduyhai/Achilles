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

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.QUORUM;
import static javax.persistence.CascadeType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SimpleTranscoder;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.utils.Pair;
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
	public void should_cast_key() throws Exception {
		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setKeyClass(String.class);

		Object test = "test";

		Object key = propertyMeta.getKey(test);
		assertThat(key).isEqualTo("test");
	}

	@Test
	public void should_get_join_meta() throws Exception {
		EntityMeta joinMeta = new EntityMeta();

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.JOIN_MAP).joinMeta(joinMeta).build();

		assertThat(propertyMeta.joinMeta()).isSameAs(joinMeta);
	}

	@Test
	public void should_return_null_if_not_join_type() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.SIMPLE).build();

		assertThat(propertyMeta.joinMeta()).isNull();
	}

	@Test
	public void should_get_join_id_meta() throws Exception {
		EntityMeta joinMeta = new EntityMeta();
		PropertyMeta joinIdMeta = new PropertyMeta();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.JOIN_MAP).joinMeta(joinMeta).build();

		assertThat(propertyMeta.joinIdMeta()).isSameAs(joinIdMeta);
	}

	@Test
	public void should_return_null_when_no_join_id_meta() throws Exception {

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.JOIN_MAP).build();

		assertThat(propertyMeta.joinIdMeta()).isNull();
	}

	@Test
	public void should_get_counter_id_meta() throws Exception {
		PropertyMeta idMeta = new PropertyMeta();

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.valueClass(Long.class).type(PropertyType.COUNTER)
				.counterIdMeta(idMeta).build();

		assertThat(propertyMeta.counterIdMeta()).isSameAs(idMeta);
	}

	@Test
	public void should_return_null_if_no_counter_id_meta() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.valueClass(Long.class).type(PropertyType.COUNTER).build();

		assertThat(propertyMeta.counterIdMeta()).isNull();
	}

	@Test
	public void should_get_fqcn() throws Exception {

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.valueClass(Long.class).type(PropertyType.COUNTER).fqcn("fqcn")
				.build();

		assertThat(propertyMeta.fqcn()).isEqualTo("fqcn");
	}

	@Test
	public void should_return_null_if_no_fqcn() throws Exception {

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.valueClass(Long.class).type(PropertyType.COUNTER).build();

		assertThat(propertyMeta.fqcn()).isNull();
	}

	@Test
	public void should_return_null_when_joinid_invoke() throws Exception {
		EntityMeta joinMeta = new EntityMeta();

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.JOIN_MAP).joinMeta(joinMeta).build();

		assertThat(propertyMeta.joinIdMeta()).isNull();
	}

	@Test
	public void should_return_true_when_join_type() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.JOIN_SIMPLE).build();

		assertThat(propertyMeta.isJoin()).isTrue();
	}

	@Test
	public void should_return_true_for_isLazy() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Void.class, String.class)
				.type(PropertyType.LAZY_LIST).build();

		assertThat(propertyMeta.isLazy()).isTrue();
	}

	@Test
	public void should_return_true_for_isCounter_when_type_is_counter()
			throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Void.class, String.class)
				.type(PropertyType.COUNTER).build();

		assertThat(propertyMeta.isCounter()).isTrue();
	}

	@Test
	public void should_have_cascade_type() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.field("name").cascadeType(MERGE).build();

		assertThat(pm.hasCascadeType(MERGE)).isTrue();
	}

	@Test
	public void should_not_have_cascade_type() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.field("name").cascadeType(MERGE).build();

		assertThat(pm.hasCascadeType(ALL)).isFalse();
	}

	@Test
	public void should_not_have_cascade_type_when_not_join() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.field("name").build();

		assertThat(pm.hasCascadeType(ALL)).isFalse();
	}

	@Test
	public void should_not_have_cascade_type_because_not_join()
			throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.field("name").build();

		assertThat(pm.hasCascadeType(ALL)).isFalse();
	}

	@Test
	public void should_have_any_cascade_type() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.field("name").cascadeTypes(MERGE, ALL).build();

		assertThat(pm.hasAnyCascadeType(MERGE, PERSIST, REMOVE)).isTrue();
		assertThat(pm.getJoinProperties().getCascadeTypes()).containsOnly(
				MERGE, ALL);
		assertThat(pm.hasAnyCascadeType(PERSIST, REMOVE)).isFalse();

	}

	@Test
	public void should_test_is_join_collection() throws Exception {
		PropertyMeta pm1 = PropertyMetaTestBuilder.valueClass(String.class)
				.type(JOIN_LIST).build();

		PropertyMeta pm2 = PropertyMetaTestBuilder.valueClass(String.class)
				.type(JOIN_SET).build();

		PropertyMeta pm3 = PropertyMetaTestBuilder.valueClass(String.class)
				.type(SIMPLE).build();

		assertThat(pm1.isJoinCollection()).isTrue();
		assertThat(pm2.isJoinCollection()).isTrue();
		assertThat(pm3.isJoinCollection()).isFalse();
	}

	@Test
	public void should_test_is_join_map() throws Exception {
		PropertyMeta pm1 = PropertyMetaTestBuilder.valueClass(String.class)
				.type(JOIN_MAP).build();
		PropertyMeta pm2 = PropertyMetaTestBuilder.valueClass(String.class)
				.type(SIMPLE).build();

		assertThat(pm1.isJoinMap()).isTrue();
		assertThat(pm2.isJoinMap()).isFalse();
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

		Set<PropertyMeta> pms = Sets.newHashSet(meta1, meta2, meta3, meta4,
				meta5);

		assertThat(pms).containsOnly(meta1, meta2, meta3, meta4);
	}

	@Test
	public void should_serialize_as_json() throws Exception {
		SimpleTranscoder transcoder = new SimpleTranscoder(objectMapper);
		PropertyMeta pm = new PropertyMeta();
		pm.setType(SIMPLE);
		pm.setTranscoder(transcoder);

		assertThat(pm.forceEncodeToJSON(new UUID(10, 10))).isEqualTo(
				"\"00000000-0000-000a-0000-00000000000a\"");
	}

	@Test
	public void should_get_cql_ordering_component() throws Exception {
		PropertyMeta meta = new PropertyMeta();
		EmbeddedIdProperties multiKeyProperties = new EmbeddedIdProperties();
		multiKeyProperties
				.setComponentNames(Arrays.asList("id", "age", "name"));
		meta.setEmbeddedIdProperties(multiKeyProperties);

		assertThat(meta.getOrderingComponent()).isEqualTo("age");
	}

	@Test
	public void should_return_null_for_cql_ordering_component_if_no_multikey()
			throws Exception {
		PropertyMeta meta = new PropertyMeta();

		assertThat(meta.getOrderingComponent()).isNull();

	}

	@Test
	public void should_get_component_getters() throws Exception {
		Method idGetter = CompoundKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compGetters(Arrays.asList(idGetter, nameGetter)).build();

		assertThat(idMeta.getComponentGetters()).containsExactly(idGetter,
				nameGetter);
	}

	@Test
	public void should_return_empty_list_when_no_component_getters()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(
				CompoundKey.class).build();

		assertThat(idMeta.getComponentGetters()).isEmpty();
	}

	@Test
	public void should_get_partition_key_getter() throws Exception {
		Method idGetter = CompoundKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = CompoundKey.class.getDeclaredMethod("getName");

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compGetters(Arrays.asList(idGetter, nameGetter)).build();

		assertThat(idMeta.getPartitionKeyGetter()).isEqualTo(idGetter);
	}

	@Test
	public void should_get_partition_key_setter() throws Exception {
		Method idSetter = CompoundKey.class.getDeclaredMethod("setUserId",
				Long.class);
		Method nameSetter = CompoundKey.class.getDeclaredMethod("setName",
				String.class);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compSetters(Arrays.asList(idSetter, nameSetter)).build();

		assertThat(idMeta.getPartitionKeySetter()).isEqualTo(idSetter);
	}

	@Test
	public void should_return_null_when_no_partition_key_getter()
			throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(
				CompoundKey.class).build();

		assertThat(idMeta.getPartitionKeyGetter()).isNull();
	}

	@Test
	public void should_return_null_when_no_partition_key_setter()
			throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(
				CompoundKey.class).build();

		assertThat(idMeta.getPartitionKeySetter()).isNull();
	}

	@Test
	public void should_get_component_names() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class).compNames("a", "b").build();

		assertThat(idMeta.getComponentNames()).containsExactly("a", "b");
	}

	@Test
	public void should_get_empty_component_names() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(
				CompoundKey.class).build();

		assertThat(idMeta.getComponentNames()).isEmpty();
	}

	@Test
	public void should_get_component_setters() throws Exception {
		Method idSetter = CompoundKey.class.getDeclaredMethod("setUserId",
				Long.class);
		Method nameSetter = CompoundKey.class.getDeclaredMethod("setName",
				String.class);

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compSetters(Arrays.asList(idSetter, nameSetter)).build();

		assertThat(idMeta.getComponentSetters()).containsExactly(idSetter,
				nameSetter);
	}

	@Test
	public void should_return_empty_list_when_no_component_setters()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(
				CompoundKey.class).build();

		assertThat(idMeta.getComponentSetters()).isEmpty();
	}

	@Test
	public void should_get_component_classes() throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class)
				.compClasses(Arrays.<Class<?>> asList(Long.class, String.class))
				.build();

		assertThat(idMeta.getComponentClasses()).containsExactly(Long.class,
				String.class);
	}

	@Test
	public void should_return_empty_list_when_no_component_classes()
			throws Exception {

		PropertyMeta idMeta = PropertyMetaTestBuilder.valueClass(
				CompoundKey.class).build();

		assertThat(idMeta.getComponentClasses()).isEmpty();
	}

	@Test
	public void should_return_true_for_is_embedded_id() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class).type(EMBEDDED_ID).build();

		assertThat(idMeta.isEmbeddedId()).isTrue();
	}

	@Test
	public void should_return_false_for_is_embedded_id() throws Exception {
		PropertyMeta idMeta = PropertyMetaTestBuilder
				.valueClass(CompoundKey.class).type(ID).build();

		assertThat(idMeta.isEmbeddedId()).isFalse();
	}

	@Test
	public void should_get_read_consistency() throws Exception {
		PropertyMeta pm = new PropertyMeta();

		assertThat(pm.getReadConsistencyLevel()).isNull();
		assertThat(pm.getWriteConsistencyLevel()).isNull();

		pm.setConsistencyLevels(Pair
				.<ConsistencyLevel, ConsistencyLevel> create(QUORUM, QUORUM));

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

		when(transcoder.forceDecodeFromJSON("value", String.class)).thenReturn(
				"decoded");

		Object decoded = pm.forceDecodeFromJSON("value");

		assertThat(decoded).isEqualTo("decoded");
	}

	@Test
	public void should_force_decode_from_json_join_value() throws Exception {

		PropertyMeta joinIdMeta = new PropertyMeta();
		joinIdMeta.setType(ID);
		joinIdMeta.setValueClass(Long.class);
		joinIdMeta.setTranscoder(transcoder);

		EntityMeta joinMeta = new EntityMeta();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta pm = new PropertyMeta();
		pm.setType(JOIN_SIMPLE);
		pm.setValueClass(UserBean.class);
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);
		pm.setJoinProperties(joinProperties);

		when(transcoder.forceDecodeFromJSON("11", Long.class)).thenReturn(11L);

		Object decoded = pm.forceDecodeFromJSON("11");

		assertThat(decoded).isEqualTo(11L);
	}

	@Test
	public void should_force_decode_from_json_to_type() throws Exception {
		PropertyMeta pm = new PropertyMeta();
		pm.setTranscoder(transcoder);

		when(transcoder.forceDecodeFromJSON("test", String.class)).thenReturn(
				"test");

		assertThat(pm.forceDecodeFromJSON("test", String.class)).isEqualTo(
				"test");
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
	public void should_exception_when_asking_primary_key_on_non_id_field()
			throws Exception {

		CompleteBean entity = new CompleteBean();

		PropertyMeta pm = new PropertyMeta();
		pm.setPropertyName("property");
		pm.setType(SIMPLE);

		exception.expect(IllegalStateException.class);
		exception
				.expectMessage("Cannot get primary key on a non id field 'property'");

		pm.getPrimaryKey(entity);
	}

	@Test
	public void should_get_join_primary_key() throws Exception {

		long userId = RandomUtils.nextLong();
		UserBean entity = new UserBean();
		entity.setUserId(userId);

		EntityMeta joinMeta = mock(EntityMeta.class);

		PropertyMeta pm = new PropertyMeta();
		pm.setType(JOIN_SIMPLE);
		JoinProperties joinProperties = new JoinProperties();
		joinProperties.setEntityMeta(joinMeta);
		pm.setJoinProperties(joinProperties);

		when(joinMeta.getPrimaryKey(entity)).thenReturn(userId);
		assertThat(pm.getJoinPrimaryKey(entity)).isEqualTo(userId);
	}

	@Test
	public void should_exception_when_asking_join_primary_key_on_non_join_field()
			throws Exception {

		UserBean entity = new UserBean();

		PropertyMeta pm = new PropertyMeta();
		pm.setType(SIMPLE);
		pm.setPropertyName("property");

		exception.expect(IllegalStateException.class);
		exception
				.expectMessage("Cannot get join primary key on a non join field 'property'");

		pm.getJoinPrimaryKey(entity);
	}

	@Test
	public void should_get_partition_key() throws Exception {

		long id = RandomUtils.nextLong();
		CompoundKey compoundKey = new CompoundKey(id, "name");

		PropertyMeta pm = new PropertyMeta();
		pm.setType(EMBEDDED_ID);
		pm.setInvoker(invoker);

		when(invoker.getPartitionKey(compoundKey, pm)).thenReturn(id);

		assertThat(pm.getPartitionKey(compoundKey)).isEqualTo(id);
	}

	@Test
	public void should_exception_when_asking_partition_key_on_non_embedded_id_field()
			throws Exception {

		CompoundKey compoundKey = new CompoundKey();

		PropertyMeta pm = new PropertyMeta();
		pm.setPropertyName("property");
		pm.setType(SIMPLE);

		exception.expect(IllegalStateException.class);
		exception
				.expectMessage("Cannot get partition key on a non embedded id field 'property'");

		pm.getPartitionKey(compoundKey);
	}

	@Test
	public void should_instanciate_embedded_id_with_partition_key()
			throws Exception {

		long id = RandomUtils.nextLong();
		CompoundKey compoundKey = new CompoundKey(id, "name");

		PropertyMeta pm = new PropertyMeta();
		pm.setType(EMBEDDED_ID);
		pm.setInvoker(invoker);

		when(invoker.instanciateEmbeddedIdWithPartitionKey(pm, id)).thenReturn(
				compoundKey);

		assertThat(pm.instanciateEmbeddedIdWithPartitionKey(id)).isEqualTo(
				compoundKey);
	}

	@Test
	public void should_exception_when_instanciating_embedded_id_on_non_embedded_id_field()
			throws Exception {

		long id = RandomUtils.nextLong();
		PropertyMeta pm = new PropertyMeta();
		pm.setPropertyName("property");
		pm.setType(SIMPLE);

		exception.expect(IllegalStateException.class);
		exception
				.expectMessage("Cannot instanciate embedded id with partition key on a non embedded id field 'property'");

		pm.instanciateEmbeddedIdWithPartitionKey(id);
	}

	@Test
	public void should_get_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		entity.setName("name");

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(SIMPLE).invoker(invoker).build();

		when(invoker.getValueFromField(entity, pm.getGetter())).thenReturn(
				"name");

		assertThat(pm.getValueFromField(entity)).isEqualTo("name");
	}

	@Test
	public void should_get_list_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		List<String> friends = Arrays.asList("foo", "bar");
		entity.setFriends(friends);

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("friends")
				.accessors().type(LIST).invoker(invoker).build();

		when(
				(List<String>) invoker.getListValueFromField(entity,
						pm.getGetter())).thenReturn(friends);

		assertThat((List<String>) pm.getListValueFromField(entity))
				.containsExactly("foo", "bar");
	}

	@Test
	public void should_get_set_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		Set<String> followers = Sets.newHashSet("George", "Paul");
		entity.setFollowers(followers);

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("followers")
				.accessors().type(SET).invoker(invoker).build();

		when((Set<String>) invoker.getSetValueFromField(entity, pm.getGetter()))
				.thenReturn(followers);

		assertThat((Set<String>) pm.getSetValueFromField(entity)).containsOnly(
				"George", "Paul");
	}

	@Test
	public void should_get_map_value_from_field() throws Exception {

		CompleteBean entity = new CompleteBean();
		Map<Integer, String> preferences = ImmutableMap.of(1, "FR", 2, "Paris");
		entity.setPreferences(preferences);

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Integer.class, String.class).field("preferences")
				.accessors().type(MAP).invoker(invoker).build();

		when(
				(Map<Integer, String>) invoker.getMapValueFromField(entity,
						pm.getGetter())).thenReturn(preferences);

		Map<Integer, String> actual = (Map<Integer, String>) pm
				.getMapValueFromField(entity);

		assertThat(actual).containsKey(1).containsKey(2).containsValue("FR")
				.containsValue("Paris");
	}

	@Test
	public void should_set_value_to_field() throws Exception {

		CompleteBean entity = new CompleteBean();

		PropertyMeta pm = PropertyMetaTestBuilder
				.completeBean(Void.class, String.class).field("name")
				.accessors().type(SIMPLE).invoker(invoker).build();

		pm.setValueToField(entity, "name");

		verify(invoker).setValueToField(entity, pm.getSetter(), "name");
	}
}
