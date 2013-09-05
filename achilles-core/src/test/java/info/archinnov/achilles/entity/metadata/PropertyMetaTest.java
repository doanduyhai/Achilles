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
import static org.mockito.Mockito.when;
import info.archinnov.achilles.entity.metadata.transcoding.DataTranscoder;
import info.archinnov.achilles.entity.metadata.transcoding.SimpleTranscoder;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.TweetCompoundKey;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;
import info.archinnov.achilles.test.parser.entity.CompoundKeyByConstructor;
import info.archinnov.achilles.type.ConsistencyLevel;

import java.lang.reflect.Constructor;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class PropertyMetaTest {

	private ObjectMapper objectMapper = new ObjectMapper();

	@Mock
	private DataTranscoder transcoder;

	@Test
	public void should_get_value_from_string_with_correct_type()
			throws Exception {
		PropertyMeta meta = new PropertyMeta();
		meta.setValueClass(Long.class);
		meta.setObjectMapper(objectMapper);

		Object testString = "123";

		Object casted = meta.getValueFromString(testString);

		assertThat(casted).isInstanceOf(Long.class);
	}

	@Test
	public void should_get_value_from_string() throws Exception {
		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setValueClass(String.class);
		propertyMeta.setObjectMapper(objectMapper);

		Object test = "test";

		Object value = propertyMeta.getValueFromString(test);
		assertThat(value).isEqualTo("test");
	}

	@Test
	public void should_get_primitive_value_from_string() throws Exception {

		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setValueClass(boolean.class);
		propertyMeta.setObjectMapper(objectMapper);

		Object test = "true";

		Boolean value = (Boolean) propertyMeta.getValueFromString(test);
		assertThat(value).isTrue();
	}

	@Test
	public void should_get_enum_value_from_string() throws Exception {

		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setValueClass(PropertyType.class);
		propertyMeta.setObjectMapper(objectMapper);

		Object test = "\"JOIN_MAP\"";

		PropertyType value = (PropertyType) propertyMeta
				.getValueFromString(test);
		assertThat(value).isEqualTo(PropertyType.JOIN_MAP);
	}

	@Test
	public void should_get_allowed_type_from_string() throws Exception {

		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setValueClass(UUID.class);
		propertyMeta.setObjectMapper(objectMapper);

		UUID uuid = new UUID(10L, 100L);

		Object test = objectMapper.writeValueAsString(uuid);

		UUID value = (UUID) propertyMeta.getValueFromString(test);
		assertThat(value).isEqualTo(uuid);
	}

	@Test
	public void should_write_value_to_string() throws Exception {
		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setObjectMapper(objectMapper);

		UUID timeUUID = new UUID(10L, 100L);

		String uuidString = objectMapper.writeValueAsString(timeUUID);

		String converted = propertyMeta.writeValueToString(timeUUID);

		assertThat(converted).isEqualTo(uuidString);
	}

	@Test
	public void should_write_value_as_supported_type() throws Exception {
		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setObjectMapper(objectMapper);
		propertyMeta.setValueClass(String.class);

		String value = "test";

		Object converted = propertyMeta
				.writeValueAsSupportedTypeOrString(value);

		assertThat(converted).isSameAs(value);
	}

	@Test
	public void should_write_value_as_string_because_not_supported_type()
			throws Exception {
		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setObjectMapper(objectMapper);

		UserBean value = new UserBean();
		String stringValue = objectMapper.writeValueAsString(value);

		Object converted = propertyMeta
				.writeValueAsSupportedTypeOrString(value);

		assertThat(converted).isEqualTo(stringValue);
	}

	@Test
	public void should_cast_key() throws Exception {
		PropertyMeta propertyMeta = new PropertyMeta();
		propertyMeta.setKeyClass(String.class);
		propertyMeta.setObjectMapper(objectMapper);

		Object test = "test";

		Object key = propertyMeta.getKey(test);
		assertThat(key).isEqualTo("test");
	}

	@Test
	public void should_cast_value_as_join_type() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.JOIN_SIMPLE).build();

		UserBean uuid = new UserBean();

		Object cast = propertyMeta.castValue(uuid);

		assertThat(cast).isSameAs(uuid);
	}

	@Test
	public void should_cast_value_as_supported_type() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UUID.class)
				.type(PropertyType.SIMPLE).build();

		Object uuid = new UUID(10L, 100L);

		Object cast = propertyMeta.castValue(uuid);

		assertThat(cast).isSameAs(uuid);
	}

	@Test
	public void should_cast_value_as_string() throws Exception {
		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.SIMPLE).build();

		UserBean bean = new UserBean();
		bean.setName("name");
		bean.setUserId(12L);

		UserBean cast = (UserBean) propertyMeta.castValue(objectMapper
				.writeValueAsString(bean));

		assertThat(cast.getName()).isEqualTo(bean.getName());
		assertThat(cast.getUserId()).isEqualTo(bean.getUserId());
	}

	@Test
	public void should_return_join_entity_meta() throws Exception {
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
	public void should_return_join_id_meta() throws Exception {
		EntityMeta joinMeta = new EntityMeta();
		PropertyMeta joinIdMeta = new PropertyMeta();
		joinMeta.setIdMeta(joinIdMeta);

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.keyValueClass(Integer.class, UserBean.class)
				.type(PropertyType.JOIN_MAP).joinMeta(joinMeta).build();

		assertThat((PropertyMeta) propertyMeta.joinIdMeta()).isSameAs(
				joinIdMeta);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void should_return_counter_id_meta() throws Exception {
		PropertyMeta idMeta = new PropertyMeta();

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.valueClass(Long.class) //
				.type(PropertyType.COUNTER)
				//
				.counterIdMeta(idMeta)
				//
				.build();

		assertThat((PropertyMeta) propertyMeta.counterIdMeta())
				.isSameAs(idMeta);
	}

	public void should_return_fqcn() throws Exception {

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.valueClass(Long.class) //
				.type(PropertyType.COUNTER).fqcn("fqcn").build();

		assertThat(propertyMeta.fqcn()).isEqualTo("fqcn");
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
	public void should_return_true_if_type_is_join_collection()
			throws Exception {
		PropertyMeta pm1 = PropertyMetaTestBuilder.valueClass(String.class)
				.type(JOIN_LIST).build();

		PropertyMeta pm2 = PropertyMetaTestBuilder.valueClass(String.class)
				.type(JOIN_SET).build();

		assertThat(pm1.isJoinCollection()).isTrue();
		assertThat(pm2.isJoinCollection()).isTrue();
	}

	@Test
	public void should_return_true_if_type_is_join_map() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.type(JOIN_MAP).build();
		assertThat(pm.isJoinMap()).isTrue();
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
	public void should_return_compound_key_constructor() throws Exception {
		Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class
				.getConstructor(Long.class, String.class);
		EmbeddedIdProperties props = new EmbeddedIdProperties();
		props.setConstructor(constructor);

		PropertyMeta meta = new PropertyMeta();
		meta.setEmbeddedIdProperties(props);

		assertThat(meta.<CompoundKeyByConstructor> getEmbeddedIdConstructor())
				.isSameAs(constructor);
	}

	@Test
	public void should_return_null_when_no_compound_key_constructor()
			throws Exception {
		PropertyMeta meta = new PropertyMeta();
		assertThat(meta.getEmbeddedIdConstructor()).isNull();
	}

	@Test
	public void should_return_true_when_compound_key_has_default_constructor()
			throws Exception {

		Constructor<TweetCompoundKey> constructor = TweetCompoundKey.class
				.getConstructor();
		EmbeddedIdProperties props = new EmbeddedIdProperties();
		props.setConstructor(constructor);

		PropertyMeta meta = new PropertyMeta();
		meta.setEmbeddedIdProperties(props);

		assertThat(meta.hasDefaultConstructorForEmbeddedId()).isTrue();
	}

	@Test
	public void should_return_false_when_compound_key_has_no_default_constructor()
			throws Exception {

		Constructor<CompoundKeyByConstructor> constructor = CompoundKeyByConstructor.class
				.getConstructor(Long.class, String.class);
		EmbeddedIdProperties props = new EmbeddedIdProperties();
		props.setConstructor(constructor);

		PropertyMeta meta = new PropertyMeta();
		meta.setEmbeddedIdProperties(props);

		assertThat(meta.hasDefaultConstructorForEmbeddedId()).isFalse();
	}

	@Test
	public void should_return_false_when_no_compound_key_constructor()
			throws Exception {
		PropertyMeta meta = new PropertyMeta();
		assertThat(meta.hasDefaultConstructorForEmbeddedId()).isFalse();
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
		assertThat(pm.encodeComponents((List<?>) null)).isNull();

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
		when(transcoder.encodeComponents(pm, list)).thenReturn(list);

		assertThat(pm.encode(value)).isEqualTo(value);
		assertThat(pm.encodeKey(value)).isEqualTo(value);
		assertThat(pm.encode(list)).isEqualTo(list);
		assertThat(pm.encode(set)).isEqualTo(set);
		assertThat(pm.encode(map)).isEqualTo(map);
		assertThat(pm.encodeToComponents(list)).isEqualTo(list);
		assertThat(pm.encodeComponents(list)).isEqualTo(list);
	}

}
