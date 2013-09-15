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
package info.archinnov.achilles.entity.metadata.transcoding;

import static info.archinnov.achilles.entity.metadata.PropertyType.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.mapping.entity.UserBean;
import info.archinnov.achilles.test.parser.entity.CompoundKey;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class AbstractTranscoderTest {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Mock(answer = Answers.CALLS_REAL_METHODS)
	private AbstractTranscoder transcoder;

	@Mock
	private ObjectMapper objectMapper;

	@Mock
	private ReflectionInvoker invoker;

	@Mock
	private PropertyMeta pm;

	@Before
	public void setUp() {
		Whitebox.setInternalState(transcoder, ObjectMapper.class, objectMapper);
		Whitebox.setInternalState(transcoder, ReflectionInvoker.class, invoker);
	}

	@Test
	public void should_encode_supported_type() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.type(SIMPLE).build();

		Object actual = transcoder.encode(pm, String.class, "value");

		assertThat(actual).isEqualTo("value");
	}

	@Test
	public void should_encode_enum_type() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder
				.valueClass(PropertyType.class).type(SIMPLE).build();

		Object actual = transcoder.encode(pm, PropertyType.class, SIMPLE);

		assertThat(actual).isEqualTo("SIMPLE");
	}

	@Test
	public void should_encode_unsopported_type_to_json() throws Exception {
		UserBean bean = new UserBean();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		when(objectMapper.writeValueAsString(bean)).thenReturn("json_bean");
		Object actual = transcoder.encode(pm, UserBean.class, bean);

		assertThat(actual).isEqualTo("json_bean");
	}

	@Test
	public void should_delegate_encoding_to_join_type() throws Exception {
		Long joinId = RandomUtils.nextLong();
		UserBean bean = new UserBean();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(pm);

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.valueClass(UserBean.class).type(JOIN_SIMPLE).joinMeta(meta)
				.build();

		when(invoker.getPrimaryKey(bean, pm)).thenReturn(joinId);
		when(pm.encode(joinId)).thenReturn(joinId);

		Object actual = transcoder.encode(propertyMeta, UserBean.class, bean);

		assertThat(actual).isEqualTo(joinId);
	}

	@Test
	public void should_decode_supported_type() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class)
				.type(SIMPLE).build();

		Object actual = transcoder.decode(pm, String.class, "value");

		assertThat(actual).isEqualTo("value");
	}

	@Test
	public void should_decode_enum_type() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder
				.valueClass(PropertyType.class).type(SIMPLE).build();

		Object actual = transcoder.decode(pm, PropertyType.class, "SIMPLE");

		assertThat(actual).isEqualTo(SIMPLE);
	}

	@Test
	public void should_decode_unsopported_type_to_json() throws Exception {
		UserBean bean = new UserBean();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		when(objectMapper.readValue("json_bean", UserBean.class)).thenReturn(
				bean);
		Object actual = transcoder.decode(pm, UserBean.class, "json_bean");

		assertThat(actual).isEqualTo(bean);
	}

	@Test
	public void should_delegate_decoding_to_join_type() throws Exception {
		UserBean bean = new UserBean();

		EntityMeta meta = new EntityMeta();
		meta.setIdMeta(pm);

		PropertyMeta propertyMeta = PropertyMetaTestBuilder
				.valueClass(UserBean.class).type(JOIN_SIMPLE).joinMeta(meta)
				.build();

		when(pm.decode(bean)).thenReturn(bean);

		Object actual = transcoder.decode(propertyMeta, UserBean.class, bean);

		assertThat(actual).isEqualTo(bean);
	}

	@Test
	public void should_exception_when_unsupported_type_for_decoding_is_not_string()
			throws Exception {
		UserBean bean = new UserBean();

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Error while decoding value '" + bean
				+ "' to type '" + UserBean.class.getCanonicalName() + "'");

		transcoder.decode(pm, UserBean.class, bean);
	}

	// /////////////

	@Test
	public void should_exception_by_default_on_encode_object() throws Exception {
		UserBean bean = new UserBean();
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot encode value '" + bean
				+ "' for type '" + pm.type().name() + "'");

		transcoder.encode(pm, bean);
	}

	@Test
	public void should_exception_by_default_on_encode_list() throws Exception {
		List<UserBean> list = Arrays.asList(new UserBean());
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot encode value '" + list
				+ "' for type '" + pm.type().name() + "'");

		transcoder.encode(pm, list);
	}

	@Test
	public void should_exception_by_default_on_encode_set() throws Exception {
		Set<UserBean> set = Sets.newHashSet(new UserBean());
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot encode value '" + set
				+ "' for type '" + pm.type().name() + "'");

		transcoder.encode(pm, set);
	}

	@Test
	public void should_exception_by_default_on_encode_map() throws Exception {
		Map<Integer, UserBean> map = ImmutableMap.of(1, new UserBean());
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot encode value '" + map
				+ "' for type '" + pm.type().name() + "'");

		transcoder.encode(pm, map);
	}

	@Test
	public void should_exception_by_default_on_encode_to_components()
			throws Exception {
		CompoundKey compound = new CompoundKey();
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot encode from value '"
				+ compound + "' to components for type '" + pm.type().name()
				+ "'");

		transcoder.encodeToComponents(pm, compound);
	}

	@Test
	public void should_exception_by_default_on_decode_object() throws Exception {
		UserBean bean = new UserBean();
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot decode value '" + bean
				+ "' for type '" + pm.type().name() + "'");

		transcoder.decode(pm, bean);
	}

	@Test
	public void should_exception_by_default_on_decode_list() throws Exception {
		List<UserBean> list = Arrays.asList(new UserBean());
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot decode value '" + list
				+ "' for type '" + pm.type().name() + "'");

		transcoder.decode(pm, list);
	}

	@Test
	public void should_exception_by_default_on_decode_set() throws Exception {
		Set<UserBean> set = Sets.newHashSet(new UserBean());
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot decode value '" + set
				+ "' for type '" + pm.type().name() + "'");

		transcoder.decode(pm, set);
	}

	@Test
	public void should_exception_by_default_on_decode_map() throws Exception {
		Map<Integer, UserBean> map = ImmutableMap.of(1, new UserBean());
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(UserBean.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception.expectMessage("Transcoder cannot decode value '" + map
				+ "' for type '" + pm.type().name() + "'");

		transcoder.decode(pm, map);
	}

	@Test
	public void should_exception_by_default_on_decode_from_components()
			throws Exception {
		List<Object> components = new ArrayList<Object>();
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(CompoundKey.class)
				.type(SIMPLE).build();

		exception.expect(AchillesException.class);
		exception
				.expectMessage("Transcoder cannot decode from components '"
						+ components + "' to value for type '"
						+ pm.type().name() + "'");

		transcoder.decodeFromComponents(pm, components);
	}

	@Test
	public void should_force_encode_to_json_object_type() throws Exception {
		when(objectMapper.writeValueAsString(10L)).thenReturn("10");

		assertThat(transcoder.forceEncodeToJSON(10L)).isEqualTo("10");
	}

	@Test
	public void should_force_encode_to_json_string_type() throws Exception {
		assertThat(transcoder.forceEncodeToJSON("test")).isEqualTo("test");
		verifyZeroInteractions(objectMapper);
	}

	@Test
	public void should_return_null_when_force_encode_null() throws Exception {

		assertThat(transcoder.forceEncodeToJSON(null)).isNull();
	}

	@Test
	public void should_force_decode_from_json_object_type() throws Exception {
		when(objectMapper.readValue("10", Long.class)).thenReturn(10L);

		assertThat(transcoder.forceDecodeFromJSON("10", Long.class)).isEqualTo(
				10L);
	}

	@Test
	public void should_force_decode_to_json_string_type() throws Exception {
		assertThat(transcoder.forceDecodeFromJSON("test", String.class))
				.isEqualTo("test");
		verifyZeroInteractions(objectMapper);
	}

	@Test
	public void should_return_null_when_force_decode_null() throws Exception {

		assertThat(transcoder.forceDecodeFromJSON(null, Long.class)).isNull();
	}
}
