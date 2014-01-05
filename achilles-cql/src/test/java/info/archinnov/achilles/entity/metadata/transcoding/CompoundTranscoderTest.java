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

import static info.archinnov.achilles.entity.metadata.PropertyType.EMBEDDED_ID;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.lang.math.RandomUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.reflection.ReflectionInvoker;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;

@RunWith(MockitoJUnitRunner.class)
public class CompoundTranscoderTest {

	private CompoundTranscoder transcoder;

	@Mock
	private ObjectMapper objectMapper;

	@Mock
	private ReflectionInvoker invoker;

	@Before
	public void setUp() {
		transcoder = new CompoundTranscoder(objectMapper);
	}

	@Test
	public void should_encode_to_components() throws Exception {
		Long userId = RandomUtils.nextLong();
		String name = "name";
		EmbeddedKey compound = new EmbeddedKey(userId, name);

		Field userIdField = EmbeddedKey.class.getDeclaredField("userId");
        Field nameField = EmbeddedKey.class.getDeclaredField("name");

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.compClasses(Long.class, String.class).compFields(userIdField, nameField).build();

		when(invoker.getValueFromField(compound, userIdField)).thenReturn(userId);
		when(invoker.getValueFromField(compound, nameField)).thenReturn(name);

		List<Object> actual = transcoder.encodeToComponents(pm, compound);

		assertThat(actual).containsExactly(userId, name);
	}

	@Test
	public void should_encode_to_null_components() throws Exception {

		Method userIdGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = EmbeddedKey.class.getDeclaredMethod("getName");

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.compClasses(Long.class, String.class).compGetters(userIdGetter, nameGetter).build();
		List<Object> actual = transcoder.encodeToComponents(pm, (Object) null);

		assertThat(actual).isEmpty();
	}

	@Test
	public void should_not_encode_to_null_components() throws Exception {
		Method userIdGetter = EmbeddedKey.class.getDeclaredMethod("getUserId");
		Method nameGetter = EmbeddedKey.class.getDeclaredMethod("getName");

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.compClasses(Long.class, String.class).compGetters(userIdGetter, nameGetter).build();

		List<Object> actual = transcoder.encodeToComponents(pm, Arrays.<Object> asList());

		assertThat(actual).isEmpty();
	}

	@Test
	public void should_encode_components() throws Exception {
		Long userId = RandomUtils.nextLong();
		String name = "name";

		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.compClasses(Long.class, String.class, PropertyType.class).build();

		List<Object> actual = transcoder.encodeToComponents(pm,
				Arrays.<Object> asList(userId, PropertyType.EMBEDDED_ID, name));

		assertThat(actual).containsExactly(userId, "EMBEDDED_ID", name);
	}

	@Test
	public void should_encode_null_components() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.compClasses(Long.class, String.class, PropertyType.class).build();

		List<Object> actual = transcoder.encodeToComponents(pm, Arrays.<Object> asList(null, null));

		assertThat(actual).isEmpty();
	}

	@Test
	public void should_decode_from_components_with_injection_by_setters() throws Exception {
		Long userId = RandomUtils.nextLong();
		String name = "name";

		Field userIdField = EmbeddedKey.class.getDeclaredField("userId");
        Field nameField = EmbeddedKey.class.getDeclaredField("name");
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(EmbeddedKey.class).type(EMBEDDED_ID)
				.compClasses(Long.class, String.class).compFields(userIdField, nameField).invoker(invoker).build();

		when(invoker.instantiate(EmbeddedKey.class)).thenReturn(new EmbeddedKey());

		Object actual = transcoder.decodeFromComponents(pm, Arrays.<Object> asList(userId, name));

		assertThat(actual).isInstanceOf(EmbeddedKey.class);

		EmbeddedKey compound = (EmbeddedKey) actual;

		assertThat(compound.getUserId()).isEqualTo(userId);
		assertThat(compound.getName()).isEqualTo(name);
	}
}
