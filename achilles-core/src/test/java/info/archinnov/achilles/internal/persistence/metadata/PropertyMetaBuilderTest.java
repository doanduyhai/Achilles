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
package info.archinnov.achilles.internal.persistence.metadata;

import static info.archinnov.achilles.internal.persistence.metadata.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.internal.persistence.metadata.transcoding.CompoundTranscoder;
import info.archinnov.achilles.internal.persistence.metadata.transcoding.ListTranscoder;
import info.archinnov.achilles.internal.persistence.metadata.transcoding.MapTranscoder;
import info.archinnov.achilles.internal.persistence.metadata.transcoding.SetTranscoder;
import info.archinnov.achilles.internal.persistence.metadata.transcoding.SimpleTranscoder;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.Bean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

public class PropertyMetaBuilderTest {
	private Method[] accessors = new Method[2];
    private Field field;

	private ObjectMapper objectMapper = new ObjectMapper();

	@Before
	public void setUp() throws Exception {
		accessors[0] = Bean.class.getDeclaredMethod("getId");
		accessors[1] = Bean.class.getDeclaredMethod("setId", Long.class);
        field = CompleteBean.class.getDeclaredField("id");
	}

	@Test
	public void should_build_simple() throws Exception {

		PropertyMeta built = PropertyMetaBuilder.factory().type(SIMPLE).propertyName("prop").accessors(accessors)
                .field(field).objectMapper(objectMapper).consistencyLevels(Pair.create(ONE, ALL)).build(Void.class, String.class);

		assertThat(built.type()).isEqualTo(SIMPLE);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.getField()).isEqualTo(field);
		assertThat(built.isEmbeddedId()).isFalse();
		assertThat(built.getReadConsistencyLevel()).isEqualTo(ONE);
		assertThat(built.getWriteConsistencyLevel()).isEqualTo(ALL);
		assertThat(built.getTranscoder()).isInstanceOf(SimpleTranscoder.class);
	}

	@Test
	public void should_build_compound_id() throws Exception {

		EmbeddedIdProperties props = new EmbeddedIdProperties(null, null, null, null, null, null,null, null);

		PropertyMeta built = PropertyMetaBuilder.factory().type(EMBEDDED_ID).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).consistencyLevels(Pair.create(ONE, ALL)).embeddedIdProperties(props)
				.build(Void.class, EmbeddedKey.class);

		assertThat(built.type()).isEqualTo(EMBEDDED_ID);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<EmbeddedKey> getValueClass()).isEqualTo(EmbeddedKey.class);

		assertThat(built.isEmbeddedId()).isTrue();
		assertThat(built.getReadConsistencyLevel()).isEqualTo(ONE);
		assertThat(built.getWriteConsistencyLevel()).isEqualTo(ALL);
		assertThat(built.getTranscoder()).isInstanceOf(CompoundTranscoder.class);
	}


	@Test
	public void should_build_simple_with_object_as_value() throws Exception {
		PropertyMeta built = PropertyMetaBuilder.factory().type(SIMPLE).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Void.class, Bean.class);

		assertThat(built.type()).isEqualTo(SIMPLE);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<Bean> getValueClass()).isEqualTo(Bean.class);

		assertThat(built.isEmbeddedId()).isFalse();
		assertThat(built.getTranscoder()).isInstanceOf(SimpleTranscoder.class);
	}

	@Test
	public void should_build_list() throws Exception {

		PropertyMeta built = PropertyMetaBuilder.factory().type(LIST).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Void.class, String.class);

		assertThat(built.type()).isEqualTo(LIST);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.isEmbeddedId()).isFalse();
		assertThat(built.getTranscoder()).isInstanceOf(ListTranscoder.class);
	}

	@Test
	public void should_build_set() throws Exception {

		PropertyMeta built = PropertyMetaBuilder.factory().type(SET).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Void.class, String.class);

		assertThat(built.type()).isEqualTo(SET);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.isEmbeddedId()).isFalse();
		assertThat(built.getTranscoder()).isInstanceOf(SetTranscoder.class);
	}

	@Test
	public void should_build_map() throws Exception {

		PropertyMeta built = PropertyMetaBuilder.factory().type(MAP).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Integer.class, String.class);

		assertThat(built.type()).isEqualTo(MAP);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<Integer> getKeyClass()).isEqualTo(Integer.class);

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.isEmbeddedId()).isFalse();
		assertThat(built.getTranscoder()).isInstanceOf(MapTranscoder.class);
	}

	@Test
	public void should_build_map_with_object_as_key() throws Exception {
		PropertyMeta built = PropertyMetaBuilder.factory().type(MAP).propertyName("prop").accessors(accessors)
				.objectMapper(objectMapper).build(Bean.class, String.class);

		assertThat(built.type()).isEqualTo(MAP);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<Bean> getKeyClass()).isEqualTo(Bean.class);

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.getTranscoder()).isInstanceOf(MapTranscoder.class);
	}
}
