/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package info.archinnov.achilles.internal.metadata.holder;

import static info.archinnov.achilles.internal.metadata.holder.PropertyType.*;
import static info.archinnov.achilles.type.ConsistencyLevel.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import info.archinnov.achilles.codec.Codec;
import info.archinnov.achilles.internal.metadata.codec.ListCodec;
import info.archinnov.achilles.internal.metadata.codec.MapCodec;
import info.archinnov.achilles.internal.metadata.codec.SetCodec;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.Bean;
import info.archinnov.achilles.test.parser.entity.EmbeddedKey;
import info.archinnov.achilles.type.Counter;
import info.archinnov.achilles.type.Pair;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

import com.fasterxml.jackson.databind.ObjectMapper;
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

        Codec simpleCodec = mock(Codec.class);

		PropertyMeta built = PropertyMetaBuilder.factory().type(SIMPLE).propertyName("prop").accessors(accessors)
                .field(field).objectMapper(objectMapper)
                .consistencyLevels(Pair.create(ONE, ALL))
                .simpleCodec(simpleCodec)
                .build(Void.class, String.class);

		assertThat(built.type()).isEqualTo(SIMPLE);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);

		assertThat(built.getField()).isEqualTo(field);
		assertThat(built.structure().isEmbeddedId()).isFalse();
		assertThat(built.config().getReadConsistencyLevel()).isEqualTo(ONE);
		assertThat(built.config().getWriteConsistencyLevel()).isEqualTo(ALL);
		assertThat(built.getSimpleCodec()).isSameAs(simpleCodec);
	}

	@Test
	public void should_build_compound_id() throws Exception {

        EmbeddedIdProperties props = mock(EmbeddedIdProperties.class);

        PropertyMeta built = PropertyMetaBuilder.factory().type(EMBEDDED_ID).propertyName("prop").accessors(accessors)
                .objectMapper(objectMapper).consistencyLevels(Pair.create(ONE, ALL)).embeddedIdProperties(props)
                .build(Void.class, EmbeddedKey.class);

        assertThat(built.type()).isEqualTo(EMBEDDED_ID);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.<EmbeddedKey>getValueClass()).isEqualTo(EmbeddedKey.class);

        assertThat(built.structure().isEmbeddedId()).isTrue();
        assertThat(built.config().getReadConsistencyLevel()).isEqualTo(ONE);
        assertThat(built.config().getWriteConsistencyLevel()).isEqualTo(ALL);
        assertThat(built.getSimpleCodec()).isNull();
    }


	@Test
	public void should_build_simple_with_object_as_value() throws Exception {
        Codec simpleCodec = mock(Codec.class);

        PropertyMeta built = PropertyMetaBuilder.factory()
                .type(SIMPLE)
                .propertyName("prop")
                .accessors(accessors)
				.objectMapper(objectMapper)
                .simpleCodec(simpleCodec)
                .build(Void.class, Bean.class);

		assertThat(built.type()).isEqualTo(SIMPLE);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<Bean> getValueClass()).isEqualTo(Bean.class);

		assertThat(built.structure().isEmbeddedId()).isFalse();
        assertThat(built.getSimpleCodec()).isSameAs(simpleCodec);
    }

	@Test
	public void should_build_list_with_default_empty_when_null() throws Exception {

        ListCodec listCodec = mock(ListCodec.class);

        PropertyMeta built = PropertyMetaBuilder.factory()
                .type(LIST)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .emptyCollectionAndMapIfNull(true)
                .listCodec(listCodec)
                .build(Void.class, String.class);

        assertThat(built.type()).isEqualTo(LIST);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.<String>getValueClass()).isEqualTo(String.class);

        assertThat(built.structure().isEmbeddedId()).isFalse();
        assertThat(built.forValues().nullValueForCollectionAndMap()).isNotNull().isInstanceOf(List.class);
        assertThat(built.getListCodec()).isSameAs(listCodec);
    }

	@Test
	public void should_build_set() throws Exception {

        SetCodec setCodec = mock(SetCodec.class);

        PropertyMeta built = PropertyMetaBuilder.factory()
                .type(SET)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .setCodec(setCodec)
                .build(Void.class, String.class);

        assertThat(built.type()).isEqualTo(SET);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.<String>getValueClass()).isEqualTo(String.class);

        assertThat(built.structure().isEmbeddedId()).isFalse();
        assertThat(built.getSetCodec()).isSameAs(setCodec);
    }

	@Test
	public void should_build_map() throws Exception {

        MapCodec mapCodec = mock(MapCodec.class);

        PropertyMeta built = PropertyMetaBuilder.factory()
                .type(MAP)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .mapCodec(mapCodec)
                .build(Integer.class, String.class);

        assertThat(built.type()).isEqualTo(MAP);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.<Integer>getKeyClass()).isEqualTo(Integer.class);

        assertThat(built.<String>getValueClass()).isEqualTo(String.class);

        assertThat(built.structure().isEmbeddedId()).isFalse();
        assertThat(built.getMapCodec()).isSameAs(mapCodec);
    }

	@Test
	public void should_build_map_with_object_as_key() throws Exception {

        MapCodec mapCodec = mock(MapCodec.class);

        PropertyMeta built = PropertyMetaBuilder.factory()
                .type(MAP)
                .propertyName("prop")
                .accessors(accessors)
				.objectMapper(objectMapper)
                .mapCodec(mapCodec)
                .build(Bean.class, String.class);

		assertThat(built.type()).isEqualTo(MAP);
		assertThat(built.getPropertyName()).isEqualTo("prop");

		assertThat(built.<Bean> getKeyClass()).isEqualTo(Bean.class);

		assertThat(built.<String> getValueClass()).isEqualTo(String.class);
        assertThat(built.getMapCodec()).isSameAs(mapCodec);
	}

    @Test
    public void should_build_counter_meta() throws Exception {
        //Given
        CounterProperties counterProperties = mock(CounterProperties.class);

        //When
        PropertyMeta built = PropertyMetaBuilder.factory()
                .type(COUNTER)
                .propertyName("prop")
                .accessors(accessors)
                .objectMapper(objectMapper)
                .counterProperties(counterProperties)
                .build(Void.class, Counter.class);

        //Then
        assertThat(built.type()).isEqualTo(COUNTER);
        assertThat(built.getPropertyName()).isEqualTo("prop");

        assertThat(built.<Counter>getValueClass()).isEqualTo(Counter.class);
        assertThat(built.counterProperties).isSameAs(counterProperties);
    }
}
