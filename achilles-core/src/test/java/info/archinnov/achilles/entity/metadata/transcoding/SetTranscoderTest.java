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

import static info.archinnov.achilles.entity.metadata.PropertyType.SIMPLE;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.test.builders.PropertyMetaTestBuilder;

import java.util.Set;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.google.common.collect.Sets;

public class SetTranscoderTest {

	private SetTranscoder transcoder = new SetTranscoder(mock(ObjectMapper.class));

	@Test
	public void should_encode() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).type(SIMPLE).build();

		Set actual = transcoder.encode(pm, Sets.newHashSet("value"));

		assertThat(actual).containsExactly("value");
	}

	@Test
	public void should_decode() throws Exception {
		PropertyMeta pm = PropertyMetaTestBuilder.valueClass(String.class).type(SIMPLE).build();

		Set actual = transcoder.decode(pm, Sets.newHashSet("value"));

		assertThat(actual).containsExactly("value");
	}
}
