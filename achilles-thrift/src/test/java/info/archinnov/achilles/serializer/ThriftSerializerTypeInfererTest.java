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
package info.archinnov.achilles.serializer;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import static org.fest.assertions.api.Assertions.assertThat;
import info.archinnov.achilles.test.integration.entity.User;

import java.io.Serializable;

import org.junit.Test;

public class ThriftSerializerTypeInfererTest {

	@Test
	public void should_return_string_serializer_for_non_native_class()
			throws Exception {
		assertThat(
				ThriftSerializerTypeInferer.<String> getSerializer(User.class))
				.isEqualTo(ThriftSerializerUtils.STRING_SRZ);
	}

	@Test
	public void should_return_string_serializer_for_supported_class()
			throws Exception {
		assertThat(ThriftSerializerTypeInferer.<Long> getSerializer(Long.class))
				.isEqualTo(LONG_SRZ);
	}

	@Test
	public void should_return_string_serializer_for_serializable_type()
			throws Exception {
		assertThat(
				ThriftSerializerTypeInferer.<String> getSerializer(Pojo.class))
				.isEqualTo(STRING_SRZ);
	}

	@Test
	public void should_return_string_serializer_for_object_instance()
			throws Exception {
		assertThat(ThriftSerializerTypeInferer.<Long> getSerializer(10L))
				.isEqualTo(LONG_SRZ);
	}

	@Test
	public void should_return_null_for_null_class_param() throws Exception {
		assertThat(ThriftSerializerTypeInferer.<Long> getSerializer(null))
				.isNull();
	}

	@Test
	public void should_return_null_for_null_object_param() throws Exception {
		assertThat(
				ThriftSerializerTypeInferer.<Long> getSerializer((Object) null))
				.isNull();
	}

	public static class Pojo implements Serializable {
		private static final long serialVersionUID = 1L;

	}
}
