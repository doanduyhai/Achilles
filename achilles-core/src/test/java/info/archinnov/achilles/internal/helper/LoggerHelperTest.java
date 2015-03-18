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
package info.archinnov.achilles.internal.helper;

import static org.fest.assertions.api.Assertions.assertThat;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.Lists;
import info.archinnov.achilles.internal.utils.UUIDGen;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

public class LoggerHelperTest {

    private final byte[] long_blob = byteArrayOf("abcdefghijklmnoprstuvwxyz1234567890");
    private final byte[] short_blob = byteArrayOf("12345678");
    private final byte[] uuid_blob = byteArrayOf(UUIDGen.getTimeUUID());

    private byte[] byteArrayOf(UUID timeUUID) {
        return ByteBuffer.wrap(new byte[16])
                .putLong(timeUUID.getMostSignificantBits())
                .putLong(timeUUID.getLeastSignificantBits())
                .array();
    }


    @Test
	public void should_transform_class_list_to_canonical_class_name_list() throws Exception {
		List<Class<?>> classes = new ArrayList<>();
		classes.add(Long.class);

		assertThat(Lists.transform(classes, LoggerHelper.fqcnToStringFn)).contains(Long.class.getCanonicalName());
	}

	@Test
	public void should_transform_field_list_to_field_name_list() throws Exception {
		Field field = CompleteBean.class.getDeclaredField("id");
		List<Field> fields = new ArrayList<>();
		fields.add(field);

		assertThat(Lists.transform(fields, LoggerHelper.fieldToStringFn)).contains("id");
	}

    @Test
    public void should_transform_long_byte_array_to_truncated_hex_string() throws Exception {
        List<Object> objects = LoggerHelper.replaceByteBuffersByHexString(new Object[] { long_blob });

        assertThat(objects.get(0)).isEqualTo(Bytes.toHexString(long_blob).substring(0, 32 + 2) + "...");
    }

    @Test
    public void should_transform_long_ByteBuffer_to_truncated_hex_string() throws Exception {
        List<Object> objects = LoggerHelper.replaceByteBuffersByHexString(ByteBuffer.wrap(long_blob));

        assertThat(objects.get(0)).isEqualTo(Bytes.toHexString(long_blob).substring(0, 32 + 2) + "...");
    }

    @Test
    public void should_transform_short_byte_array_to_non_truncated_hex_string() throws Exception {
        List<Object> objects = LoggerHelper.replaceByteBuffersByHexString(new Object[] { short_blob });

        assertThat(objects.get(0)).isEqualTo(Bytes.toHexString(short_blob));
    }

    @Test
    public void should_transform_short_ByteBuffer_to_non_truncated_hex_string() throws Exception {
        List<Object> objects = LoggerHelper.replaceByteBuffersByHexString(ByteBuffer.wrap(short_blob));

        assertThat(objects.get(0)).isEqualTo(Bytes.toHexString(short_blob));
    }

    @Test
    public void should_transform_short_byte_array_of_a_UUID_to_non_truncated_hex_string() throws Exception {
        List<Object> objects = LoggerHelper.replaceByteBuffersByHexString(new Object[] { uuid_blob });

        assertThat(objects.get(0)).isEqualTo(Bytes.toHexString(uuid_blob));
    }

    @Test
    public void should_transform_short_ByteBuffer_of_a_UUID_to_non_truncated_hex_string() throws Exception {
        List<Object> objects = LoggerHelper.replaceByteBuffersByHexString(ByteBuffer.wrap(uuid_blob));

        assertThat(objects.get(0)).isEqualTo(Bytes.toHexString(uuid_blob));
    }

    private byte[] byteArrayOf(String string) {
        try {
            return string.getBytes("UTF-8");
        } catch (UnsupportedEncodingException charset_bad_luck) {
            throw new RuntimeException("JVM failure ?", charset_bad_luck);
        }
    }

}
