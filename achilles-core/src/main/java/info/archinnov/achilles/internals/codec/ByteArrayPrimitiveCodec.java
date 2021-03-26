/*
 * Copyright (C) 2012-2021 DuyHai DOAN
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

package info.archinnov.achilles.internals.codec;

import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.utils.Bytes;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.type.codec.Codec;

public class ByteArrayPrimitiveCodec implements Codec<byte[], ByteBuffer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ByteArrayPrimitiveCodec.class);

    @Override
    public Class<byte[]> sourceType() {
        return byte[].class;
    }

    @Override
    public Class<ByteBuffer> targetType() {
        return ByteBuffer.class;
    }

    @Override
    public ByteBuffer encode(byte[] fromJava) throws AchillesTranscodingException {
        if (fromJava == null) return null;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Encoding byte[] '%s' to ByteBuffer", Bytes.toHexString(ByteBuffer.wrap(fromJava).duplicate().array())));
        }
        return ByteBuffer.wrap(fromJava);
    }

    @Override
    public byte[] decode(ByteBuffer fromCassandra) {
        if (fromCassandra == null) return null;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Decoding ByteBuffer '%s' to byte[]", Bytes.toHexString(fromCassandra.duplicate())));
        }
        return readByteBuffer(fromCassandra);
    }

    private byte[] readByteBuffer(Object fromCassandra) {
        ByteBuffer byteBuffer = (ByteBuffer) fromCassandra;
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return bytes;
    }
}
