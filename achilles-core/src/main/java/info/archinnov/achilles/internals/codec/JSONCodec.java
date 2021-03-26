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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.type.codec.Codec;

public class JSONCodec<TYPE> implements Codec<TYPE, String> {

    public static final TypeFactory TYPE_FACTORY_INSTANCE = TypeFactory.defaultInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(JSONCodec.class);
    private final Class<?> sourceType;
    private final JavaType exactType;

    private ObjectMapper objectMapper;

    public JSONCodec(Class<?> sourceType, JavaType exactType) {
        this.sourceType = sourceType;
        this.exactType = exactType;
    }

    public void setObjectMapper(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Class<TYPE> sourceType() {
        return (Class<TYPE>) sourceType;
    }

    @Override
    public Class<String> targetType() {
        return String.class;
    }

    @Override
    public String encode(TYPE fromJava) throws AchillesTranscodingException {
        if (fromJava == null) return null;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Encoding object %s to JSON", fromJava));
        }
        try {
            return objectMapper.writeValueAsString(fromJava);
        } catch (JsonProcessingException e) {
            throw new AchillesTranscodingException(e);
        }
    }

    @Override
    public TYPE decode(String fromCassandra) throws AchillesTranscodingException {
        if (fromCassandra == null) return null;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Decoding object type %s from JSON %s", exactType, fromCassandra));
        }
        try {
            return objectMapper.readValue(fromCassandra, exactType);
        } catch (IOException e) {
            throw new AchillesTranscodingException(e);
        }
    }
}
