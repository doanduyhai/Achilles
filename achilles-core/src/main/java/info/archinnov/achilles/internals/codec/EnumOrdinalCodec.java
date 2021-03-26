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

import static java.lang.String.format;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.type.codec.Codec;

public class EnumOrdinalCodec<ENUM> implements Codec<ENUM, Integer> {

    private static final Logger LOGGER = LoggerFactory.getLogger(EnumOrdinalCodec.class);

    private final List<ENUM> enumValues;
    private final Class<ENUM> sourceType;

    public EnumOrdinalCodec(List<ENUM> enumValues, Class<ENUM> sourceType) {
        this.enumValues = enumValues;
        this.sourceType = sourceType;
    }

    public static <TYPE> EnumOrdinalCodec<TYPE> create(List<TYPE> enumValues, Class<TYPE> sourceType) {
        return new EnumOrdinalCodec<>(enumValues, sourceType);
    }

    @Override
    public Class<ENUM> sourceType() {
        return sourceType;
    }

    @Override
    public Class<Integer> targetType() {
        return Integer.class;
    }

    @Override
    public Integer encode(ENUM fromJava) {
        if (fromJava == null) return null;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Encoding enum %s to int", fromJava));
        }
        if (!fromJava.getClass().isEnum()) {
            throw new AchillesTranscodingException(format("Object '%s' to be encoded should be an enum", fromJava));
        }
        for (int i = 0; i < enumValues.size(); i++) {
            if (enumValues.get(i) == fromJava) {
                return i;
            }
        }
        throw new AchillesTranscodingException(format("Cannot find matching enum values for '%s' from possible enum constants '%s' ", fromJava, enumValues));
    }

    @Override
    public ENUM decode(Integer fromCassandra) {
        if (fromCassandra == null) return null;
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(String.format("Decoding enum type %s from int %s", sourceType.getCanonicalName(), fromCassandra));
        }
        if (fromCassandra > enumValues.size() - 1 || fromCassandra < 0) {
            throw new AchillesTranscodingException(format("Cannot find matching enum values for '%s' from possible enum constants '%s' ", fromCassandra, enumValues));
        }
        return enumValues.get(fromCassandra);
    }
}
