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

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.type.codec.Codec;

public class FallThroughCodec<TYPE> implements Codec<TYPE, TYPE> {

    private final Class<TYPE> type;

    public FallThroughCodec(Class<TYPE> type) {
        this.type = type;
    }

    public static <T> FallThroughCodec<T> create(Class<T> type) {
        return new FallThroughCodec<>(type);
    }

    @Override
    public Class<TYPE> sourceType() {
        return type;
    }

    @Override
    public Class<TYPE> targetType() {
        return type;
    }

    @Override
    public TYPE encode(TYPE fromJava) throws AchillesTranscodingException {
        return fromJava;
    }

    @Override
    public TYPE decode(TYPE fromCassandra) throws AchillesTranscodingException {
        return fromCassandra;
    }
}
