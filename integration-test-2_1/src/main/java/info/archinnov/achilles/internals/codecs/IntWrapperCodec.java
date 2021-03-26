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

package info.archinnov.achilles.internals.codecs;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.internals.types.IntWrapper;
import info.archinnov.achilles.type.codec.Codec;

public class IntWrapperCodec implements Codec<IntWrapper, Integer> {
    @Override
    public Class<IntWrapper> sourceType() {
        return IntWrapper.class;
    }

    @Override
    public Class<Integer> targetType() {
        return Integer.class;
    }

    @Override
    public Integer encode(IntWrapper fromJava) throws AchillesTranscodingException {
        return fromJava.val;
    }

    @Override
    public IntWrapper decode(Integer fromCassandra) throws AchillesTranscodingException {
        return new IntWrapper(fromCassandra);
    }
}
