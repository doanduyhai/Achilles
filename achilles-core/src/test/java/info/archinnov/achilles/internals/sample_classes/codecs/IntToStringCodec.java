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

package info.archinnov.achilles.internals.sample_classes.codecs;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.type.codec.Codec;

public class IntToStringCodec implements Codec<Integer, String> {

    @Override
    public Class<String> targetType() {
        return String.class;
    }

    @Override
    public Class<Integer> sourceType() {
        return Integer.class;
    }

    @Override
    public Integer decode(String fromJava) throws AchillesTranscodingException {
        return Integer.parseInt(fromJava);
    }

    @Override
    public String encode(Integer fromCassandra) throws AchillesTranscodingException {
        return fromCassandra.toString();
    }
}