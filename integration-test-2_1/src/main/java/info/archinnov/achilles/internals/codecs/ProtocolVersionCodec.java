/*
 * Copyright (C) 2012-2017 DuyHai DOAN
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

import org.apache.commons.lang3.StringUtils;

import com.datastax.driver.core.ProtocolVersion;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.type.codec.Codec;

public class ProtocolVersionCodec implements Codec<ProtocolVersion, String> {

    @Override
    public Class<ProtocolVersion> sourceType() {
        return ProtocolVersion.class;
    }

    @Override
    public Class<String> targetType() {
        return String.class;
    }

    @Override
    public String encode(ProtocolVersion fromJava) throws AchillesTranscodingException {
        if (fromJava != null) {
            return fromJava.name();
        } else {
            return null;
        }
    }

    @Override
    public ProtocolVersion decode(String fromCassandra) throws AchillesTranscodingException {
        if (StringUtils.isEmpty(fromCassandra)) {
            return null;
        } else {
            return ProtocolVersion.valueOf(fromCassandra);
        }
    }
}
