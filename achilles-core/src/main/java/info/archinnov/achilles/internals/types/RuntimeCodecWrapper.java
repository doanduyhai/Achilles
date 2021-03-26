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

package info.archinnov.achilles.internals.types;

import static java.lang.String.format;

import java.util.Map;
import java.util.Optional;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.codec.CodecSignature;

public class RuntimeCodecWrapper<FROM,TO> implements Codec<FROM, TO> {

    public final Class<FROM> sourceType;
    public final Class<TO> targetType;
    public final Optional<String> codecName;
    private Codec<FROM, TO> delegate;

    public RuntimeCodecWrapper(Class<FROM> sourceType, Class<TO> targetType, Optional<String> codecName) {
        this.sourceType = sourceType;
        this.targetType = targetType;
        this.codecName = codecName;
    }

    @Override
    public Class<FROM> sourceType() {
        return sourceType;
    }

    @Override
    public Class<TO> targetType() {
        return targetType;
    }

    @Override
    public TO encode(FROM fromJava) throws AchillesTranscodingException {
        return delegate.encode(fromJava);
    }

    @Override
    public FROM decode(TO fromCassandra) throws AchillesTranscodingException {
        return delegate.decode(fromCassandra);
    }

    public void inject(Map<CodecSignature<?,?>, Codec<?,?>> runtimeCodecRegistry) {
        final CodecSignature<FROM, TO> mySignature = new CodecSignature<>(sourceType, targetType, codecName);
        if (runtimeCodecRegistry.containsKey(mySignature)) {
            this.delegate = (Codec<FROM,TO>)runtimeCodecRegistry.get(mySignature);
        } else {
            throw new AchillesTranscodingException(
                    format("Cannot find runtime codec for %s. " +
                            "Did you forget to register it with the ManagerFactoryBuilder ?", mySignature));
        }
    }
}
