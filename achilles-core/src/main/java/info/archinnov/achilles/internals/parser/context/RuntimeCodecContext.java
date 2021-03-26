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

package info.archinnov.achilles.internals.parser.context;

import java.util.Optional;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.type.codec.Codec;

public class RuntimeCodecContext extends CodecContext{

    public final Optional<String> codecName;

    public RuntimeCodecContext(TypeName sourceType, TypeName targetType, Optional<String> codecName) {
        super(TypeUtils.genericType(ClassName.get(Codec.class), sourceType, targetType), sourceType, targetType);
        this.codecName = codecName;
    }
}
