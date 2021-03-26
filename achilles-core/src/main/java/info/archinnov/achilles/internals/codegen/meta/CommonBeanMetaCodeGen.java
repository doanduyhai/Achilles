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

package info.archinnov.achilles.internals.codegen.meta;

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy.getNamingStrategy;

import java.util.Optional;
import javax.lang.model.element.Modifier;

import com.squareup.javapoet.MethodSpec;

import info.archinnov.achilles.annotations.Strategy;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;

public interface CommonBeanMetaCodeGen {

    default MethodSpec buildGetStaticNamingStrategy(Optional<Strategy> strategy) {
        final MethodSpec.Builder builder = MethodSpec.methodBuilder("getStaticNamingStrategy")
                .addAnnotation(Override.class)
                .addModifiers(Modifier.PROTECTED)
                .returns(genericType(OPTIONAL, NAMING_STRATEGY));

        if (strategy.isPresent()) {
            final InternalNamingStrategy internalStrategy = getNamingStrategy(strategy.get().naming());
            return builder
                    .addStatement("return $T.of(new $L())", OPTIONAL, internalStrategy.FQCN())
                    .build();
        } else {
            return emptyOption(builder);
        }
    }

    default MethodSpec emptyOption(MethodSpec.Builder builder) {
        return builder
                .addStatement("return $T.empty()", OPTIONAL)
                .build();
    }
}
