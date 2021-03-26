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

import java.util.Collections;
import java.util.List;
import javax.lang.model.element.TypeElement;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.parser.CodecFactory;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;

public class EntityParsingContext {
    public final TypeElement entityTypeElement;
    public final TypeName entityType;
    public final InternalNamingStrategy namingStrategy;
    public final GlobalParsingContext globalContext;
    public final String className;
    public final List<AccessorsExclusionContext> accessorsExclusionContexts;

    public EntityParsingContext(TypeElement elm, TypeName entityType, InternalNamingStrategy namingStrategy,
                                GlobalParsingContext globalContext) {
        this.entityTypeElement = elm;
        this.entityType = entityType;
        this.accessorsExclusionContexts = Collections.emptyList();
        this.globalContext = globalContext;
        this.namingStrategy = namingStrategy;
        this.className = entityType.toString();
    }

    public EntityParsingContext(TypeElement elm, TypeName entityType, InternalNamingStrategy namingStrategy,
                                List<AccessorsExclusionContext> accessorsExclusionContexts,
                                GlobalParsingContext globalContext) {
        this.entityTypeElement = elm;
        this.entityType = entityType;
        this.accessorsExclusionContexts = accessorsExclusionContexts;
        this.globalContext = globalContext;
        this.namingStrategy = namingStrategy;
        this.className = entityType.toString();
    }

    public CodecFactory.CodecInfo getCodecFor(TypeName typeName) {
        return globalContext.getCodecFor(typeName);
    }
}
