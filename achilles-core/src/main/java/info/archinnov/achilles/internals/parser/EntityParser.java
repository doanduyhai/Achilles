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

package info.archinnov.achilles.internals.parser;

import java.util.List;
import javax.lang.model.element.TypeElement;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen.EntityMetaSignature;
import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty.EntityType;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.context.AccessorsExclusionContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;


public class EntityParser extends AbstractBeanParser {

    private final FieldParser fieldParser;
    private final EntityMetaCodeGen entityMetaCodeGen;

    public EntityParser(AptUtils aptUtils) {
        super(aptUtils);
        this.fieldParser = new FieldParser(aptUtils);
        this.entityMetaCodeGen = new EntityMetaCodeGen(aptUtils);
    }

    public EntityMetaSignature parseEntity(TypeElement elm, GlobalParsingContext globalParsingContext) {

        final List<AccessorsExclusionContext> accessorsExclusionContexts = prebuildAccessorsExclusion(elm, globalParsingContext);
        final List<FieldMetaSignature> fieldMetaSignatures = parseFields(elm, fieldParser, accessorsExclusionContexts, globalParsingContext);
        final List<FieldMetaSignature> customConstructorFieldMetaSignatures =
                parseAndValidateCustomConstructor(globalParsingContext.beanValidator(),
                        elm.getSimpleName().toString(), elm, fieldMetaSignatures);
        return entityMetaCodeGen.buildEntityMeta(EntityType.TABLE, elm, globalParsingContext,
                fieldMetaSignatures, customConstructorFieldMetaSignatures);
    }

    public EntityMetaSignature parseView(TypeElement elm, GlobalParsingContext globalParsingContext) {

        final List<AccessorsExclusionContext> accessorsExclusionContexts = prebuildAccessorsExclusion(elm, globalParsingContext);
        final List<FieldMetaSignature> fieldMetaSignatures = parseFields(elm, fieldParser, accessorsExclusionContexts, globalParsingContext);
        final List<FieldMetaSignature> customConstructorFieldMetaSignatures =
                parseAndValidateCustomConstructor(globalParsingContext.beanValidator(),
                        elm.getSimpleName().toString(), elm, fieldMetaSignatures);
        return entityMetaCodeGen.buildEntityMeta(EntityType.VIEW, elm, globalParsingContext,
                fieldMetaSignatures, customConstructorFieldMetaSignatures);
    }


}
