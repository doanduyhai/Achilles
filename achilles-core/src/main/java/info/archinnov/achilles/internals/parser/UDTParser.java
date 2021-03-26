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

import static info.archinnov.achilles.internals.apt.AptUtils.containsAnnotation;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;

import java.util.List;
import java.util.Optional;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeMirror;

import com.squareup.javapoet.*;

import info.archinnov.achilles.annotations.Frozen;
import info.archinnov.achilles.annotations.UDT;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.codegen.meta.UDTMetaCodeGen;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.FieldParser.UDTMetaSignature;
import info.archinnov.achilles.internals.parser.context.AccessorsExclusionContext;
import info.archinnov.achilles.internals.parser.context.FieldParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;

public class UDTParser extends AbstractBeanParser {

    private final UDTMetaCodeGen udtMetaCodeGen;

    public UDTParser(AptUtils aptUtils) {
        super(aptUtils);
        this.udtMetaCodeGen = new UDTMetaCodeGen(aptUtils);
    }

    public FieldMetaSignature parseUDT(AnnotationTree annotationTree, FieldParsingContext context, FieldParser fieldParser) {
        final TypeMirror typeMirror = annotationTree.getCurrentType();
        final TypeName udtTypeName = TypeName.get(typeMirror);
        final TypeName rawUdtTypeName = getRawType(udtTypeName);
        final TypeElement typeElement = aptUtils.asTypeElement(typeMirror);

        final GlobalParsingContext globalContext = context.entityContext.globalContext;
        validateUDT(globalContext, rawUdtTypeName, typeElement);

        if (!context.hasProcessedUDT(rawUdtTypeName)) {

            final List<AccessorsExclusionContext> accessorsExclusionContexts = prebuildAccessorsExclusion(typeElement, globalContext);
            final List<FieldMetaSignature> fieldMetaSignatures = parseFields(typeElement, fieldParser,
                    accessorsExclusionContexts, globalContext);

            final List<FieldMetaSignature> customConstructorFieldMetaSignatures =
                    parseAndValidateCustomConstructor(globalContext.beanValidator(),
                            rawUdtTypeName.toString(), typeElement, fieldMetaSignatures);

            TypeSpec udtClassPropertyCode = udtMetaCodeGen.buildUDTClassProperty(typeElement, context.entityContext,
                    fieldMetaSignatures, customConstructorFieldMetaSignatures);
            context.addUDTMeta(rawUdtTypeName, udtClassPropertyCode);
            final boolean isFrozen = containsAnnotation(annotationTree, Frozen.class);
            final UDTMetaSignature udtMetaSignature = new UDTMetaSignature(context.fieldName, context.quotedCqlColumn,
                    fieldMetaSignatures, customConstructorFieldMetaSignatures, isFrozen);
            globalContext.nestedTypesValidator().validateUDT(aptUtils, udtMetaSignature, context.fieldName, context.entityRawType);
            context.addUDTMetaSignature(rawUdtTypeName, udtMetaSignature);
        }

        Optional<UDTMetaSignature> udtMetaSignature = Optional.of(context.getUDTMetaSignature(rawUdtTypeName));

        TypeName udtClassMetaTypeName = ClassName.get(UDT_META_PACKAGE, typeElement.getSimpleName() + META_SUFFIX);
        CodeBlock typeCode = CodeBlock.builder().add("new $T<$T, $T, $T>($L, $T.class, $T.INSTANCE)",
                UDT_PROPERTY,
                context.entityRawType,
                udtClassMetaTypeName,
                rawUdtTypeName.box(),
                context.fieldInfoCode,
                rawUdtTypeName.box(),
                udtClassMetaTypeName)
                .build();
        final ParameterizedTypeName propertyType = genericType(UDT_PROPERTY, context.entityRawType, udtClassMetaTypeName, rawUdtTypeName);
        return new FieldMetaSignature(context, annotationTree.hasNext() ? annotationTree.next() : annotationTree,
                udtTypeName, JAVA_DRIVER_UDT_VALUE_TYPE, propertyType, typeCode, FieldParser.IndexMetaSignature.simpleType(udtTypeName),
                udtMetaSignature);
    }

    void validateUDT(GlobalParsingContext context, TypeName udtTypeName, TypeElement typeElement) {
        context.beanValidator().validateIsAConcreteClass(aptUtils, typeElement);
        final boolean isSupportedType = context.typeValidator().getAllowedTypes().contains(udtTypeName);
        aptUtils.validateFalse(isSupportedType,
                "Type '%s' cannot be annotated with '%s' because it is a supported type",
                udtTypeName, UDT.class.getCanonicalName());
        context.beanValidator().validateConstructor(aptUtils, udtTypeName, typeElement);
    }
}
