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
import static info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy.inferNamingStrategy;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.lang.model.element.TypeElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.context.FunctionSignature.FunctionParamSignature;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.type.tuples.*;

public class FunctionParamParser {

    private final AptUtils aptUtils;
    private final CodecFactory codecFactory;
    private final UDTParser udtParser;

    public FunctionParamParser(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
        this.codecFactory = new CodecFactory(aptUtils);
        this.udtParser = new UDTParser(aptUtils);
    }

    public FunctionParamSignature parseParam(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType,
                                             String methodName, String paramName) {
        final TypeMirror currentTypeMirror = annotationTree.getCurrentType();
        final TypeName sourceType = TypeName.get(currentTypeMirror).box();
        boolean isUDT = currentTypeMirror.getKind() == TypeKind.DECLARED
                && aptUtils.getAnnotationOnClass(currentTypeMirror, UDT.class).isPresent();

        if (containsAnnotation(annotationTree, JSON.class)) {
            return new FunctionParamSignature(paramName, sourceType, STRING, "text");
        } else if (containsAnnotation(annotationTree, Computed.class)) {
            throw new AchillesBeanMappingException(format("Cannot have @Computed annotation on param '%s' of method '%s''", paramName, methodName));
        } else if (aptUtils.isAssignableFrom(Tuple1.class, currentTypeMirror)) {
            return parseTuple1(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple2.class, currentTypeMirror)) {
            return parseTuple2(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple3.class, currentTypeMirror)) {
            return parseTuple3(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple4.class, currentTypeMirror)) {
            return parseTuple4(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple5.class, currentTypeMirror)) {
            return parseTuple5(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple6.class, currentTypeMirror)) {
            return parseTuple6(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple7.class, currentTypeMirror)) {
            return parseTuple7(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple8.class, currentTypeMirror)) {
            return parseTuple8(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple9.class, currentTypeMirror)) {
            return parseTuple9(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Tuple10.class, currentTypeMirror)) {
            return parseTuple10(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(List.class, currentTypeMirror)) {
            return parseList(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Set.class, currentTypeMirror)) {
            return parseSet(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(Map.class, currentTypeMirror)) {
            return parseMap(context, annotationTree, parentType, methodName, paramName);
        } else if (aptUtils.isAssignableFrom(java.util.Optional.class, currentTypeMirror)) {
            return parseOptional(context, annotationTree, parentType, methodName, paramName);
        } else if (isUDT) {
            return parseUDT(context, annotationTree, paramName);
        } else {
            return parseSimpleType(context, annotationTree, parentType, methodName, paramName);
        }
    }

    private FunctionParamSignature parseSimpleType(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType,
                                                   String methodName, String paramName) {
        final TypeMirror typeMirror = annotationTree.getCurrentType();
        TypeName sourceType = TypeName.get(typeMirror);
        TypeName typeNameFromRegistry = buildOrGetCodecFromRegistry(context, annotationTree, parentType, sourceType,
                methodName, paramName);

        final TypeName rawTargetType = getRawType(typeNameFromRegistry);

        aptUtils.validateTrue(context.typeValidator().getAllowedTypes().contains(rawTargetType),
                "Impossible to parse type '%s' from param '%s' of method '%s' on class '%s'. It should be a supported type",
                rawTargetType.toString(), paramName, methodName);

        if (containsAnnotation(annotationTree.getAnnotations().keySet(), Counter.class)) {
            aptUtils.validateTrue(rawTargetType.box().equals(TypeName.LONG.box()),
                    "Param '%s' of method '%s' on class '%s' annotated with @Counter should be of type Long/long",
                    paramName, methodName);
        }

        if (containsAnnotation(annotationTree, TimeUUID.class)) {
            return new FunctionParamSignature(paramName, UUID, UUID, "timeuuid");
        } else if (containsAnnotation(annotationTree, Counter.class)) {
            return new FunctionParamSignature(paramName, OBJECT_LONG, OBJECT_LONG, "counter");
        } else {
            return new FunctionParamSignature(paramName, sourceType, rawTargetType.box(), DRIVER_TYPES_FUNCTION_PARAM_MAPPING.get(rawTargetType));
        }
    }

    protected FunctionParamSignature parseTuple1(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {
        final FunctionParamSignature parsingResult = parseParam(context, annotationTree.next(), parentType, methodName, paramName);
        return FunctionParamSignature.tupleType(paramName, TUPLE1, parsingResult);
    }

    protected FunctionParamSignature parseTuple2(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {
        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE2, parsingResult1, parsingResult2);
    }

    protected FunctionParamSignature parseTuple3(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {
        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();
        final AnnotationTree annotationTree3 = annotationTree2.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult3 = parseParam(context, annotationTree3, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE3, parsingResult1, parsingResult2, parsingResult3);
    }

    protected FunctionParamSignature parseTuple4(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {

        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();
        final AnnotationTree annotationTree3 = annotationTree2.next();
        final AnnotationTree annotationTree4 = annotationTree3.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult3 = parseParam(context, annotationTree3, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult4 = parseParam(context, annotationTree4, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE4, parsingResult1, parsingResult2, parsingResult3, parsingResult4);
    }

    protected FunctionParamSignature parseTuple5(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {

        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();
        final AnnotationTree annotationTree3 = annotationTree2.next();
        final AnnotationTree annotationTree4 = annotationTree3.next();
        final AnnotationTree annotationTree5 = annotationTree4.next();


        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult3 = parseParam(context, annotationTree3, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult4 = parseParam(context, annotationTree4, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult5 = parseParam(context, annotationTree5, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE5, parsingResult1, parsingResult2, parsingResult3, parsingResult4, parsingResult5);
    }

    protected FunctionParamSignature parseTuple6(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {

        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();
        final AnnotationTree annotationTree3 = annotationTree2.next();
        final AnnotationTree annotationTree4 = annotationTree3.next();
        final AnnotationTree annotationTree5 = annotationTree4.next();
        final AnnotationTree annotationTree6 = annotationTree5.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult3 = parseParam(context, annotationTree3, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult4 = parseParam(context, annotationTree4, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult5 = parseParam(context, annotationTree5, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult6 = parseParam(context, annotationTree6, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE6, parsingResult1, parsingResult2, parsingResult3, parsingResult4, parsingResult5, parsingResult6);
    }

    protected FunctionParamSignature parseTuple7(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {

        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();
        final AnnotationTree annotationTree3 = annotationTree2.next();
        final AnnotationTree annotationTree4 = annotationTree3.next();
        final AnnotationTree annotationTree5 = annotationTree4.next();
        final AnnotationTree annotationTree6 = annotationTree5.next();
        final AnnotationTree annotationTree7 = annotationTree6.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult3 = parseParam(context, annotationTree3, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult4 = parseParam(context, annotationTree4, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult5 = parseParam(context, annotationTree5, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult6 = parseParam(context, annotationTree6, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult7 = parseParam(context, annotationTree7, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE7, parsingResult1, parsingResult2, parsingResult3, parsingResult4, parsingResult5,
                parsingResult6, parsingResult7);
    }

    protected FunctionParamSignature parseTuple8(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {

        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();
        final AnnotationTree annotationTree3 = annotationTree2.next();
        final AnnotationTree annotationTree4 = annotationTree3.next();
        final AnnotationTree annotationTree5 = annotationTree4.next();
        final AnnotationTree annotationTree6 = annotationTree5.next();
        final AnnotationTree annotationTree7 = annotationTree6.next();
        final AnnotationTree annotationTree8 = annotationTree7.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult3 = parseParam(context, annotationTree3, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult4 = parseParam(context, annotationTree4, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult5 = parseParam(context, annotationTree5, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult6 = parseParam(context, annotationTree6, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult7 = parseParam(context, annotationTree7, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult8 = parseParam(context, annotationTree8, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE8, parsingResult1, parsingResult2, parsingResult3, parsingResult4, parsingResult5,
                parsingResult6, parsingResult7, parsingResult8);
    }

    protected FunctionParamSignature parseTuple9(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {

        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();
        final AnnotationTree annotationTree3 = annotationTree2.next();
        final AnnotationTree annotationTree4 = annotationTree3.next();
        final AnnotationTree annotationTree5 = annotationTree4.next();
        final AnnotationTree annotationTree6 = annotationTree5.next();
        final AnnotationTree annotationTree7 = annotationTree6.next();
        final AnnotationTree annotationTree8 = annotationTree7.next();
        final AnnotationTree annotationTree9 = annotationTree8.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult3 = parseParam(context, annotationTree3, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult4 = parseParam(context, annotationTree4, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult5 = parseParam(context, annotationTree5, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult6 = parseParam(context, annotationTree6, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult7 = parseParam(context, annotationTree7, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult8 = parseParam(context, annotationTree8, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult9 = parseParam(context, annotationTree9, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE9, parsingResult1, parsingResult2, parsingResult3, parsingResult4, parsingResult5,
                parsingResult6, parsingResult7, parsingResult8, parsingResult9);
    }

    protected FunctionParamSignature parseTuple10(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {

        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();
        final AnnotationTree annotationTree3 = annotationTree2.next();
        final AnnotationTree annotationTree4 = annotationTree3.next();
        final AnnotationTree annotationTree5 = annotationTree4.next();
        final AnnotationTree annotationTree6 = annotationTree5.next();
        final AnnotationTree annotationTree7 = annotationTree6.next();
        final AnnotationTree annotationTree8 = annotationTree7.next();
        final AnnotationTree annotationTree9 = annotationTree8.next();
        final AnnotationTree annotationTree10 = annotationTree9.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult3 = parseParam(context, annotationTree3, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult4 = parseParam(context, annotationTree4, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult5 = parseParam(context, annotationTree5, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult6 = parseParam(context, annotationTree6, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult7 = parseParam(context, annotationTree7, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult8 = parseParam(context, annotationTree8, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult9 = parseParam(context, annotationTree9, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult10 = parseParam(context, annotationTree10, parentType, methodName, paramName);

        return FunctionParamSignature.tupleType(paramName, TUPLE10, parsingResult1, parsingResult2, parsingResult3, parsingResult4, parsingResult5,
                parsingResult6, parsingResult7, parsingResult8, parsingResult9, parsingResult10);
    }

    protected FunctionParamSignature parseList(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {
        final boolean isFrozen = AptUtils.containsAnnotation(annotationTree, Frozen.class);
        final FunctionParamSignature parsingResult = parseParam(context, annotationTree.next(), parentType, methodName, paramName);
        return FunctionParamSignature.listType(paramName, parsingResult, isFrozen);
    }

    protected FunctionParamSignature parseSet(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {
        final boolean isFrozen = AptUtils.containsAnnotation(annotationTree, Frozen.class);
        final FunctionParamSignature parsingResult = parseParam(context, annotationTree.next(), parentType, methodName, paramName);
        return FunctionParamSignature.setType(paramName, parsingResult, isFrozen);
    }

    protected FunctionParamSignature parseMap(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {
        final boolean isFrozen = AptUtils.containsAnnotation(annotationTree, Frozen.class);
        final AnnotationTree annotationTree1 = annotationTree.next();
        final AnnotationTree annotationTree2 = annotationTree1.next();

        final FunctionParamSignature parsingResult1 = parseParam(context, annotationTree1, parentType, methodName, paramName);
        final FunctionParamSignature parsingResult2 = parseParam(context, annotationTree2, parentType, methodName, paramName);

        return FunctionParamSignature.mapType(paramName, parsingResult1, parsingResult2, isFrozen);
    }

    protected FunctionParamSignature parseOptional(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, String methodName, String paramName) {
        final FunctionParamSignature parsingResult = parseParam(context, annotationTree.next(), parentType, methodName, paramName);
        return FunctionParamSignature.optionalType(paramName, parsingResult);
    }

    protected FunctionParamSignature parseUDT(GlobalParsingContext context, AnnotationTree annotationTree, String paramName) {
        final boolean isFrozen = AptUtils.containsAnnotation(annotationTree, Frozen.class);

        final TypeMirror typeMirror = annotationTree.getCurrentType();
        final TypeName udtTypeName = TypeName.get(typeMirror);
        final TypeElement typeElement = aptUtils.asTypeElement(typeMirror);
        udtParser.validateUDT(context, udtTypeName, typeElement);

        final Optional<Strategy> strategy = aptUtils.getAnnotationOnClass(typeElement, Strategy.class);
        final UDT udt = aptUtils.getAnnotationOnClass(typeElement, UDT.class).get();
        final String udtName = isBlank(udt.name())
                ? inferNamingStrategy(strategy, context.namingStrategy).apply(typeElement.getSimpleName().toString())
                : udt.name();

        if (isFrozen) {
            return new FunctionParamSignature(paramName, udtTypeName, JAVA_DRIVER_UDT_VALUE_TYPE, "frozen<" + udtName + ">");
        } else {
            return new FunctionParamSignature(paramName, udtTypeName, JAVA_DRIVER_UDT_VALUE_TYPE, udtName);
        }

    }

    private TypeName buildOrGetCodecFromRegistry(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType,
                                                 TypeName sourceType, String methodName, String paramName) {
        return codecFactory.determineTargetCQLType(context, annotationTree, parentType, sourceType, methodName, paramName,
                Optional.ofNullable(context.getCodecFor(sourceType))) ;
    }
}