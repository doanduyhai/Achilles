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

import static com.google.auto.common.MoreTypes.asDeclared;
import static info.archinnov.achilles.internals.apt.AptUtils.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.stream.Collectors;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import org.apache.commons.lang3.ArrayUtils;

import com.squareup.javapoet.*;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.annotations.Enumerated.Encoding;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.context.CodecContext;
import info.archinnov.achilles.internals.parser.context.FieldParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.parser.context.RuntimeCodecContext;
import info.archinnov.achilles.internals.parser.validator.TypeValidator;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.Tuple2;


public class CodecFactory {

    private final AptUtils aptUtils;


    public CodecFactory(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
    }

    public static CodecContext buildCodecContext(AptUtils aptUtils, AnnotationMirror codecFromType) {
        Optional<Class<Codec>> codecClassO = getElementValueClass(codecFromType, "value", false);
        if (codecClassO.isPresent()) {
            Class<Codec> codecClass = codecClassO.get();
            List<Type> genericTypes = Arrays.asList(codecClass.getGenericInterfaces());

            final List<TypeName> codecTypes = genericTypes
                    .stream()
                    .filter(x -> x instanceof ParameterizedType)
                    .map(x -> (ParameterizedType) x)
                    .filter(x -> x.getRawType().getTypeName().equals(info.archinnov.achilles.type.codec.Codec.class.getCanonicalName()))
                    .flatMap(x -> Arrays.asList(x.getActualTypeArguments()).stream())
                    .map(TypeName::get)
                    .collect(Collectors.toList());
            aptUtils.validateTrue(codecTypes.size() == 2, "Codec class '%s' should have 2 parameters: Codec<FROM, TO>", codecClass);
            return new CodecContext(ClassName.get(codecClass), codecTypes.get(0), codecTypes.get(1));
        } else {
            return buildCodecContext(aptUtils, getElementValueClassName(codecFromType, "value", false).toString());
        }
    }

    public static CodecContext buildCodecContext(AptUtils aptUtils, String codecClassName) {

        final TypeMirror codecInterfaceType = aptUtils.erasure(aptUtils.elementUtils.getTypeElement(info.archinnov.achilles.type.codec.Codec.class.getCanonicalName()).asType());
        final Optional<? extends TypeMirror> foundCodecInterface = aptUtils.elementUtils.getTypeElement(codecClassName)
                .getInterfaces()
                .stream()
                .filter(x -> aptUtils.typeUtils.isSameType(aptUtils.erasure(x), codecInterfaceType))
                .findFirst();

        aptUtils.validateTrue(foundCodecInterface.isPresent(), "Codec class '%s' should implement the Codec<FROM, TO> interface", codecClassName);

        final TypeMirror typeMirror = foundCodecInterface.get();

        final List<TypeName> codecTypes = asDeclared(typeMirror)
                .getTypeArguments()
                .stream()
                .map(TypeName::get)
                .collect(toList());
        aptUtils.validateTrue(codecTypes.size() == 2, "Codec class '%s' should have 2 parameters: Codec<FROM, TO>", codecInterfaceType);

        final TypeMirror codecType = aptUtils.erasure(aptUtils.elementUtils.getTypeElement(codecClassName));

        return new CodecContext(TypeName.get(codecType), codecTypes.get(0), codecTypes.get(1));
    }

    public static RuntimeCodecContext buildRuntimeCodecContext(TypeMirror currentType, AnnotationMirror annotation) {
        final Class<?> targetType = getElementValueClass(annotation, "cqlClass", false).get();
        final Optional<String> codecName = ofNullable(getElementValue(annotation, "codecName", String.class, false));
        return new RuntimeCodecContext(TypeName.get(currentType), TypeName.get(targetType), codecName);
    }

    public CodecInfo createCodec(TypeName sourceType, AnnotationTree annotationTree, FieldParsingContext context, Optional<CodecInfo> codecFromRegistry) {
        final String fieldName = context.fieldName;
        final String className = context.className;

        TypeName targetType = sourceType;
        TypeMirror typeMirror = annotationTree.getCurrentType();

        final Optional<TypedMap> jsonTransform = extractTypedMap(annotationTree, JSON.class);
        final Optional<TypedMap> enumerated = extractTypedMap(annotationTree, Enumerated.class);
        final Optional<TypedMap> codecFromType = extractTypedMap(annotationTree, Codec.class);
        final Optional<TypedMap> runtimeCodec = extractTypedMap(annotationTree, RuntimeCodec.class);
        final Optional<TypedMap> computed = extractTypedMap(annotationTree, Computed.class);
        final Optional<TypeName> computedCQLClass = computed
                .map(x -> x.<Class<?>>getTyped("cqlClass"))
                .map(ClassName::get);
        final boolean isCounter = extractTypedMap(annotationTree, Counter.class).isPresent();

        CodeBlock codec;

        if (jsonTransform.isPresent()) {
            codec = CodeBlock.builder().add("new $T<>($T.class, $L)", JSON_CODEC, getRawType(sourceType).box(), buildJavaTypeForJackson(sourceType)).build();
            targetType = ClassName.get(String.class);
            return new CodecInfo(codec, sourceType, targetType);
        } else if (codecFromType.isPresent()) {
            final Tuple2<TypeName, CodeBlock> tuple2 = codecCodeGen(context, codecFromType.get(), sourceType, computedCQLClass, isCounter);
            targetType = tuple2._1();
            codec = tuple2._2();
            return new CodecInfo(codec, sourceType, targetType);
        } else if(runtimeCodec.isPresent()) {
            final Tuple2<TypeName, CodeBlock> tuple2 = runtimeCodecCodeGen(context, runtimeCodec.get(), computedCQLClass, isCounter);
            targetType = tuple2._1();
            codec = tuple2._2();
            return new CodecInfo(codec, sourceType, targetType);
        } else if (enumerated.isPresent()) {
            final Tuple2<TypeName, CodeBlock> tuple2 = enumeratedCodecCodeGen(enumerated.get(), sourceType, fieldName, className);
            codec = tuple2._2();
            targetType = tuple2._1();
            return new CodecInfo(codec, sourceType, targetType);
        } else if (typeMirror.getKind() == TypeKind.ARRAY && typeMirror.toString().equals("byte[]")) {
            if (codecFromRegistry.isPresent()) {
                return codecFromRegistry.get();
            } else {
                codec = CodeBlock.builder().add("new $T()", BYTE_ARRAY_PRIMITIVE_CODEC).build();
                return new CodecInfo(codec, sourceType, BYTE_BUFFER);
            }
        } else if (typeMirror.getKind() == TypeKind.ARRAY && typeMirror.toString().equals("java.lang.Byte[]")) {
            if (codecFromRegistry.isPresent()) {
                return codecFromRegistry.get();
            } else {
                codec = CodeBlock.builder().add("new $T()", BYTE_ARRAY_CODEC).build();
                return new CodecInfo(codec, sourceType, BYTE_BUFFER);
            }
        } else {
            if (codecFromRegistry.isPresent()) {
                return codecFromRegistry.get();
            } else {
                if (computedCQLClass.isPresent()) {
                    aptUtils.validateTrue(sourceType.equals(computedCQLClass.get()),
                            "CQL class '%s' of @Computed field '%s' of class '%s' should be same as field class '%s'",
                            computedCQLClass.get(), fieldName, className, sourceType);
                }
                context.typeValidator().validateAllowedTypes(aptUtils, sourceType, sourceType);
                codec = CodeBlock.builder().add("new $T<>($T.class)", FALL_THROUGH_CODEC, getRawType(sourceType).box()).build();
                return new CodecInfo(codec, sourceType, targetType);
            }
        }
    }

    public TypeName determineTargetCQLType(GlobalParsingContext context, AnnotationTree annotationTree, TypeName parentType, TypeName sourceType,
                                           String methodName, String paramName, Optional<CodecInfo> codecFromRegistry) {
        final TypeValidator typeValidator = context.typeValidator();
        TypeName targetType = sourceType;
        TypeMirror typeMirror = annotationTree.getCurrentType();

        final Optional<TypedMap> jsonTransform = extractTypedMap(annotationTree, JSON.class);
        final Optional<TypedMap> enumerated = extractTypedMap(annotationTree, Enumerated.class);
        final Optional<TypedMap> codecFromType = extractTypedMap(annotationTree, Codec.class);
        final Optional<TypedMap> runtimeCodec = extractTypedMap(annotationTree, RuntimeCodec.class);
        final Optional<TypedMap> computed = extractTypedMap(annotationTree, Computed.class);
        final Optional<TypeName> computedCQLClass = computed
                .map(x -> x.<Class<?>>getTyped("cqlClass"))
                .map(ClassName::get);
        final boolean isCounter = extractTypedMap(annotationTree, Counter.class).isPresent();

        if (jsonTransform.isPresent()) {
            return ClassName.get(String.class);
        } else if (codecFromType.isPresent()) {
            final CodecContext codecContext = codecFromType.get().getTyped("codecContext");
            context.fieldValidator().validateCodec(aptUtils, typeValidator, codecContext, sourceType, computedCQLClass, isCounter);
            return codecContext.targetType.box();
        } else if(runtimeCodec.isPresent()) {
            final RuntimeCodecContext runtimeCodecContext = runtimeCodec.get().getTyped("runtimeCodecContext");
            context.fieldValidator().validateCodec(aptUtils, typeValidator, runtimeCodecContext, runtimeCodecContext.sourceType, computedCQLClass, isCounter);
            return runtimeCodecContext.targetType.box();
        } else if (enumerated.isPresent()) {
            final TypedMap typedMap = enumerated.get();
            final Object value = typedMap.getTyped("value");
            aptUtils.validateTrue(isAnEnum(value), "The type '%s' on param '%s' of method '%s' in class '%s' is not a java.lang.Enum type",
                    sourceType.toString(), paramName, methodName, parentType);
            final Encoding encoding = typedMap.getTyped("value");
            if (encoding == Encoding.NAME) {
                return STRING;

            } else {
                return OBJECT_INT;
            }
        } else if (typeMirror.getKind() == TypeKind.ARRAY && typeMirror.toString().equals("byte[]")) {
            if (codecFromRegistry.isPresent()) {
                return codecFromRegistry.get().targetType.box();
            } else {
                return BYTE_BUFFER;
            }
        } else if (typeMirror.getKind() == TypeKind.ARRAY && typeMirror.toString().equals("java.lang.Byte[]")) {
            if (codecFromRegistry.isPresent()) {
                return codecFromRegistry.get().targetType.box();
            } else {
                return BYTE_BUFFER;
            }
        } else {
            if (codecFromRegistry.isPresent()) {
                return codecFromRegistry.get().targetType.box();
            } else {
                typeValidator.validateAllowedTypesForFunction(aptUtils, parentType.toString(), methodName, sourceType);
                return targetType;
            }
        }
    }

    private Tuple2<TypeName, CodeBlock> codecCodeGen(FieldParsingContext context, TypedMap annotationInfo, TypeName sourceType,
                                                     Optional<TypeName> computedCQLClass, boolean isCounter) {

        final CodecContext codecContext = annotationInfo.getTyped("codecContext");
        context.fieldValidator().validateCodec(aptUtils, context.typeValidator(), codecContext, sourceType, computedCQLClass, isCounter);
        CodeBlock codec = CodeBlock.builder().add("new $T()", codecContext.codecType).build();

        return new Tuple2<>(codecContext.targetType.box(), codec);
    }

    private Tuple2<TypeName, CodeBlock> runtimeCodecCodeGen(FieldParsingContext context, TypedMap annotationInfo,
                                                            Optional<TypeName> computedCQLClass, boolean isCounter) {

        final RuntimeCodecContext runtimeCodecContext = annotationInfo.getTyped("runtimeCodecContext");
        final TypeName sourceType = runtimeCodecContext.sourceType;
        final TypeName targetType = runtimeCodecContext.targetType;

        context.fieldValidator().validateCodec(aptUtils, context.typeValidator(), runtimeCodecContext, sourceType, computedCQLClass, isCounter);

        final CodeBlock codecNameCode = runtimeCodecContext.codecName.isPresent()
                 ? CodeBlock.builder().add("$T.<$T>of($S)", OPTIONAL, STRING, runtimeCodecContext.codecName.get()).build()
                 : CodeBlock.builder().add("$T.<$T>empty()", OPTIONAL, STRING).build();

        CodeBlock codec = CodeBlock.builder().add("new $T<$T,$T>($T.class, $T.class, $L)", RUNTIME_CODEC_WRAPPER,
                sourceType, targetType, sourceType, targetType, codecNameCode).build();

        return new Tuple2<>(targetType.box(), codec);
    }

    private Tuple2<TypeName, CodeBlock> enumeratedCodecCodeGen(TypedMap annotationInfo, TypeName sourceType, String fieldName, String className) {

        final Object value = annotationInfo.getTyped("value");
        aptUtils.validateTrue(isAnEnum(value), "The type '%s' on field '%s' in class '%s' is not a java.lang.Enum type",
                sourceType.toString(), fieldName, className);
        final Encoding encoding = annotationInfo.getTyped("value");
        if (encoding == Encoding.NAME) {
            return new Tuple2<>(STRING, CodeBlock.builder()
                    .add("new $T<>(java.util.Arrays.asList($T.values()), $T.class)", ENUM_NAME_CODEC, sourceType, sourceType.box())
                    .build());

        } else {
            return new Tuple2<>(OBJECT_INT, CodeBlock.builder()
                    .add("new $T<>(java.util.Arrays.asList($T.values()), $T.class)", ENUM_ORDINAL_CODEC, sourceType, sourceType.box())
                    .build());
        }
    }

    CodeBlock buildJavaTypeForJackson(TypeName sourceType) {
        if (sourceType instanceof ClassName) {
            final ClassName className = (ClassName) sourceType;
            return CodeBlock.builder().add("$T.construct($T.class)", SIMPLE_TYPE, className.box()).build();

        } else if (sourceType instanceof ParameterizedTypeName) {
            final ParameterizedTypeName paramTypeName = (ParameterizedTypeName) sourceType;

            StringJoiner code = new StringJoiner(",",
                    "$T.TYPE_FACTORY_INSTANCE.constructParametricType($T.class,",
                    ")");

            for (TypeName x : paramTypeName.typeArguments) {
                code.add("$L");
            }

            final Object[] headTypes = new Object[]{JSON_CODEC, paramTypeName.rawType};
            final Object[] codeBlocks = paramTypeName.typeArguments
                    .stream()
                    .map(typeName -> (Object) buildJavaTypeForJackson(typeName))
                    .toArray();

            return CodeBlock.builder().add(code.toString(),
                    ArrayUtils.addAll(headTypes, codeBlocks)).build();

        } else if (sourceType instanceof ArrayTypeName) {
            final TypeName componentType = ((ArrayTypeName) sourceType).componentType;
            return CodeBlock.builder().add("$T.TYPE_FACTORY_INSTANCE.constructArrayType($L)",
                    JSON_CODEC, buildJavaTypeForJackson(componentType.box())).build();

        } else if (sourceType instanceof WildcardTypeName) {
            aptUtils.printError("Cannot build Jackson Mapper JavaType for wildcard type " + sourceType.toString());
        } else {
            aptUtils.printError("Cannot build Jackson Mapper JavaType for type " + sourceType.toString());
        }


        return null;
    }

    public static final class CodecInfo {

        final protected CodeBlock codecCode;
        final protected TypeName sourceType;
        final protected TypeName targetType;

        public CodecInfo(CodeBlock codecCode, TypeName sourceType, TypeName targetType) {
            this.codecCode = codecCode;
            this.sourceType = sourceType;
            this.targetType = targetType;
        }
    }

}
