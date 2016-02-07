/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

import static info.archinnov.achilles.internals.apt.AptUtils.*;
import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static info.archinnov.achilles.internals.parser.validator.FieldValidator.validateAllowedType;
import static info.archinnov.achilles.internals.parser.validator.FieldValidator.validateCounter;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.VariableElement;
import javax.lang.model.type.TypeKind;
import javax.lang.model.type.TypeMirror;

import com.datastax.driver.core.GettableData;
import com.squareup.javapoet.*;

import info.archinnov.achilles.annotations.*;
import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.CodecFactory.CodecInfo;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.FieldInfoContext;
import info.archinnov.achilles.internals.parser.context.FieldParsingContext;
import info.archinnov.achilles.internals.strategy.types_nesting.NestedTypesStrategy;
import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.*;


public class FieldParser {

    static final CodeBlock NO_GETTER = CodeBlock.builder().add("gettable$$ -> null").build();
    static final CodeBlock NO_UDT_SETTER = CodeBlock.builder().add("(udt$$, value$$) -> {}").build();
    private final AptUtils aptUtils;
    private final CodecFactory codecFactory;
    private final FieldInfoParser fieldInfoParser;
    private final UDTParser udtParser;

    public FieldParser(AptUtils aptUtils) {
        this.aptUtils = aptUtils;
        this.codecFactory = new CodecFactory(aptUtils);
        this.fieldInfoParser = new FieldInfoParser(aptUtils);
        this.udtParser = new UDTParser(aptUtils);
    }

    public TypeParsingResult parse(VariableElement elm, EntityParsingContext entityContext) {
        final AnnotationTree annotationTree = AnnotationTree.buildFrom(aptUtils, elm);
        final FieldInfoContext fieldInfoContext = fieldInfoParser.buildFieldInfo(elm, annotationTree, entityContext);
        final FieldParsingContext context = new FieldParsingContext(entityContext, getRawType(entityContext.entityType), fieldInfoContext);

        // Perform nested type checking with regard to @Frozen if not a tuple type
        // Tuple types are frozen by default
        final NestedTypesStrategy nestedTypesStrategy = context.entityContext.globalContext.nestedTypesStrategy;
        nestedTypesStrategy.validate(aptUtils, annotationTree, context.fieldName, context.entityRawType);

        return parseType(annotationTree, context, TypeName.get(elm.asType()));
    }

    protected TypeParsingResult parseType(AnnotationTree annotationTree, FieldParsingContext context, TypeName sourceType) {
        final TypeMirror currentTypeMirror = annotationTree.getCurrentType();
        final Map<Class<? extends Annotation>, TypedMap> annotationsInfo = annotationTree.getAnnotations();
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(currentTypeMirror);

        boolean isUDT = currentTypeMirror.getKind() == TypeKind.DECLARED
                && aptUtils.getAnnotationOnClass(currentTypeMirror, UDT.class).isPresent();

        if (containsAnnotation(annotationTree, JSON.class)) {
            return parseSimpleType(annotationTree, context, sourceType);
        } else if (containsAnnotation(annotationTree, Computed.class)) {
            return parseComputedType(annotationTree, context, sourceType);
        } else if (aptUtils.isAssignableFrom(Tuple1.class, currentTypeMirror)) {
            return parseTuple1(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple2.class, currentTypeMirror)) {
            return parseTuple2(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple3.class, currentTypeMirror)) {
            return parseTuple3(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple4.class, currentTypeMirror)) {
            return parseTuple4(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple5.class, currentTypeMirror)) {
            return parseTuple5(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple6.class, currentTypeMirror)) {
            return parseTuple6(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple7.class, currentTypeMirror)) {
            return parseTuple7(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple8.class, currentTypeMirror)) {
            return parseTuple8(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple9.class, currentTypeMirror)) {
            return parseTuple9(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(Tuple10.class, currentTypeMirror)) {
            return parseTuple10(annotationTree, context);
        } else if (aptUtils.isAssignableFrom(List.class, currentTypeMirror)) {
            return parseList(annotationTree, context, annotationsInfo, typeArguments);
        } else if (aptUtils.isAssignableFrom(Set.class, currentTypeMirror)) {
            return parseSet(annotationTree, context, annotationsInfo, typeArguments);
        } else if (aptUtils.isAssignableFrom(Map.class, currentTypeMirror)) {
            return parseMap(annotationTree, context, annotationsInfo, typeArguments);
        } else if (isUDT) {
            return udtParser.parseUDT(annotationTree, context, this);
        } else {
            return parseSimpleType(annotationTree, context, sourceType);
        }
    }


    private TypeParsingResult parseComputedType(AnnotationTree annotationTree, FieldParsingContext context, TypeName sourceType) {
        final CodecInfo codecInfo = codecFactory.createCodec(sourceType, annotationTree, context);
        final TypeName rawTargetType = getRawType(codecInfo.targetType);
        final TypedMap computed = extractTypedMap(annotationTree, Computed.class).get();
        final String alias = computed.getTyped("alias");

        CodeBlock extractor = context.buildExtractor
                ? CodeBlock.builder().add("gettableData$$ -> gettableData$$.$L", TypeUtils.gettableDataGetter(rawTargetType, alias)).build()
                : NO_GETTER;

        CodeBlock typeCode = CodeBlock.builder().add("new $T<$T, $T, $T>($L, $L, $L)",
                COMPUTED_PROPERTY,
                context.entityRawType,
                sourceType,
                codecInfo.targetType,
                context.fieldInfoCode,
                extractor,
                codecInfo.codecCode)
                .build();

        final ParameterizedTypeName propertyType = genericType(COMPUTED_PROPERTY, context.entityRawType, codecInfo.sourceType, codecInfo.targetType);

        return new TypeParsingResult(context, annotationTree.hasNext() ? annotationTree.next() : annotationTree,
                sourceType, codecInfo.targetType, propertyType, typeCode);
    }

    private TypeParsingResult parseSimpleType(AnnotationTree annotationTree, FieldParsingContext context, TypeName sourceType) {
        final CodecInfo codecInfo = context.hasCodecFor(sourceType)
                                ? context.getCodecFor(sourceType)
                                : codecFactory.createCodec(sourceType, annotationTree, context);

        final TypeName rawTargetType = getRawType(codecInfo.targetType);

        validateAllowedType(aptUtils, rawTargetType, context);
        validateCounter(aptUtils, rawTargetType, annotationTree.getAnnotations().keySet(), context);

        final CodeBlock dataType;

        if (containsAnnotation(annotationTree, TimeUUID.class)) {
            dataType = CodeBlock.builder().add("$T.timeuuid()", DATATYPE).build();
        } else if (containsAnnotation(annotationTree, Counter.class)) {
            dataType = CodeBlock.builder().add("$T.counter()", DATATYPE).build();
        } else {
            dataType = TypeUtils.buildDataTypeFor(rawTargetType);
        }

        CodeBlock gettable = context.buildExtractor
                ? CodeBlock.builder().add("gettableData$$ -> gettableData$$.$L", TypeUtils.gettableDataGetter(rawTargetType, context.cqlColumn)).build()
                : NO_GETTER;

        CodeBlock settable = context.buildExtractor
                ? CodeBlock.builder().add("(settableData$$, value$$) -> settableData$$.$L", TypeUtils.settableDataSetter(rawTargetType, context.cqlColumn)).build()
                : NO_UDT_SETTER;


        CodeBlock typeCode = CodeBlock.builder().add("new $T<$T, $T, $T>($L, $L, $L, $L, new $T(){}, new $T(){}, $L)",
                SIMPLE_PROPERTY,
                context.entityRawType,
                sourceType.box(),
                codecInfo.targetType.box(),
                context.fieldInfoCode,
                dataType,
                gettable,
                settable,
                genericType(TYPE_TOKEN, sourceType.box()),
                genericType(TYPE_TOKEN, codecInfo.targetType.box()),
                codecInfo.codecCode)
                .build();
        final ParameterizedTypeName propertyType = genericType(SIMPLE_PROPERTY, context.entityRawType, codecInfo.sourceType.box(), codecInfo.targetType.box());

        return new TypeParsingResult(context, annotationTree.hasNext() ? annotationTree.next() : annotationTree,
                sourceType, codecInfo.targetType, propertyType, typeCode);
    }

    private TypeParsingResult parseMap(AnnotationTree annotationTree, FieldParsingContext context, Map<Class<? extends Annotation>, TypedMap> annotationsInfo, List<? extends TypeMirror> typeArguments) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final boolean hasFrozen = containsAnnotation(annotationsInfo.keySet(), Frozen.class);
        final boolean hasEmptyCollectionIfNull = containsAnnotation(annotationsInfo.keySet(), EmptyCollectionIfNull.class);
        final TypeMirror keyTypeMirror = typeArguments.get(0);
        final TypeMirror valueTypeMirror = typeArguments.get(1);
        final TypeName sourceKeyType = TypeName.get(keyTypeMirror);
        final TypeParsingResult keyParsingResult = this.parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceKeyType), sourceKeyType);
        final TypeName sourceValueType = TypeName.get(valueTypeMirror);
        final TypeParsingResult valueParsingResult = this.parseType(keyParsingResult.annotationTree, context.noLambda(context.entityRawType, sourceValueType), sourceValueType);
        final CodeBlock typeCode = CodeBlock.builder()
                .add("new $T<$T, $T, $T, $T, $T>($L, $L, $L, $L, $L)",
                        MAP_PROPERTY,
                        context.entityRawType,
                        sourceKeyType,
                        keyParsingResult.targetType.box(),
                        sourceValueType,
                        valueParsingResult.targetType.box(),
                        context.fieldInfoCode,
                        hasFrozen,
                        hasEmptyCollectionIfNull,
                        keyParsingResult.typeCode,
                        valueParsingResult.typeCode).build();
        final TypeName targetType = genericType(MAP, keyParsingResult.targetType, valueParsingResult.targetType);
        final ParameterizedTypeName propertyType = genericType(MAP_PROPERTY, context.entityRawType, sourceKeyType, keyParsingResult.targetType, sourceValueType, valueParsingResult.targetType);
        return new TypeParsingResult(context, valueParsingResult.annotationTree, sourceType, targetType, propertyType, typeCode);
    }

    private TypeParsingResult parseSet(AnnotationTree annotationTree, FieldParsingContext context, Map<Class<? extends Annotation>, TypedMap> annotationsInfo, List<? extends TypeMirror> typeArguments) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final boolean hasFrozen = containsAnnotation(annotationsInfo.keySet(), Frozen.class);
        final boolean hasEmptyCollectionIfNull = containsAnnotation(annotationsInfo.keySet(), EmptyCollectionIfNull.class);
        final TypeMirror typeMirror1 = typeArguments.get(0);
        final TypeName sourceType1 = TypeName.get(typeMirror1);
        final TypeParsingResult parsingResult = this.parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final CodeBlock typeCode = CodeBlock.builder()
                .add("new $T<>($L, $L, $L, $T.class, $L)",
                        SET_PROPERTY,
                        context.fieldInfoCode,
                        hasFrozen,
                        hasEmptyCollectionIfNull,
                        getRawType(parsingResult.targetType).box(),
                        parsingResult.typeCode).build();
        final TypeName targetType = genericType(SET, parsingResult.targetType);
        final ParameterizedTypeName propertyType = genericType(SET_PROPERTY, context.entityRawType, sourceType1, parsingResult.targetType);
        return new TypeParsingResult(context, parsingResult.annotationTree, sourceType, targetType, propertyType, typeCode);
    }

    private TypeParsingResult parseList(AnnotationTree annotationTree, FieldParsingContext context, Map<Class<? extends Annotation>, TypedMap> annotationsInfo, List<? extends TypeMirror> typeArguments) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final boolean hasFrozen = containsAnnotation(annotationsInfo.keySet(), Frozen.class);
        final boolean hasEmptyCollectionIfNull = containsAnnotation(annotationsInfo.keySet(), EmptyCollectionIfNull.class);
        final TypeMirror typeMirror1 = typeArguments.get(0);
        final TypeName sourceType1 = TypeName.get(typeMirror1);
        final TypeParsingResult parsingResult = this.parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final CodeBlock typeCode = CodeBlock.builder()
                .add("new $T<>($L, $L, $L, $T.class, $L)",
                        LIST_PROPERTY,
                        context.fieldInfoCode,
                        hasFrozen,
                        hasEmptyCollectionIfNull,
                        getRawType(parsingResult.targetType).box(),
                        parsingResult.typeCode).build();
        final TypeName targetType = genericType(LIST, parsingResult.targetType);
        final ParameterizedTypeName propertyType = genericType(LIST_PROPERTY, context.entityRawType, sourceType1, parsingResult.targetType);
        return new TypeParsingResult(context, parsingResult.annotationTree, sourceType, targetType, propertyType, typeCode);
    }


    protected TypeParsingResult parseTuple1(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final TypeMirror typeMirror1 = AptUtils.getTypeArguments(annotationTree.getCurrentType()).get(0);
        final TypeName sourceType1 = TypeName.get(typeMirror1);
        final TypeParsingResult parsingResult = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L)",
                TUPLE1_PROPERTY,
                context.fieldInfoCode,
                parsingResult.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE1_PROPERTY, context.entityRawType, sourceType1);
        return new TypeParsingResult(context, parsingResult.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple2(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L)",
                TUPLE2_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE2_PROPERTY, context.entityRawType, sourceType1, sourceType2);
        return new TypeParsingResult(context, parsingResult2.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple3(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));
        final TypeName sourceType3 = TypeName.get(typeArguments.get(2));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final TypeParsingResult parsingResult3 = parseType(parsingResult2.annotationTree, context.noLambda(context.entityRawType, sourceType3), sourceType3);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L, $L)",
                TUPLE3_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode,
                parsingResult3.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE3_PROPERTY, context.entityRawType, sourceType1,
                sourceType2, sourceType3);
        return new TypeParsingResult(context, parsingResult3.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple4(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));
        final TypeName sourceType3 = TypeName.get(typeArguments.get(2));
        final TypeName sourceType4 = TypeName.get(typeArguments.get(3));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final TypeParsingResult parsingResult3 = parseType(parsingResult2.annotationTree, context.noLambda(context.entityRawType, sourceType3), sourceType3);
        final TypeParsingResult parsingResult4 = parseType(parsingResult3.annotationTree, context.noLambda(context.entityRawType, sourceType4), sourceType4);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L, $L, $L)",
                TUPLE4_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode,
                parsingResult3.typeCode,
                parsingResult4.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE4_PROPERTY, context.entityRawType, sourceType1,
                sourceType2, sourceType3, sourceType4);
        return new TypeParsingResult(context, parsingResult4.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple5(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));
        final TypeName sourceType3 = TypeName.get(typeArguments.get(2));
        final TypeName sourceType4 = TypeName.get(typeArguments.get(3));
        final TypeName sourceType5 = TypeName.get(typeArguments.get(4));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final TypeParsingResult parsingResult3 = parseType(parsingResult2.annotationTree, context.noLambda(context.entityRawType, sourceType3), sourceType3);
        final TypeParsingResult parsingResult4 = parseType(parsingResult3.annotationTree, context.noLambda(context.entityRawType, sourceType4), sourceType4);
        final TypeParsingResult parsingResult5 = parseType(parsingResult4.annotationTree, context.noLambda(context.entityRawType, sourceType5), sourceType5);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L, $L, $L, $L)",
                TUPLE5_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode,
                parsingResult3.typeCode,
                parsingResult4.typeCode,
                parsingResult5.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE5_PROPERTY, context.entityRawType, sourceType1,
                sourceType2, sourceType3, sourceType4, sourceType5);
        return new TypeParsingResult(context, parsingResult5.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple6(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));
        final TypeName sourceType3 = TypeName.get(typeArguments.get(2));
        final TypeName sourceType4 = TypeName.get(typeArguments.get(3));
        final TypeName sourceType5 = TypeName.get(typeArguments.get(4));
        final TypeName sourceType6 = TypeName.get(typeArguments.get(5));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final TypeParsingResult parsingResult3 = parseType(parsingResult2.annotationTree, context.noLambda(context.entityRawType, sourceType3), sourceType3);
        final TypeParsingResult parsingResult4 = parseType(parsingResult3.annotationTree, context.noLambda(context.entityRawType, sourceType4), sourceType4);
        final TypeParsingResult parsingResult5 = parseType(parsingResult4.annotationTree, context.noLambda(context.entityRawType, sourceType5), sourceType5);
        final TypeParsingResult parsingResult6 = parseType(parsingResult5.annotationTree, context.noLambda(context.entityRawType, sourceType6), sourceType6);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L, $L, $L, $L, $L)",
                TUPLE6_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode,
                parsingResult3.typeCode,
                parsingResult4.typeCode,
                parsingResult5.typeCode,
                parsingResult6.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE6_PROPERTY, context.entityRawType, sourceType1,
                sourceType2, sourceType3, sourceType4, sourceType5, sourceType6);
        return new TypeParsingResult(context, parsingResult5.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple7(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));
        final TypeName sourceType3 = TypeName.get(typeArguments.get(2));
        final TypeName sourceType4 = TypeName.get(typeArguments.get(3));
        final TypeName sourceType5 = TypeName.get(typeArguments.get(4));
        final TypeName sourceType6 = TypeName.get(typeArguments.get(5));
        final TypeName sourceType7 = TypeName.get(typeArguments.get(6));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final TypeParsingResult parsingResult3 = parseType(parsingResult2.annotationTree, context.noLambda(context.entityRawType, sourceType3), sourceType3);
        final TypeParsingResult parsingResult4 = parseType(parsingResult3.annotationTree, context.noLambda(context.entityRawType, sourceType4), sourceType4);
        final TypeParsingResult parsingResult5 = parseType(parsingResult4.annotationTree, context.noLambda(context.entityRawType, sourceType5), sourceType5);
        final TypeParsingResult parsingResult6 = parseType(parsingResult5.annotationTree, context.noLambda(context.entityRawType, sourceType6), sourceType6);
        final TypeParsingResult parsingResult7 = parseType(parsingResult6.annotationTree, context.noLambda(context.entityRawType, sourceType7), sourceType7);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L, $L, $L, $L, $L, $L)",
                TUPLE7_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode,
                parsingResult3.typeCode,
                parsingResult4.typeCode,
                parsingResult5.typeCode,
                parsingResult6.typeCode,
                parsingResult7.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE7_PROPERTY, context.entityRawType, sourceType1,
                sourceType2, sourceType3, sourceType4, sourceType5, sourceType6, sourceType7);
        return new TypeParsingResult(context, parsingResult5.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple8(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));
        final TypeName sourceType3 = TypeName.get(typeArguments.get(2));
        final TypeName sourceType4 = TypeName.get(typeArguments.get(3));
        final TypeName sourceType5 = TypeName.get(typeArguments.get(4));
        final TypeName sourceType6 = TypeName.get(typeArguments.get(5));
        final TypeName sourceType7 = TypeName.get(typeArguments.get(6));
        final TypeName sourceType8 = TypeName.get(typeArguments.get(7));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final TypeParsingResult parsingResult3 = parseType(parsingResult2.annotationTree, context.noLambda(context.entityRawType, sourceType3), sourceType3);
        final TypeParsingResult parsingResult4 = parseType(parsingResult3.annotationTree, context.noLambda(context.entityRawType, sourceType4), sourceType4);
        final TypeParsingResult parsingResult5 = parseType(parsingResult4.annotationTree, context.noLambda(context.entityRawType, sourceType5), sourceType5);
        final TypeParsingResult parsingResult6 = parseType(parsingResult5.annotationTree, context.noLambda(context.entityRawType, sourceType6), sourceType6);
        final TypeParsingResult parsingResult7 = parseType(parsingResult6.annotationTree, context.noLambda(context.entityRawType, sourceType7), sourceType7);
        final TypeParsingResult parsingResult8 = parseType(parsingResult7.annotationTree, context.noLambda(context.entityRawType, sourceType8), sourceType8);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L, $L, $L, $L, $L, $L, $L)",
                TUPLE8_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode,
                parsingResult3.typeCode,
                parsingResult4.typeCode,
                parsingResult5.typeCode,
                parsingResult6.typeCode,
                parsingResult7.typeCode,
                parsingResult8.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE8_PROPERTY, context.entityRawType, sourceType1,
                sourceType2, sourceType3, sourceType4, sourceType5, sourceType6, sourceType7, sourceType8);
        return new TypeParsingResult(context, parsingResult5.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple9(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));
        final TypeName sourceType3 = TypeName.get(typeArguments.get(2));
        final TypeName sourceType4 = TypeName.get(typeArguments.get(3));
        final TypeName sourceType5 = TypeName.get(typeArguments.get(4));
        final TypeName sourceType6 = TypeName.get(typeArguments.get(5));
        final TypeName sourceType7 = TypeName.get(typeArguments.get(6));
        final TypeName sourceType8 = TypeName.get(typeArguments.get(7));
        final TypeName sourceType9 = TypeName.get(typeArguments.get(8));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final TypeParsingResult parsingResult3 = parseType(parsingResult2.annotationTree, context.noLambda(context.entityRawType, sourceType3), sourceType3);
        final TypeParsingResult parsingResult4 = parseType(parsingResult3.annotationTree, context.noLambda(context.entityRawType, sourceType4), sourceType4);
        final TypeParsingResult parsingResult5 = parseType(parsingResult4.annotationTree, context.noLambda(context.entityRawType, sourceType5), sourceType5);
        final TypeParsingResult parsingResult6 = parseType(parsingResult5.annotationTree, context.noLambda(context.entityRawType, sourceType6), sourceType6);
        final TypeParsingResult parsingResult7 = parseType(parsingResult6.annotationTree, context.noLambda(context.entityRawType, sourceType7), sourceType7);
        final TypeParsingResult parsingResult8 = parseType(parsingResult7.annotationTree, context.noLambda(context.entityRawType, sourceType8), sourceType8);
        final TypeParsingResult parsingResult9 = parseType(parsingResult8.annotationTree, context.noLambda(context.entityRawType, sourceType9), sourceType9);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L, $L, $L, $L, $L, $L, $L, $L)",
                TUPLE9_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode,
                parsingResult3.typeCode,
                parsingResult4.typeCode,
                parsingResult5.typeCode,
                parsingResult6.typeCode,
                parsingResult7.typeCode,
                parsingResult8.typeCode,
                parsingResult9.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE9_PROPERTY, context.entityRawType, sourceType1,
                sourceType2, sourceType3, sourceType4, sourceType5, sourceType6, sourceType7, sourceType8, sourceType9);
        return new TypeParsingResult(context, parsingResult5.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    protected TypeParsingResult parseTuple10(AnnotationTree annotationTree, FieldParsingContext context) {
        final TypeName sourceType = TypeName.get(annotationTree.getCurrentType());
        final List<? extends TypeMirror> typeArguments = AptUtils.getTypeArguments(annotationTree.getCurrentType());
        final TypeName sourceType1 = TypeName.get(typeArguments.get(0));
        final TypeName sourceType2 = TypeName.get(typeArguments.get(1));
        final TypeName sourceType3 = TypeName.get(typeArguments.get(2));
        final TypeName sourceType4 = TypeName.get(typeArguments.get(3));
        final TypeName sourceType5 = TypeName.get(typeArguments.get(4));
        final TypeName sourceType6 = TypeName.get(typeArguments.get(5));
        final TypeName sourceType7 = TypeName.get(typeArguments.get(6));
        final TypeName sourceType8 = TypeName.get(typeArguments.get(7));
        final TypeName sourceType9 = TypeName.get(typeArguments.get(8));
        final TypeName sourceType10 = TypeName.get(typeArguments.get(9));

        final TypeParsingResult parsingResult1 = parseType(annotationTree.next(), context.noLambda(context.entityRawType, sourceType1), sourceType1);
        final TypeParsingResult parsingResult2 = parseType(parsingResult1.annotationTree, context.noLambda(context.entityRawType, sourceType2), sourceType2);
        final TypeParsingResult parsingResult3 = parseType(parsingResult2.annotationTree, context.noLambda(context.entityRawType, sourceType3), sourceType3);
        final TypeParsingResult parsingResult4 = parseType(parsingResult3.annotationTree, context.noLambda(context.entityRawType, sourceType4), sourceType4);
        final TypeParsingResult parsingResult5 = parseType(parsingResult4.annotationTree, context.noLambda(context.entityRawType, sourceType5), sourceType5);
        final TypeParsingResult parsingResult6 = parseType(parsingResult5.annotationTree, context.noLambda(context.entityRawType, sourceType6), sourceType6);
        final TypeParsingResult parsingResult7 = parseType(parsingResult6.annotationTree, context.noLambda(context.entityRawType, sourceType7), sourceType7);
        final TypeParsingResult parsingResult8 = parseType(parsingResult7.annotationTree, context.noLambda(context.entityRawType, sourceType8), sourceType8);
        final TypeParsingResult parsingResult9 = parseType(parsingResult8.annotationTree, context.noLambda(context.entityRawType, sourceType9), sourceType9);
        final TypeParsingResult parsingResult10 = parseType(parsingResult9.annotationTree, context.noLambda(context.entityRawType, sourceType10), sourceType10);
        final CodeBlock codeBlock = CodeBlock.builder().add("new $T<>($L, $L, $L, $L, $L, $L, $L, $L, $L, $L, $L)",
                TUPLE10_PROPERTY,
                context.fieldInfoCode,
                parsingResult1.typeCode,
                parsingResult2.typeCode,
                parsingResult3.typeCode,
                parsingResult4.typeCode,
                parsingResult5.typeCode,
                parsingResult6.typeCode,
                parsingResult7.typeCode,
                parsingResult8.typeCode,
                parsingResult9.typeCode,
                parsingResult10.typeCode).build();
        final ParameterizedTypeName propertyType = genericType(TUPLE10_PROPERTY, context.entityRawType, sourceType1,
                sourceType2, sourceType3, sourceType4, sourceType5, sourceType6, sourceType7, sourceType8, sourceType9, sourceType10);
        return new TypeParsingResult(context, parsingResult5.annotationTree, sourceType, JAVA_DRIVER_TUPLE_VALUE_TYPE, propertyType, codeBlock);
    }

    public static class TypeParsingResult {
        private static final Modifier[] FIELD_MODIFIERS = new Modifier[]{Modifier.FINAL, Modifier.PUBLIC, Modifier.STATIC};
        final public FieldParsingContext context;
        final public AnnotationTree annotationTree;
        final public TypeName sourceType;
        final public TypeName targetType;
        final public CodeBlock typeCode;
        final public TypeName propertyType;

        public TypeParsingResult(FieldParsingContext context, AnnotationTree annotationTree, TypeName sourceType, TypeName targetType,
                                 TypeName propertyType, CodeBlock typeCode) {
            this.context = context;
            this.annotationTree = annotationTree;
            this.sourceType = sourceType;
            this.targetType = targetType;
            this.propertyType = propertyType;
            this.typeCode = typeCode;
        }

        public FieldSpec buildPropertyAsField() {
            final FieldSpec.Builder builder = FieldSpec
                    .builder(propertyType, context.fieldName, FIELD_MODIFIERS)
                    .addJavadoc("Meta class for '$L' property <br/>\n", context.fieldName)
                    .addJavadoc("The meta class exposes some useful methods: ")
                    .addJavadoc("<ul>\n")
                    .addJavadoc("   <li>encodeFromJava: encode a property from raw Java to CQL java compatible type </li>\n")
                    .addJavadoc("   <li>encodeField: extract the current property value from the given $T instance and encode to CQL java compatible type </li>\n", context.entityRawType)
                    .addJavadoc("   <li>decodeFromGettable: decode from a {@link $T} instance (Row, UDTValue, TupleValue) the current property</li>\n", ClassName.get(GettableData.class))
                    .addJavadoc("</ul>\n")
                    .initializer(typeCode);
            AnnotationSpec.Builder annotationBuilder = null;
            if (typeCode.toString().contains("SimpleProperty")) {
                annotationBuilder = AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "serial");
            }

            if (typeCode.toString().contains("FieldInfo.<")) {
                annotationBuilder = annotationBuilder == null
                        ? AnnotationSpec.builder(SuppressWarnings.class).addMember("value", "$S", "unchecked")
                        : annotationBuilder.addMember("value", "$S", "unchecked");
            }

            if (annotationBuilder != null) {
                builder.addAnnotation(annotationBuilder.build());
            }

            return builder.build();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TypeParsingResult that = (TypeParsingResult) o;
            return Objects.equals(context, that.context);
        }

        @Override
        public int hashCode() {
            return Objects.hash(context);
        }
    }

}
