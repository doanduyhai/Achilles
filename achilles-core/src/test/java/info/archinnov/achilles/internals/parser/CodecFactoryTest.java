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

import static info.archinnov.achilles.internals.apt.AptUtils.findFieldInType;
import static info.archinnov.achilles.internals.parser.TypeUtils.LIST;
import static info.archinnov.achilles.internals.parser.TypeUtils.MAP;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.Optional;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ConsistencyLevel;
import com.squareup.javapoet.*;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.metamodel.columns.ColumnInfo;
import info.archinnov.achilles.internals.metamodel.columns.ColumnType;
import info.archinnov.achilles.internals.metamodel.index.IndexInfo;
import info.archinnov.achilles.internals.parser.CodecFactory.CodecInfo;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.FieldInfoContext;
import info.archinnov.achilles.internals.parser.context.FieldParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.codecs.IntToStringCodec;
import info.archinnov.achilles.internals.sample_classes.codecs.StringToLongCodec;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs;

public class CodecFactoryTest extends AbstractTestProcessor {

    private final CodeBlock fieldInfoCode = CodeBlock.builder().build();

    @Before
    public void setUp() {
        super.testEntityClass = TestEntityForCodecs.class;
    }


    @Test
    public void should_create_codec_for_json_map() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @JSON private Map<@JSON Integer, List<Integer>> jsonMap;
            final VariableElement elm = findFieldInType(typeElement, "jsonMap");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(Map.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(Map.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo("new info.archinnov.achilles.internals.codec.JSONCodec<>(java.util.Map.class, com.fasterxml.jackson.databind.type.SimpleType.construct(java.util.Map.class))");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_transformed_int() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Codec(IntToStringCodec.class) private Integer integer;
            final VariableElement elm = findFieldInType(typeElement, "integer");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(Integer.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(Integer.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo("new " +
                    IntToStringCodec.class.getCanonicalName() + "()");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_object_byte_array() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // private byte[] objectByteArray;
            final VariableElement elm = findFieldInType(typeElement, "objectByteArray");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ArrayTypeName.of(ClassName.get(Byte.class)), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(Byte[].class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(ByteBuffer.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo("new info.archinnov.achilles.internals.codec.ByteArrayCodec()");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_primitive_byte_array() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // private byte[] primitiveByteArray;
            final VariableElement elm = findFieldInType(typeElement, "primitiveByteArray");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ArrayTypeName.of(TypeName.BYTE), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(byte[].class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(ByteBuffer.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo("new info.archinnov.achilles.internals.codec.ByteArrayPrimitiveCodec()");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_object_byte() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // private Byte objectByte
            final VariableElement elm = findFieldInType(typeElement, "objectByte");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(Byte.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(Byte.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(Byte.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo("new info.archinnov.achilles.internals.codec.FallThroughCodec<>(java.lang.Byte.class)");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_primitive_byte() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // private byte primitiveByte
            final VariableElement elm = findFieldInType(typeElement, "primitiveByte");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(TypeName.BYTE, tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(byte.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(byte.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo("new info.archinnov.achilles.internals.codec.FallThroughCodec<>(java.lang.Byte.class)");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_string() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // private String value
            final VariableElement elm = findFieldInType(typeElement, "value");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(String.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo("new info.archinnov.achilles.internals.codec.FallThroughCodec<>(java.lang.String.class)");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_json() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // private @JSON Date time
            final VariableElement elm = findFieldInType(typeElement, "time");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(Date.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(Date.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo("new info.archinnov.achilles.internals.codec.JSONCodec<>(java.util.Date.class, com.fasterxml.jackson.databind.type.SimpleType.construct(java.util.Date.class))");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_enumerated() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Enumerated(value = NAME) private ConsistencyLevel consistencyLevel
            final VariableElement elm = findFieldInType(typeElement, "consistencyLevel");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(ConsistencyLevel.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(ConsistencyLevel.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo(
                    "new info.archinnov.achilles.internals.codec.EnumNameCodec<>(java.util.Arrays.asList(com.datastax.driver.core.ConsistencyLevel.values()), com.datastax.driver.core.ConsistencyLevel.class)");
        });
        launchTest();
    }

    @Test
    public void should_create_codec_for_computed() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Computed(function = "writetime",  alias = "writetime", targettargetColumnsap"}, cqlClass = Integer.class)
            // @Codec(IntToStringCodec.class)
            // private Integer writeTimeAsInt;
            final VariableElement elm = findFieldInType(typeElement, "writeTimeAsInt");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(Integer.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(Integer.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo(
                    "new " + IntToStringCodec.class.getCanonicalName() + "()");
        });
        launchTest();
    }

    @Test
    public void should_create_native_codec_for_computed() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Computed(function = "writetime",  alias = "writetime", targettargetColumnsap"}, cqlClass = Long.class)
            // private Long writeTime;
            final VariableElement elm = findFieldInType(typeElement, "writeTime");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(Long.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(Long.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(Long.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo(
                    "new info.archinnov.achilles.internals.codec.FallThroughCodec<>(java.lang.Long.class)");
        });
        launchTest();
    }

    @Test
    public void should_create_native_codec_for_counter() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Counter private Long counter;
            final VariableElement elm = findFieldInType(typeElement, "counter");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(Long.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(Long.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(Long.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo(
                    "new info.archinnov.achilles.internals.codec.FallThroughCodec<>(java.lang.Long.class)");
        });
        launchTest();
    }

    @Test
    public void should_create_custom_codec_for_counter() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Counter @Codec(StringToLongCodec.class) private String counterWithCodec;
            final VariableElement elm = findFieldInType(typeElement, "counterWithCodec");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            final CodecInfo codecInfo = codecFactory.createCodec(ClassName.get(String.class), tree, context, Optional.empty());

            assertThat(codecInfo.sourceType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(codecInfo.targetType.toString()).isEqualTo(Long.class.getCanonicalName());
            assertThat(codecInfo.codecCode.toString()).isEqualTo(
                    "new " + StringToLongCodec.class.getCanonicalName() + "()");
        });
        launchTest();
    }

    @Test
    public void should_fail_create_custom_codec_for_counter_if_target_type_not_matched() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Counter @Codec(IntToStringCodec.class) private Integer counterWithWrongCodec;
            final VariableElement elm = findFieldInType(typeElement, "counterWithWrongCodec");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            codecFactory.createCodec(ClassName.get(Integer.class), tree, context, Optional.empty());
        });
        failTestWithMessage("Codec 'info.archinnov.achilles.internals.sample_classes.codecs.IntToStringCodec' " +
                "target type 'java.lang.String' should be Long/long because the column is annotated with @Counter");
    }

    @Test
    public void should_fail_creating_codec_for_computed_if_target_type_not_matched() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Computed(function = "writetime",  alias = "writetime", targettargetColumnsap"}, cqlClass = Long.class)
            // @Codec(IntToStringCodec.class)
            // private Integer writeTimeAsLong;
            final VariableElement elm = findFieldInType(typeElement, "writeTimeAsLong");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            codecFactory.createCodec(ClassName.get(Integer.class), tree, context, Optional.empty());
        });
        failTestWithMessage("Codec 'info.archinnov.achilles.internals.sample_classes.codecs.IntToStringCodec' " +
                "target type 'java.lang.String' should match computed CQL type 'java.lang.Long'");
    }

    @Test
    public void should_fail_creating_native_codec_for_computed_if_target_type_not_matched() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityForCodecs.class.getCanonicalName());
            final FieldParsingContext context = getFieldParsingContext(aptUtils, typeElement);

            // @Computed(function = "writetime",  alias = "writetime", targettargetColumnsap"}, cqlClass = Long.class)
            // private Integer writeTimeNotMatchingComputed;
            final VariableElement elm = findFieldInType(typeElement, "writeTimeNotMatchingComputed");
            final AnnotationTree tree = AnnotationTree.buildFrom(aptUtils, context.entityContext.globalContext, elm);
            codecFactory.createCodec(ClassName.get(Integer.class), tree, context, Optional.empty());
        });
        failTestWithMessage("CQL class 'java.lang.Long' of @Computed field 'field' " +
                "of class 'info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs' " +
                "should be same as field class 'java.lang.Integer'");
    }

    @Test
    public void should_create_JavaType_for_simple_type() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);

            final CodeBlock actual = codecFactory.buildJavaTypeForJackson(ClassName.get(String.class));
            assertThat(actual.toString()).isEqualTo("com.fasterxml.jackson.databind.type.SimpleType.construct(java.lang.String.class)");
        });
        launchTest();
    }

    @Test
    public void should_create_JavaType_for_parameterized_type() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final ParameterizedTypeName listOfString = TypeUtils.genericType(LIST, ClassName.get(String.class));
            TypeName paramType = ParameterizedTypeName.get(MAP, ClassName.get(Integer.class), listOfString);
            final CodeBlock actual = codecFactory.buildJavaTypeForJackson(paramType);
            assertThat(actual.toString()).isEqualTo("info.archinnov.achilles.internals.codec.JSONCodec.TYPE_FACTORY_INSTANCE.constructParametricType(" +
                    "java.util.Map.class," +
                    "com.fasterxml.jackson.databind.type.SimpleType.construct(java.lang.Integer.class)," +
                    "info.archinnov.achilles.internals.codec.JSONCodec.TYPE_FACTORY_INSTANCE.constructParametricType(" +
                    "java.util.List.class," +
                    "com.fasterxml.jackson.databind.type.SimpleType.construct(java.lang.String.class)))");
        });
        launchTest();
    }

    @Test
    public void should_create_JavaType_for_array_type() throws Exception {
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            final ParameterizedTypeName listOfString = TypeUtils.genericType(LIST, ClassName.get(String.class));
            TypeName arrayType = ArrayTypeName.of(listOfString);
            final CodeBlock actual = codecFactory.buildJavaTypeForJackson(arrayType);
            assertThat(actual.toString()).isEqualTo("info.archinnov.achilles.internals.codec.JSONCodec.TYPE_FACTORY_INSTANCE.constructArrayType(" +
                    "info.archinnov.achilles.internals.codec.JSONCodec.TYPE_FACTORY_INSTANCE.constructParametricType(" +
                    "java.util.List.class," +
                    "com.fasterxml.jackson.databind.type.SimpleType.construct(java.lang.String.class)))");
        });
        launchTest();
    }

    @Test
    public void should_fail_creating_JavaType_for_wildcard_type() throws Exception {
        final WildcardTypeName wildCardType = WildcardTypeName.subtypeOf(TypeName.OBJECT);
        setExec(aptUtils -> {
            final CodecFactory codecFactory = new CodecFactory(aptUtils);
            codecFactory.buildJavaTypeForJackson(wildCardType);
        });
        failTestWithMessage("Cannot build Jackson Mapper JavaType for wildcard type " + wildCardType.toString());
    }

    private FieldParsingContext getFieldParsingContext(AptUtils aptUtils, TypeElement typeElement) {
        final EntityParsingContext epc = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), null, GlobalParsingContext.defaultContext());
        return new FieldParsingContext(epc, ClassName.get(aptUtils.erasure(typeElement)),
                new FieldInfoContext(fieldInfoCode, "field", "column", ColumnType.NORMAL, new ColumnInfo((false)), IndexInfo.noIndex()));
    }
}