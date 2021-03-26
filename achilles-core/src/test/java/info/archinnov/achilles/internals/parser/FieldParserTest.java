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
import static info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter.EXPLICIT_ENTITY_FIELD_FILTER;
import static info.archinnov.achilles.internals.strategy.field_filtering.FieldFilter.EXPLICIT_UDT_FIELD_FILTER;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;

import org.junit.Before;
import org.junit.Test;
import org.truth0.Truth;

import com.datastax.driver.core.UDTValue;
import com.google.common.collect.Sets;
import com.google.testing.compile.JavaSourcesSubjectFactory;
import com.squareup.javapoet.ClassName;

import info.archinnov.achilles.exception.AchillesTranscodingException;
import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.cassandra_version.V3_6;
import info.archinnov.achilles.internals.cassandra_version.V3_7;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.config.TestCodecRegistry;
import info.archinnov.achilles.internals.sample_classes.config.TestCodecRegistry2;
import info.archinnov.achilles.internals.sample_classes.config.TestCodecRegistryWrong;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForSASI;
import info.archinnov.achilles.internals.strategy.naming.InternalNamingStrategy;
import info.archinnov.achilles.internals.strategy.naming.LowerCaseNaming;
import info.archinnov.achilles.internals.strategy.naming.SnakeCaseNaming;
import info.archinnov.achilles.type.codec.Codec;
import info.archinnov.achilles.type.strategy.InsertStrategy;

public class FieldParserTest extends AbstractTestProcessor {

    private static final String TUPLE_VALUE_CLASSNAME = "com.datastax.driver.core.TupleValue";
    private final InternalNamingStrategy strategy = new SnakeCaseNaming();
    private final GlobalParsingContext globalParsingContext = GlobalParsingContext.defaultContext();

    @Before
    public void setUp() {
        super.testEntityClass = TestEntityForCodecs.class;
    }

    @Test
    public void should_parse_primitive_boolean() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private boolean primitiveBoolean
            VariableElement elm = findFieldInType(typeElement, "primitiveBoolean");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("boolean");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_primitive_boolean.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_object_boolean() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private boolean objectBoolean
            VariableElement elm = findFieldInType(typeElement, "objectBoolean");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(Boolean.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_object_boolean.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_primitive_byte_array() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private byte[] primitiveByteArray;
            VariableElement elm = findFieldInType(typeElement, "primitiveByteArray");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(java.nio.ByteBuffer.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_primitive_byte_array.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_object_byte_array() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Byte[] objectByteArray;
            VariableElement elm = findFieldInType(typeElement, "objectByteArray");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(java.nio.ByteBuffer.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_object_byte_array.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_enumerated_type() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Enumerated(value = NAME) private ConsistencyLevel consistencyLevel
            VariableElement elm = findFieldInType(typeElement, "consistencyLevel");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_enumerated_type.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_json_type() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private @JSON Date time
            VariableElement elm = findFieldInType(typeElement, "time");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_json_type.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_string_type() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private String value
            VariableElement elm = findFieldInType(typeElement, "value");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_string_type.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_transformed_type() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Codec(IntToStringCodec.class) private Integer okInteger;
            VariableElement elm = findFieldInType(typeElement, "okInteger");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_transformed_type.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_enumerated_set_type() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Set<@Enumerated(value = ORDINAL) Double> okSet
            VariableElement elm = findFieldInType(typeElement, "okSet");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Set<java.lang.Integer>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_enumerated_set_type.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_json_map() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @JSON private Map<@JSON Integer, List<Integer>> jsonMap;
            VariableElement elm = findFieldInType(typeElement, "jsonMap");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.String");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_json_map.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_map_with_nested_json() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Map<Integer, @JSON List<Map<Integer, String>>> mapWithNestedJson;
            VariableElement elm = findFieldInType(typeElement, "mapWithNestedJson");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Map<java.lang.Integer, java.lang.String>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_map_with_nested_json.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_list_nesting() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private List<Map<Integer,String>> listNesting;
            VariableElement elm = findFieldInType(typeElement, "listNesting");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.List<java.util.Map<java.lang.Integer, java.lang.String>>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_list_nesting.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_set_nesting() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Set<Map<Integer,String>> setNesting;
            VariableElement elm = findFieldInType(typeElement, "setNesting");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Set<java.util.Map<java.lang.Integer, java.lang.String>>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_set_nesting.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_map_nesting() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Map<Integer,List<String>> mapNesting
            VariableElement elm = findFieldInType(typeElement, "mapNesting");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Map<java.lang.Integer, java.util.List<java.lang.String>>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_map_nesting.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple_nesting() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Tuple2<Integer, List<String>> tupleNesting;
            VariableElement elm = findFieldInType(typeElement, "tupleNesting");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple_nesting.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_nested_tuple() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Map<Integer, Tuple2<Integer, String>> nestedTuple;
            VariableElement elm = findFieldInType(typeElement, "nestedTuple");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Map<java.lang.Integer, com.datastax.driver.core.TupleValue>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_nested_tuple.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_complex_nested_map() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /**
             *  @EmptyCollectionIfNull
             *  private Map<@JSON TestUDT,
             *              @EmptyCollectionIfNull Map<Integer,
             *                                          Tuple3<@Codec(IntToStringCodec.class) Integer,
             *                                                  Integer,
             *                                                  @Enumerated(value = ORDINAL) ConsistencyLevel>>> map;
             */
            VariableElement elm = findFieldInType(typeElement, "map");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Map<java.lang.String, java.util.Map<java.lang.Integer, com.datastax.driver.core.TupleValue>>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_complex_nested_map.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple1() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Tuple1<@JSON ConsistencyLevel> tuple1;
            VariableElement elm = findFieldInType(typeElement, "tuple1");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple1.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple2() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Tuple2<@JSON ConsistencyLevel, @Codec(IntToStringCodec.class) Integer> tuple2;
            VariableElement elm = findFieldInType(typeElement, "tuple2");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple2.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple3() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             private Tuple3<@JSON ConsistencyLevel,@Codec(IntToStringCodec.class) Integer,
                    Integer> tuple3;
             */
            VariableElement elm = findFieldInType(typeElement, "tuple3");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple3.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple4() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             private Tuple4<@JSON ConsistencyLevel,@Codec(IntToStringCodec.class) Integer,
                    Integer, Integer> tuple4;
             */
            VariableElement elm = findFieldInType(typeElement, "tuple4");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple4.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple5() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             private Tuple5<@JSON ConsistencyLevel,@Codec(IntToStringCodec.class) Integer,
                    Integer, Integer, Integer> tuple5;
             */
            VariableElement elm = findFieldInType(typeElement, "tuple5");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple5.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple6() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             private Tuple6<@JSON ConsistencyLevel,@Codec(IntToStringCodec.class) Integer,
                    Integer, Integer, Integer, Integer> tuple6;
             */
            VariableElement elm = findFieldInType(typeElement, "tuple6");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple6.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple7() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             private Tuple7<@JSON ConsistencyLevel,@Codec(IntToStringCodec.class) Integer,
                    Integer, Integer, Integer, Integer, Integer> tuple7;
             */
            VariableElement elm = findFieldInType(typeElement, "tuple7");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple7.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple8() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             private Tuple8<@JSON ConsistencyLevel,@Codec(IntToStringCodec.class) Integer,
                    Integer, Integer, Integer, Integer, Integer, Integer> tuple8;
             */
            VariableElement elm = findFieldInType(typeElement, "tuple8");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple8.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple9() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             private Tuple9<@JSON ConsistencyLevel,@Codec(IntToStringCodec.class) Integer,
                    Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple9;
             */
            VariableElement elm = findFieldInType(typeElement, "tuple9");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple9.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_tuple10() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             private Tuple10<@JSON ConsistencyLevel,@Codec(IntToStringCodec.class) Integer,
                    Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple10;
             */
            VariableElement elm = findFieldInType(typeElement, "tuple10");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(TUPLE_VALUE_CLASSNAME);
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_tuple10.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_udt() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private TestUDT simpleUdt;
            VariableElement elm = findFieldInType(typeElement, "simpleUdt");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(UDTValue.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_udt.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_nested_udt() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private TestNestedUDT nestedUDT;
            VariableElement elm = findFieldInType(typeElement, "nestedUDT");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(UDTValue.class.getCanonicalName());
            assertThat(parsingResult.udtMetaSignature.isPresent()).isTrue();
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_nested_udt.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_list_udt() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private List<TestUDT> listUdt;
            VariableElement elm = findFieldInType(typeElement, "listUdt");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.List<com.datastax.driver.core.UDTValue>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_list_udt.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_set_udt() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Set<TestUDT> setUdt;
            VariableElement elm = findFieldInType(typeElement, "setUdt");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Set<com.datastax.driver.core.UDTValue>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_set_udt.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_map_udt() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Map<Integer, TestUDT> mapUdt;
            VariableElement elm = findFieldInType(typeElement, "mapUdt");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Map<java.lang.Integer, com.datastax.driver.core.UDTValue>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_map_udt.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_map_of_double_array() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // private Map<@JSON Integer, double[]> mapOfDoubleArray;
            VariableElement elm = findFieldInType(typeElement, "mapOfDoubleArray");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.Map<java.lang.String, double[]>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_map_of_double_array.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_nested_int_array() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            //  private List<@Frozen Map<@Enumerated ProtocolVersion, List<int[]>>> nestedArrays;
            VariableElement elm = findFieldInType(typeElement, "nestedArrays");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.List<java.util.Map<java.lang.String, java.util.List<int[]>>>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_nested_int_array.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_computed_field() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Computed(function = "writetime",  alias = "writetime", targettargetColumnsap"}, cqlClass = Long.class)
            // private Long writeTime;
            VariableElement elm = findFieldInType(typeElement, "writeTime");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.Long");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_computed_field.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_computed_field_with_codec() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Computed(function = "writetime",  alias = "writetime", targettargetColumnsap"}, cqlClass = String.class)
            // @info.archinnov.achilles.annotations.Codec(IntToStringCodec.class)
            // private Integer writeTimeAsInt;
            VariableElement elm = findFieldInType(typeElement, "writeTimeAsInt");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.String");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_computed_field_with_codec.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_runtime_codec() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            //@RuntimeCodec(codecName = "protocol_version", cqlClass = String.class)
            //@Column
            //private ProtocolVersion runtimeCodec;
            VariableElement elm = findFieldInType(typeElement, "runtimeCodec");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.String");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_runtime_codec.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_simple_codec_from_registry() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            //@Column
            //private SimpleLongWrapper longWrapper;
            VariableElement elm = findFieldInType(typeElement, "longWrapper");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.Long");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_simple_codec_from_registry.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class), loadClass(TestCodecRegistry.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_parse_json_codec_from_registry() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            //@Column
            //private MyBean myBean;
            VariableElement elm = findFieldInType(typeElement, "myBean");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.String");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_json_codec_from_registry.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class), loadClass(TestCodecRegistry.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_parse_enumerated_codec_from_registry() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            //@Column
            //private ProtocolVersion protocolVersion;
            VariableElement elm = findFieldInType(typeElement, "protocolVersion");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.String");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_enumerated_codec_from_registry.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class), loadClass(TestCodecRegistry.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_parse_codec_from_registry() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            //@Column
            //private Enumerated.Encoding encoding;
            VariableElement elm = findFieldInType(typeElement, "encoding");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.Integer");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_runtime_codec_from_registry.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class),
                        loadClass(TestCodecRegistry.class), loadClass(TestCodecRegistry2.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_parse_codec_overriding_registry() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            // @Column
            // @Enumerated(Enumerated.Encoding.ORDINAL)
            // private ProtocolVersion protocolVersionAsOrdinal;
            VariableElement elm = findFieldInType(typeElement, "protocolVersionAsOrdinal");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.Integer");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_codec_overriding_registry.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class),
                        loadClass(TestCodecRegistry.class), loadClass(TestCodecRegistry2.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_parse_optional_string() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            // @Column
            // private Optional<String> optionalString;
            VariableElement elm = findFieldInType(typeElement, "optionalString");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.String");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_optional_string.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class),
                        loadClass(TestCodecRegistry.class), loadClass(TestCodecRegistry2.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_parse_optional_protocol_version_from_codec_registry() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            // @Column
            // private Optional<ProtocolVersion> optionalProtocolVersion;
            VariableElement elm = findFieldInType(typeElement, "optionalProtocolVersion");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.String");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_optional_protocol_version_from_codec_registry.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class),
                        loadClass(TestCodecRegistry.class), loadClass(TestCodecRegistry2.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_parse_optional_protocol_version_from_inline_codec() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            // @Column
            // private Optional<@Enumerated(Encoding.ORDINAL) ProtocolVersion> optionalEncodingAsOrdinal;
            VariableElement elm = findFieldInType(typeElement, "optionalEncodingAsOrdinal");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.lang.Integer");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_optional_protocol_version_from_inline_codec.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class),
                        loadClass(TestCodecRegistry.class), loadClass(TestCodecRegistry2.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_parse_list_optional_string() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy,
                    globalContext);

            // @Column
            // private List<Optional<String>> listOfOptional;
            VariableElement elm = findFieldInType(typeElement, "listOfOptional");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo("java.util.List<java.lang.String>");
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_list_optional_string.txt"));
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class),
                        loadClass(TestCodecRegistry.class), loadClass(TestCodecRegistry2.class)))
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_fail_parsing_codec_from_registry() throws Exception {

        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = globalParsingContext;
            new CodecRegistryParser(aptUtils).parseCodecs(env, globalContext);
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestCodecRegistryWrong.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("There is already a codec for source type info." +
                        "archinnov.achilles.internals.sample_classes.types.SimpleLongWrapper " +
                        "in the class " +
                        "info.archinnov.achilles.internals.sample_classes.config.TestCodecRegistryWrong");
    }

    @Test
    public void should_parse_field_with_case_sensitive_overriden_name() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Column("\"overRiden\"")
            // private String overridenName;
            VariableElement elm = findFieldInType(typeElement, "overridenName");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_field_with_case_sensitive_overriden_name.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_jdk8_instant() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Column
            // private Instant jdkInstant;
            VariableElement elm = findFieldInType(typeElement, "jdkInstant");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(Instant.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_jdk8_instant.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_jdk8_local_date() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Column
            // private java.time.LocalDate jdkLocalDate;
            VariableElement elm = findFieldInType(typeElement, "jdkLocalDate");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(LocalDate.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_jdk8_local_date.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_jdk8_local_time() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Column
            // private java.time.LocalTime jdkLocalTime;
            VariableElement elm = findFieldInType(typeElement, "jdkLocalTime");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(LocalTime.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_jdk8_local_time.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_jdk8_zoned_date_time() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            // @Column
            // private ZonedDateTime jdkZonedDateTime;
            VariableElement elm = findFieldInType(typeElement, "jdkZonedDateTime");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(ZonedDateTime.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_jdk8_zoned_date_time.txt"));
        });
        launchTest();
    }

    @Test
    public void should_parse_non_frozen_udt() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_6.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // private TestUDT nonFrozenUDT;
            VariableElement elm = findFieldInType(typeElement, "nonFrozenUDT");

            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(UDTValue.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_non_frozen_udt.txt"));
        });
        launchTest();
    }

    @Test
    public void should_fail_parsing_non_frozen_nested_udt() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_6.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // private TestNonFrozenNestedUDT nonFrozenNestedUDT;
            VariableElement elm = findFieldInType(typeElement, "nonFrozenNestedUDT");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestCodecRegistryWrong.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("Nested udt type TestUDT of field TestEntityForCodecs.nonFrozenNestedUDT.udt should has @Frozen annotation");
    }

    @Test
    public void should_fail_parsing_counter_in_udt() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_6.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // private TestUDTWithCounter udtWithCounter;
            VariableElement elm = findFieldInType(typeElement, "udtWithCounter");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestCodecRegistryWrong.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("Counter column count is not allowed inside UDT type TestEntityForCodecs");
    }

    @Test
    public void should_fail_parsing_partition_key_in_udt() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_6.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // private TestUDTWithPartitionKey udtWithPartitionKey;
            VariableElement elm = findFieldInType(typeElement, "udtWithPartitionKey");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestCodecRegistryWrong.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("Partition key column count is not allowed inside UDT type TestEntityForCodecs");
    }

    @Test
    public void should_fail_parsing_clustering_column_in_udt() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_6.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // private TestUDTWithClusteringColumn udtWithClusteringColumn;
            VariableElement elm = findFieldInType(typeElement, "udtWithClusteringColumn");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestCodecRegistryWrong.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("Clustering column count is not allowed inside UDT type TestEntityForCodecs");
    }

    @Test
    public void should_fail_parsing_static_column_in_udt() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_6.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // private TestUDTWithStaticColumn udtWithStaticColumn;
            VariableElement elm = findFieldInType(typeElement, "udtWithStaticColumn");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestCodecRegistryWrong.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("Static column count is not allowed inside UDT type TestEntityForCodecs");
    }

    @Test
    public void should_fail_parsing_non_frozen_collection_in_udt() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_6.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // private TestUDTWithNonFrozenCollection udtWithNonFrozenCollection;
            VariableElement elm = findFieldInType(typeElement, "udtWithNonFrozenCollection");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestCodecRegistryWrong.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("Collection type List of field TestEntityForCodecs.udtWithNonFrozenCollection.li should has @Frozen annotation because TestEntityForCodecs.udtWithNonFrozenCollection is a non-frozen UDT");
    }

    @Test
    public void should_fail_parsing_SASI_analyzed_but_not_string() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = true)
            // private Long analyzedNotString;
            VariableElement elm = findFieldInType(typeElement, "analyzedNotString");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The target type java.lang.Long of field analyzedNotString from entity " +
                        "info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs is not text/ascii " +
                        "so @SASI option 'analyzed' should be false AND 'analyzerClass' should be NO_OP_ANALYZER");
    }


    @Test
    public void should_fail_parsing_SASI_analyzed_but_SPARSE() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = true, indexMode = SPARSE)
            // private String analyzedSparse;
            VariableElement elm = findFieldInType(typeElement, "analyzedSparse");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'indexMode' for field analyzedSparse from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be SPARSE because @SASI option 'analyzed' = true");
    }

    @Test
    public void should_fail_parsing_SASI_analyzed_but_NoOpAnalyzer() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = true, analyzerClass = NO_OP_ANALYZER)
            // private String analyzedNoOpAnalyzer
            VariableElement elm = findFieldInType(typeElement, "analyzedNoOpAnalyzer");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'analyzerClass' for field analyzedNoOpAnalyzer from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be NO_OP_ANALYZER because @SASI option 'analyzed' = true");
    }

    @Test
    public void should_fail_parsing_SASI_standardAnalyzer_Not_String() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = true, analyzerClass = STANDARD_ANALYZER)
            // private Long standardAnalyzerNotString
            VariableElement elm = findFieldInType(typeElement, "standardAnalyzerNotString");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The target type java.lang.Long of field standardAnalyzerNotString from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs is not text/ascii so @SASI option 'analyzed' should be false AND 'analyzerClass' should be NO_OP_ANALYZER");
    }

    @Test
    public void should_fail_parsing_SASI_standardAnalyzer_But_SPARSE() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = false, analyzerClass = STANDARD_ANALYZER, indexMode = SPARSE)
            // private String standardAnalyzerSparse
            VariableElement elm = findFieldInType(typeElement, "standardAnalyzerSparse");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'indexMode' for field standardAnalyzerSparse from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be SPARSE because @SASI option 'analyzerClass' = STANDARD_ANALYZER");
    }

    @Test
    public void should_fail_parsing_SASI_standardAnalyzer_But_NotAnalyzed() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = false, analyzerClass = STANDARD_ANALYZER)
            // private String standardAnalyzerNotAnalyzed
            VariableElement elm = findFieldInType(typeElement, "standardAnalyzerNotAnalyzed");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'analyzed' for field standardAnalyzerNotAnalyzed from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be false because @SASI option 'analyzerClass' = STANDARD_ANALYZER");
    }

    @Test
    public void should_fail_parsing_SASI_lowercase_But_Not_String() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = true, analyzerClass = NON_TOKENIZING_ANALYZER, normalization = LOWERCASE)
            // private Long normalizationNotString
            VariableElement elm = findFieldInType(typeElement, "normalizationNotString");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The target type java.lang.Long of field normalizationNotString from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs is not text/ascii so @SASI option 'analyzed' should be false AND 'analyzerClass' should be NO_OP_ANALYZER");
    }

    @Test
    public void should_fail_parsing_SASI_lowercase_But_SPARSE() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = true, analyzerClass = NON_TOKENIZING_ANALYZER, normalization = LOWERCASE, indexMode = SPARSE)
            // private String normalizationSparse
            VariableElement elm = findFieldInType(typeElement, "normalizationSparse");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'indexMode' for field normalizationSparse from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be SPARSE because @SASI option 'normalization' = LOWERCASE");
    }

    @Test
    public void should_fail_parsing_SASI_lowercase_But_NotAnalyzed() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = false, analyzerClass = NON_TOKENIZING_ANALYZER, normalization = LOWERCASE)
            // private String normalizationNotAnalyzed
            VariableElement elm = findFieldInType(typeElement, "normalizationNotAnalyzed");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'analyzed' for field normalizationNotAnalyzed from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be false because @SASI option 'normalization' = LOWERCASE");
    }

    @Test
    public void should_fail_parsing_SASI_lowercase_But_NoAnalyzer() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(analyzed = true, analyzerClass = NO_OP_ANALYZER, normalization = LOWERCASE)
            // private String normalizationNoAnalyzer
            VariableElement elm = findFieldInType(typeElement, "normalizationNoAnalyzer");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'analyzerClass' for field normalizationNoAnalyzer from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be NO_OP_ANALYZER because @SASI option 'analyzed' = true");
    }

    @Test
    public void should_fail_parsing_SASI_stemming_But_NotString() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(enableStemming = true)
            // private Long stemmingNotString
            VariableElement elm = findFieldInType(typeElement, "stemmingNotString");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The target type java.lang.Long of field stemmingNotString from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs should be text/ascii because @SASI options 'enableStemming'/'skipStopWords' are true");
    }

    @Test
    public void should_fail_parsing_SASI_stemming_But_SPARSE() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(enableStemming = true, indexMode = SPARSE)
            // private String stemmingSparse
            VariableElement elm = findFieldInType(typeElement, "stemmingSparse");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'indexMode' for field stemmingSparse from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be SPARSE because @SASI options 'enableStemming'/'skipStopWords' are true");
    }

    @Test
    public void should_fail_parsing_SASI_stemming_But_NotAnalyzed() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(enableStemming = true, analyzed = false)
            // private String stemmingNotAnalyzed
            VariableElement elm = findFieldInType(typeElement, "stemmingNotAnalyzed");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'analyzed' for field stemmingNotAnalyzed from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs cannot be false because @SASI options 'enableStemming'/'skipStopWords' are true");
    }

    @Test
    public void should_fail_parsing_SASI_stemming_But_NonTokeninzingAnalyzer() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(enableStemming = true, analyzed = true, analyzerClass = NON_TOKENIZING_ANALYZER)
            // private String stemmingNonTokenizingAnalyzer
            VariableElement elm = findFieldInType(typeElement, "stemmingNonTokenizingAnalyzer");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI option 'analyzerClass' for field stemmingNonTokenizingAnalyzer from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs should be STANDARD_ANALYZER because @SASI options 'enableStemming'/'skipStopWords' are true");
    }

    @Test
    public void should_fail_parsing_SASI_SPARSE_But_String() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI(indexMode = SPARSE)
            // private String sparsedButString
            VariableElement elm = findFieldInType(typeElement, "sparsedButString");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The @SASI 'indexMode' SPARSE is incompatible with data type java.lang.String for field sparsedButString of entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs");
    }

    @Test
    public void should_fail_parsing_SASI_on_list() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI
            // private List<String> indexedList
            VariableElement elm = findFieldInType(typeElement, "indexedList");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The target type java.util.List<java.lang.String> of field indexedList from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs is a collection (list/set/map). @SASI is not allowed because collections are not (yet) supported");
    }

    @Test
    public void should_fail_parsing_SASI_on_set() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI
            // private Set<String> indexedSet
            VariableElement elm = findFieldInType(typeElement, "indexedSet");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The target type java.util.Set<java.lang.String> of field indexedSet from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs is a collection (list/set/map). @SASI is not allowed because collections are not (yet) supported");
    }

    @Test
    public void should_fail_parsing_SASI_on_map() throws Exception {
        setExec(aptUtils -> {
            final GlobalParsingContext globalContext = new GlobalParsingContext(V3_7.INSTANCE, InsertStrategy.ALL_FIELDS, new LowerCaseNaming(),
                    EXPLICIT_ENTITY_FIELD_FILTER, EXPLICIT_UDT_FIELD_FILTER, Optional.empty());
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForSASI.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalContext);

            // @Column
            // @SASI
            // private Map<Integer, String> indexedMap
            VariableElement elm = findFieldInType(typeElement, "indexedMap");

            fieldParser.parse(elm, entityContext);

        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForSASI.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("The target type java.util.Map<java.lang.Integer, java.lang.String> of field indexedMap from entity info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs is a collection (list/set/map). @SASI is not allowed because collections are not (yet) supported");
    }

    @Test
    public void should_parse_timeuuid() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             * @TimeUUID
             * @Column
             * private UUID timeuuid;
             */
            VariableElement elm = findFieldInType(typeElement, "timeuuid");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(UUID.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_timeuuid.txt"));
        });
        launchTest();
    }

    @Test
    public void should_fail_parsing_wrongtimeuuid() throws Exception {

        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             * @TimeUUID
             * @Column
             * private String wrongtimeuuid;
             */
            VariableElement elm = findFieldInType(typeElement, "wrongtimeuuid");
            fieldParser.parse(elm, entityContext);
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("Incorrect use @TimeUUID annotation on field wrongtimeuuid of class info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs because its type is not java.util.UUID");
    }

    @Test
    public void should_parse_ascii() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             * @ASCII
             * @Column
             * private String ascii;
             */
            VariableElement elm = findFieldInType(typeElement, "ascii");
            FieldMetaSignature parsingResult = fieldParser.parse(elm, entityContext);

            assertThat(parsingResult.targetType.toString()).isEqualTo(String.class.getCanonicalName());
            assertThat(parsingResult.buildPropertyAsField().toString().trim().replaceAll("\n", ""))
                    .isEqualTo(readCodeLineFromFile("expected_code/field_parser/should_parse_ascii.txt"));
        });
        launchTest();
    }

    @Test
    public void should_fail_parsing_wrongascii() throws Exception {
        setExec(aptUtils -> {
            final FieldParser fieldParser = new FieldParser(aptUtils);
            final String className = TestEntityForCodecs.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            final EntityParsingContext entityContext = new EntityParsingContext(typeElement, ClassName.get(TestEntityForCodecs.class), strategy, globalParsingContext);

            /*
             * @ASCII
             * @Column
             * private Integer wrongascii;
             */
            VariableElement elm = findFieldInType(typeElement, "wrongascii");
            fieldParser.parse(elm, entityContext);
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Sets.newHashSet(loadClass(TestEntityForCodecs.class)))
                .processedWith(this)
                .failsToCompile()
                .withErrorContaining("Incorrect use @ASCII annotation on field wrongascii of class info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForCodecs because its type is not java.lang.String");
    }

    public static class MyCodec implements Codec<List<String>, String>, Serializable {

        @Override
        public Class<List<String>> sourceType() {
            return null;
        }

        @Override
        public Class<String> targetType() {
            return null;
        }

        @Override
        public String encode(List<String> fromJava) throws AchillesTranscodingException {
            return null;
        }

        @Override
        public List<String> decode(String fromCassandra) throws AchillesTranscodingException {
            return null;
        }
    }

}