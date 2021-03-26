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

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import javax.lang.model.element.TypeElement;

import org.junit.Ignore;
import org.junit.Test;
import org.truth0.Truth;

import com.google.testing.compile.JavaSourceSubjectFactory;
import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.apt_utils.AptAssertOK;
import info.archinnov.achilles.internals.parser.context.FunctionSignature;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.functions.*;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;
import info.archinnov.achilles.internals.utils.CollectionsHelper;

public class FunctionParserTest extends AbstractTestProcessor {

    private final GlobalParsingContext context = GlobalParsingContext.defaultContext();

    @Test
    public void should_parse_function() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistry.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final List<FunctionSignature> UDFSignatures = FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);

            /**
             * String toString(int val);
             * String toString(long val);
             * String noArg();
             */

            assertThat(UDFSignatures).hasSize(3);
            final FunctionSignature signature1 = UDFSignatures.get(0);
            assertThat(signature1.keyspace.isPresent()).isFalse();
            assertThat(signature1.name).isEqualTo("toString");
            assertThat(signature1.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(signature1.sourceParameterTypes).containsExactly(TypeName.INT);

            final FunctionSignature signature2 = UDFSignatures.get(1);
            assertThat(signature2.keyspace.isPresent()).isFalse();
            assertThat(signature2.name).isEqualTo("toString");
            assertThat(signature2.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(signature2.sourceParameterTypes).containsExactly(TypeName.LONG);

            final FunctionSignature signature3 = UDFSignatures.get(2);
            assertThat(signature3.keyspace.isPresent()).isFalse();
            assertThat(signature3.name).isEqualTo("noArg");
            assertThat(signature3.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(signature3.sourceParameterTypes).isNotNull().isEmpty();
        });
        launchTest();
    }

    @Test
    public void should_parse_function_with_keyspace() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithKeyspaceName.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final List<FunctionSignature> UDFSignatures = FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);

            /**
             * Long toLong(String val);
             * Integer toInt(String val);
             */

            assertThat(UDFSignatures).hasSize(2);
            final FunctionSignature signature1 = UDFSignatures.get(0);
            assertThat(signature1.keyspace.get()).isEqualTo("ks");
            assertThat(signature1.name).isEqualTo("toLong");
            assertThat(signature1.returnTypeSignature.targetCQLTypeName).isEqualTo(OBJECT_LONG);
            assertThat(signature1.sourceParameterTypes).containsExactly(STRING);

            final FunctionSignature signature2 = UDFSignatures.get(1);
            assertThat(signature2.keyspace.get()).isEqualTo("ks");
            assertThat(signature2.name).isEqualTo("toInt");
            assertThat(signature2.returnTypeSignature.targetCQLTypeName).isEqualTo(OBJECT_INT);
            assertThat(signature2.sourceParameterTypes).containsExactly(STRING);
        });
        launchTest();
    }

    @Test
    public void should_parse_functions_with_complex_types_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            final ClassName testUDTType = ClassName.get(TestUDT.class);
            final String className = TestFunctionRegistryWithComplexTypes.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final List<FunctionSignature> udfSignatures = FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);

            assertThat(udfSignatures).hasSize(24);
            final FunctionSignature enumeratedParam = udfSignatures.get(0);
            assertThat(enumeratedParam.getFunctionName()).isEqualTo("enumeratedParam");
            assertThat(enumeratedParam.parameterSignatures.get(0).sourceTypeName).isEqualTo(CONSISTENCY_LEVEL);
            assertThat(enumeratedParam.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(OBJECT_INT);
            assertThat(enumeratedParam.parameterSignatures.get(0).targetCQLDataType).isEqualTo("int");
            assertThat(enumeratedParam.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(enumeratedParam.returnTypeSignature.targetCQLDataType).isEqualTo("text");

            final FunctionSignature json = udfSignatures.get(1);
            assertThat(json.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_UTIL_DATE);
            assertThat(json.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(STRING);
            assertThat(json.parameterSignatures.get(0).targetCQLDataType).isEqualTo("text");

            final FunctionSignature primitiveByteArray = udfSignatures.get(2);
            assertThat(primitiveByteArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(byte[].class));
            assertThat(primitiveByteArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(BYTE_BUFFER);
            assertThat(primitiveByteArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("blob");

            final FunctionSignature objectByteArray = udfSignatures.get(3);
            assertThat(objectByteArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(Byte[].class));
            assertThat(objectByteArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(BYTE_BUFFER);
            assertThat(objectByteArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("blob");

            final FunctionSignature intToStringCodec = udfSignatures.get(4);
            assertThat(intToStringCodec.parameterSignatures.get(0).sourceTypeName).isEqualTo(OBJECT_INT);
            assertThat(intToStringCodec.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(STRING);
            assertThat(intToStringCodec.parameterSignatures.get(0).targetCQLDataType).isEqualTo("text");

            final FunctionSignature udf = udfSignatures.get(5);

            assertThat(udf.parameterSignatures.get(0).sourceTypeName).isEqualTo(testUDTType);
            assertThat(udf.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_DRIVER_UDT_VALUE_TYPE);
            assertThat(udf.parameterSignatures.get(0).targetCQLDataType).isEqualTo("frozen<my_type>");

            final FunctionSignature listUDT = udfSignatures.get(6);
            assertThat(listUDT.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(LIST, testUDTType));
            assertThat(listUDT.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(LIST, JAVA_DRIVER_UDT_VALUE_TYPE));
            assertThat(listUDT.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<frozen<my_type>>");

            final FunctionSignature mapUDT = udfSignatures.get(7);
            assertThat(mapUDT.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(MAP, OBJECT_INT, testUDTType));
            assertThat(mapUDT.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(MAP, OBJECT_INT, JAVA_DRIVER_UDT_VALUE_TYPE));
            assertThat(mapUDT.parameterSignatures.get(0).targetCQLDataType).isEqualTo("map<int, frozen<my_type>>");

            final FunctionSignature setEnum = udfSignatures.get(8);
            assertThat(setEnum.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(SET, CONSISTENCY_LEVEL));
            assertThat(setEnum.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(SET, STRING));
            assertThat(setEnum.parameterSignatures.get(0).targetCQLDataType).isEqualTo("set<text>");

            final FunctionSignature listOfMap = udfSignatures.get(9);
            assertThat(listOfMap.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(LIST, genericType(MAP, OBJECT_INT, STRING)));
            assertThat(listOfMap.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(LIST, genericType(MAP, OBJECT_INT, STRING)));
            assertThat(listOfMap.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<frozen<map<int, text>>>");

            final FunctionSignature tuple1 = udfSignatures.get(10);
            assertThat(tuple1.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(TUPLE1, CONSISTENCY_LEVEL));
            assertThat(tuple1.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_DRIVER_TUPLE_VALUE_TYPE);
            assertThat(tuple1.parameterSignatures.get(0).targetCQLDataType).isEqualTo("frozen<tuple<text>>");

            final FunctionSignature tuple2 = udfSignatures.get(11);
            assertThat(tuple2.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(TUPLE2, OBJECT_INT, genericType(LIST, OBJECT_INT)));
            assertThat(tuple2.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_DRIVER_TUPLE_VALUE_TYPE);
            assertThat(tuple2.parameterSignatures.get(0).targetCQLDataType).isEqualTo("frozen<tuple<int, list<text>>>");

            final FunctionSignature complicated = udfSignatures.get(12);
            assertThat(complicated.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(MAP, testUDTType, genericType(MAP, OBJECT_INT, genericType(TUPLE3, OBJECT_INT, OBJECT_INT, CONSISTENCY_LEVEL))));
            assertThat(complicated.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(MAP, STRING, genericType(MAP, OBJECT_INT, JAVA_DRIVER_TUPLE_VALUE_TYPE)));
            assertThat(complicated.parameterSignatures.get(0).targetCQLDataType).isEqualTo("map<text, frozen<map<int, frozen<tuple<text, int, int>>>>>");

            final FunctionSignature timeuuid = udfSignatures.get(13);
            assertThat(timeuuid.parameterSignatures.get(0).sourceTypeName).isEqualTo(UUID);
            assertThat(timeuuid.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(UUID);
            assertThat(timeuuid.parameterSignatures.get(0).targetCQLDataType).isEqualTo("timeuuid");

            final FunctionSignature longArray = udfSignatures.get(14);
            assertThat(longArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(long[].class));
            assertThat(longArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(TypeName.get(long[].class));
            assertThat(longArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<bigint>");

            final FunctionSignature intArray = udfSignatures.get(15);
            assertThat(intArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(int[].class));
            assertThat(intArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(TypeName.get(int[].class));
            assertThat(intArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<int>");

            final FunctionSignature doubleArray = udfSignatures.get(16);
            assertThat(doubleArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(double[].class));
            assertThat(doubleArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(TypeName.get(double[].class));
            assertThat(doubleArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<double>");

            final FunctionSignature floatArray = udfSignatures.get(17);
            assertThat(floatArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(float[].class));
            assertThat(floatArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(TypeName.get(float[].class));
            assertThat(floatArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<float>");

            final FunctionSignature localDate = udfSignatures.get(18);
            assertThat(localDate.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_DRIVER_LOCAL_DATE);
            assertThat(localDate.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_DRIVER_LOCAL_DATE);
            assertThat(localDate.parameterSignatures.get(0).targetCQLDataType).isEqualTo("date");

            final FunctionSignature jdkInstant = udfSignatures.get(19);
            assertThat(jdkInstant.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_TIME_INSTANT);
            assertThat(jdkInstant.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_TIME_INSTANT);
            assertThat(jdkInstant.parameterSignatures.get(0).targetCQLDataType).isEqualTo("timestamp");

            final FunctionSignature jdkLocalDate = udfSignatures.get(20);
            assertThat(jdkLocalDate.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_TIME_LOCAL_DATE);
            assertThat(jdkLocalDate.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_TIME_LOCAL_DATE);
            assertThat(jdkLocalDate.parameterSignatures.get(0).targetCQLDataType).isEqualTo("date");

            final FunctionSignature jdkLocalTime = udfSignatures.get(21);
            assertThat(jdkLocalTime.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_TIME_LOCAL_TIME);
            assertThat(jdkLocalTime.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_TIME_LOCAL_TIME);
            assertThat(jdkLocalTime.parameterSignatures.get(0).targetCQLDataType).isEqualTo("time");

            final FunctionSignature jdkZonedDateTime = udfSignatures.get(22);
            assertThat(jdkZonedDateTime.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_TIME_ZONED_DATE_TME);
            assertThat(jdkZonedDateTime.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_TIME_ZONED_DATE_TME);
            assertThat(jdkZonedDateTime.parameterSignatures.get(0).targetCQLDataType).isEqualTo("tuple<timestamp, varchar>");

            final FunctionSignature jdkOptional = udfSignatures.get(23);
            assertThat(jdkOptional.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(OPTIONAL, STRING));
            assertThat(jdkOptional.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(STRING);
            assertThat(jdkOptional.parameterSignatures.get(0).targetCQLDataType).isEqualTo("text");

        });
        launchTest();

    }

    @Ignore
    @Test
    public void should_parse_functions_with_complex_types_ecj() throws Exception {
        /**
         * Eclipse compiler orders method by their name:
         *
         * 0 complicated
         * 1 doubleArray
         * 2 enumeratedParam
         * 3 floatArray
         * 4 intArray
         * 5 intToStringCodec
         * 6 jdkInstant
         * 7 jdkLocalDate
         * 8 jdkLocalTime
         * 9 jdkOptional
         * 10 jdkZonedDateTime
         * 11 json
         * 12 listOfMap
         * 13 listUDT
         * 14 localDate
         * 15 longArray
         * 16 mapUDT
         * 17 objectByteArray
         * 18 primitiveByteArray
         * 19 setEnum
         * 20 timeuuid
         * 21 tuple1
         * 22 tuple2
         * 23 udf
         */
        //Given
        AptAssertOK exec = aptUtils -> {
            final ClassName testUDTType = ClassName.get(TestUDT.class);
            final String className = TestFunctionRegistryWithComplexTypes.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final List<FunctionSignature> udfSignatures = FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);

            assertThat(udfSignatures).hasSize(24);
            final FunctionSignature enumeratedParam = udfSignatures.get(2);
            assertThat(enumeratedParam.getFunctionName()).isEqualTo("enumeratedParam");
            assertThat(enumeratedParam.parameterSignatures.get(0).sourceTypeName).isEqualTo(CONSISTENCY_LEVEL);
            assertThat(enumeratedParam.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(OBJECT_INT);
            assertThat(enumeratedParam.parameterSignatures.get(0).targetCQLDataType).isEqualTo("int");
            assertThat(enumeratedParam.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(enumeratedParam.returnTypeSignature.targetCQLDataType).isEqualTo("text");

            final FunctionSignature json = udfSignatures.get(11);
            assertThat(json.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_UTIL_DATE);
            assertThat(json.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(STRING);
            assertThat(json.parameterSignatures.get(0).targetCQLDataType).isEqualTo("text");

            final FunctionSignature primitiveByteArray = udfSignatures.get(18);
            assertThat(primitiveByteArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(byte[].class));
            assertThat(primitiveByteArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(BYTE_BUFFER);
            assertThat(primitiveByteArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("blob");

            final FunctionSignature objectByteArray = udfSignatures.get(17);
            assertThat(objectByteArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(Byte[].class));
            assertThat(objectByteArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(BYTE_BUFFER);
            assertThat(objectByteArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("blob");

            final FunctionSignature intToStringCodec = udfSignatures.get(5);
            assertThat(intToStringCodec.parameterSignatures.get(0).sourceTypeName).isEqualTo(OBJECT_INT);
            assertThat(intToStringCodec.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(STRING);
            assertThat(intToStringCodec.parameterSignatures.get(0).targetCQLDataType).isEqualTo("text");

            final FunctionSignature udf = udfSignatures.get(23);
            assertThat(udf.parameterSignatures.get(0).sourceTypeName).isEqualTo(testUDTType);
            assertThat(udf.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_DRIVER_UDT_VALUE_TYPE);
            assertThat(udf.parameterSignatures.get(0).targetCQLDataType).isEqualTo("frozen<my_type>");

            final FunctionSignature listUDT = udfSignatures.get(13);
            assertThat(listUDT.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(LIST, testUDTType));
            assertThat(listUDT.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(LIST, JAVA_DRIVER_UDT_VALUE_TYPE));
            assertThat(listUDT.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<frozen<my_type>>");

            final FunctionSignature mapUDT = udfSignatures.get(16);
            assertThat(mapUDT.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(MAP, OBJECT_INT, testUDTType));
            assertThat(mapUDT.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(MAP, OBJECT_INT, JAVA_DRIVER_UDT_VALUE_TYPE));
            assertThat(mapUDT.parameterSignatures.get(0).targetCQLDataType).isEqualTo("map<int, frozen<my_type>>");

            final FunctionSignature setEnum = udfSignatures.get(19);
            assertThat(setEnum.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(SET, CONSISTENCY_LEVEL));
            assertThat(setEnum.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(SET, STRING));
            assertThat(setEnum.parameterSignatures.get(0).targetCQLDataType).isEqualTo("set<text>");

            final FunctionSignature listOfMap = udfSignatures.get(12);
            assertThat(listOfMap.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(LIST, genericType(MAP, OBJECT_INT, STRING)));
            assertThat(listOfMap.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(LIST, genericType(MAP, OBJECT_INT, STRING)));
            assertThat(listOfMap.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<frozen<map<int, text>>>");

            final FunctionSignature tuple1 = udfSignatures.get(21);
            assertThat(tuple1.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(TUPLE1, CONSISTENCY_LEVEL));
            assertThat(tuple1.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_DRIVER_TUPLE_VALUE_TYPE);
            assertThat(tuple1.parameterSignatures.get(0).targetCQLDataType).isEqualTo("frozen<tuple<text>>");

            final FunctionSignature tuple2 = udfSignatures.get(22);
            assertThat(tuple2.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(TUPLE2, OBJECT_INT, genericType(LIST, OBJECT_INT)));
            assertThat(tuple2.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_DRIVER_TUPLE_VALUE_TYPE);
            assertThat(tuple2.parameterSignatures.get(0).targetCQLDataType).isEqualTo("frozen<tuple<int, list<text>>>");

            final FunctionSignature complicated = udfSignatures.get(0);
            assertThat(complicated.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(MAP, testUDTType, genericType(MAP, OBJECT_INT, genericType(TUPLE3, OBJECT_INT, OBJECT_INT, CONSISTENCY_LEVEL))));
            assertThat(complicated.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(genericType(MAP, STRING, genericType(MAP, OBJECT_INT, JAVA_DRIVER_TUPLE_VALUE_TYPE)));
            assertThat(complicated.parameterSignatures.get(0).targetCQLDataType).isEqualTo("map<text, frozen<map<int, frozen<tuple<text, int, int>>>>>");

            final FunctionSignature timeuuid = udfSignatures.get(20);
            assertThat(timeuuid.parameterSignatures.get(0).sourceTypeName).isEqualTo(UUID);
            assertThat(timeuuid.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(UUID);
            assertThat(timeuuid.parameterSignatures.get(0).targetCQLDataType).isEqualTo("timeuuid");

            final FunctionSignature longArray = udfSignatures.get(15);
            assertThat(longArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(long[].class));
            assertThat(longArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(TypeName.get(long[].class));
            assertThat(longArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<bigint>");

            final FunctionSignature intArray = udfSignatures.get(4);
            assertThat(intArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(int[].class));
            assertThat(intArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(TypeName.get(int[].class));
            assertThat(intArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<int>");

            final FunctionSignature doubleArray = udfSignatures.get(1);
            assertThat(doubleArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(double[].class));
            assertThat(doubleArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(TypeName.get(double[].class));
            assertThat(doubleArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<double>");

            final FunctionSignature floatArray = udfSignatures.get(3);
            assertThat(floatArray.parameterSignatures.get(0).sourceTypeName).isEqualTo(TypeName.get(float[].class));
            assertThat(floatArray.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(TypeName.get(float[].class));
            assertThat(floatArray.parameterSignatures.get(0).targetCQLDataType).isEqualTo("list<float>");

            final FunctionSignature localDate = udfSignatures.get(14);
            assertThat(localDate.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_DRIVER_LOCAL_DATE);
            assertThat(localDate.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_DRIVER_LOCAL_DATE);
            assertThat(localDate.parameterSignatures.get(0).targetCQLDataType).isEqualTo("date");

            final FunctionSignature jdkInstant = udfSignatures.get(6);
            assertThat(jdkInstant.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_TIME_INSTANT);
            assertThat(jdkInstant.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_TIME_INSTANT);
            assertThat(jdkInstant.parameterSignatures.get(0).targetCQLDataType).isEqualTo("timestamp");

            final FunctionSignature jdkLocalDate = udfSignatures.get(7);
            assertThat(jdkLocalDate.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_TIME_LOCAL_DATE);
            assertThat(jdkLocalDate.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_TIME_LOCAL_DATE);
            assertThat(jdkLocalDate.parameterSignatures.get(0).targetCQLDataType).isEqualTo("date");

            final FunctionSignature jdkLocalTime = udfSignatures.get(8);
            assertThat(jdkLocalTime.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_TIME_LOCAL_TIME);
            assertThat(jdkLocalTime.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_TIME_LOCAL_TIME);
            assertThat(jdkLocalTime.parameterSignatures.get(0).targetCQLDataType).isEqualTo("time");

            final FunctionSignature jdkZonedDateTime = udfSignatures.get(10);
            assertThat(jdkZonedDateTime.parameterSignatures.get(0).sourceTypeName).isEqualTo(JAVA_TIME_ZONED_DATE_TME);
            assertThat(jdkZonedDateTime.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(JAVA_TIME_ZONED_DATE_TME);
            assertThat(jdkZonedDateTime.parameterSignatures.get(0).targetCQLDataType).isEqualTo("tuple<timestamp, varchar>");

            final FunctionSignature jdkOptional = udfSignatures.get(9);
            assertThat(jdkOptional.parameterSignatures.get(0).sourceTypeName).isEqualTo(genericType(OPTIONAL, STRING));
            assertThat(jdkOptional.parameterSignatures.get(0).targetCQLTypeName).isEqualTo(STRING);
            assertThat(jdkOptional.parameterSignatures.get(0).targetCQLDataType).isEqualTo("text");

        };

        setExec(exec);

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestFunctionRegistryWithComplexTypes.class))
//                .withCompiler(new EclipseCompiler())
                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_parse_functions_with_complex_return_types_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            final ClassName testUDTType = ClassName.get(TestUDT.class);
            final String className = TestFunctionRegistryWithComplexReturnTypes.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final List<FunctionSignature> udfSignatures = FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);

            assertThat(udfSignatures).hasSize(24);
            final FunctionSignature enumeratedParam = udfSignatures.get(0);
            assertThat(enumeratedParam.getFunctionName()).isEqualTo("enumeratedParam");
            assertThat(enumeratedParam.parameterSignatures).hasSize(0);
            assertThat(enumeratedParam.returnTypeSignature.sourceTypeName).isEqualTo(CONSISTENCY_LEVEL);
            assertThat(enumeratedParam.returnTypeSignature.targetCQLTypeName).isEqualTo(OBJECT_INT);
            assertThat(enumeratedParam.returnTypeSignature.targetCQLDataType).isEqualTo("int");

            final FunctionSignature json = udfSignatures.get(1);
            assertThat(json.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_UTIL_DATE);
            assertThat(json.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(json.returnTypeSignature.targetCQLDataType).isEqualTo("text");

            final FunctionSignature primitiveByteArray = udfSignatures.get(2);
            assertThat(primitiveByteArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(byte[].class));
            assertThat(primitiveByteArray.returnTypeSignature.targetCQLTypeName).isEqualTo(BYTE_BUFFER);
            assertThat(primitiveByteArray.returnTypeSignature.targetCQLDataType).isEqualTo("blob");

            final FunctionSignature objectByteArray = udfSignatures.get(3);
            assertThat(objectByteArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(Byte[].class));
            assertThat(objectByteArray.returnTypeSignature.targetCQLTypeName).isEqualTo(BYTE_BUFFER);
            assertThat(objectByteArray.returnTypeSignature.targetCQLDataType).isEqualTo("blob");

            final FunctionSignature intToStringCodec = udfSignatures.get(4);
            assertThat(intToStringCodec.returnTypeSignature.sourceTypeName).isEqualTo(OBJECT_INT);
            assertThat(intToStringCodec.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(intToStringCodec.returnTypeSignature.targetCQLDataType).isEqualTo("text");

            final FunctionSignature udf = udfSignatures.get(5);
            assertThat(udf.returnTypeSignature.sourceTypeName).isEqualTo(testUDTType);
            assertThat(udf.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_DRIVER_UDT_VALUE_TYPE);
            assertThat(udf.returnTypeSignature.targetCQLDataType).isEqualTo("my_type");

            final FunctionSignature listUDT = udfSignatures.get(6);
            assertThat(listUDT.returnTypeSignature.sourceTypeName).isEqualTo(genericType(LIST, testUDTType));
            assertThat(listUDT.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(LIST, JAVA_DRIVER_UDT_VALUE_TYPE));
            assertThat(listUDT.returnTypeSignature.targetCQLDataType).isEqualTo("list<my_type>");

            final FunctionSignature mapUDT = udfSignatures.get(7);
            assertThat(mapUDT.returnTypeSignature.sourceTypeName).isEqualTo(genericType(MAP, OBJECT_INT, testUDTType));
            assertThat(mapUDT.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(MAP, OBJECT_INT, JAVA_DRIVER_UDT_VALUE_TYPE));
            assertThat(mapUDT.returnTypeSignature.targetCQLDataType).isEqualTo("map<int, my_type>");

            final FunctionSignature setEnum = udfSignatures.get(8);
            assertThat(setEnum.returnTypeSignature.sourceTypeName).isEqualTo(genericType(SET, CONSISTENCY_LEVEL));
            assertThat(setEnum.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(SET, STRING));
            assertThat(setEnum.returnTypeSignature.targetCQLDataType).isEqualTo("set<text>");

            final FunctionSignature listOfMap = udfSignatures.get(9);
            assertThat(listOfMap.returnTypeSignature.sourceTypeName).isEqualTo(genericType(LIST, genericType(MAP, OBJECT_INT, STRING)));
            assertThat(listOfMap.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(LIST, genericType(MAP, OBJECT_INT, STRING)));
            assertThat(listOfMap.returnTypeSignature.targetCQLDataType).isEqualTo("list<map<int, text>>");

            final FunctionSignature tuple1 = udfSignatures.get(10);
            assertThat(tuple1.returnTypeSignature.sourceTypeName).isEqualTo(genericType(TUPLE1, CONSISTENCY_LEVEL));
            assertThat(tuple1.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_DRIVER_TUPLE_VALUE_TYPE);
            assertThat(tuple1.returnTypeSignature.targetCQLDataType).isEqualTo("frozen<tuple<text>>");

            final FunctionSignature tuple2 = udfSignatures.get(11);
            assertThat(tuple2.returnTypeSignature.sourceTypeName).isEqualTo(genericType(TUPLE2, OBJECT_INT, genericType(LIST, OBJECT_INT)));
            assertThat(tuple2.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_DRIVER_TUPLE_VALUE_TYPE);
            assertThat(tuple2.returnTypeSignature.targetCQLDataType).isEqualTo("frozen<tuple<int, list<text>>>");

            final FunctionSignature complicated = udfSignatures.get(12);
            assertThat(complicated.returnTypeSignature.sourceTypeName).isEqualTo(genericType(MAP, testUDTType, genericType(MAP, OBJECT_INT, genericType(TUPLE3, OBJECT_INT, OBJECT_INT, CONSISTENCY_LEVEL))));
            assertThat(complicated.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(MAP, STRING, genericType(MAP, OBJECT_INT, JAVA_DRIVER_TUPLE_VALUE_TYPE)));
            assertThat(complicated.returnTypeSignature.targetCQLDataType).isEqualTo("map<text, map<int, frozen<tuple<text, int, int>>>>");

            final FunctionSignature timeuuid = udfSignatures.get(13);
            assertThat(timeuuid.returnTypeSignature.sourceTypeName).isEqualTo(UUID);
            assertThat(timeuuid.returnTypeSignature.targetCQLTypeName).isEqualTo(UUID);
            assertThat(timeuuid.returnTypeSignature.targetCQLDataType).isEqualTo("timeuuid");

            final FunctionSignature longArray = udfSignatures.get(14);
            assertThat(longArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(long[].class));
            assertThat(longArray.returnTypeSignature.targetCQLTypeName).isEqualTo(TypeName.get(long[].class));
            assertThat(longArray.returnTypeSignature.targetCQLDataType).isEqualTo("list<bigint>");

            final FunctionSignature intArray = udfSignatures.get(15);
            assertThat(intArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(int[].class));
            assertThat(intArray.returnTypeSignature.targetCQLTypeName).isEqualTo(TypeName.get(int[].class));
            assertThat(intArray.returnTypeSignature.targetCQLDataType).isEqualTo("list<int>");

            final FunctionSignature doubleArray = udfSignatures.get(16);
            assertThat(doubleArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(double[].class));
            assertThat(doubleArray.returnTypeSignature.targetCQLTypeName).isEqualTo(TypeName.get(double[].class));
            assertThat(doubleArray.returnTypeSignature.targetCQLDataType).isEqualTo("list<double>");

            final FunctionSignature floatArray = udfSignatures.get(17);
            assertThat(floatArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(float[].class));
            assertThat(floatArray.returnTypeSignature.targetCQLTypeName).isEqualTo(TypeName.get(float[].class));
            assertThat(floatArray.returnTypeSignature.targetCQLDataType).isEqualTo("list<float>");

            final FunctionSignature localDate = udfSignatures.get(18);
            assertThat(localDate.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_DRIVER_LOCAL_DATE);
            assertThat(localDate.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_DRIVER_LOCAL_DATE);
            assertThat(localDate.returnTypeSignature.targetCQLDataType).isEqualTo("date");

            final FunctionSignature jdkInstant = udfSignatures.get(19);
            assertThat(jdkInstant.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_TIME_INSTANT);
            assertThat(jdkInstant.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_TIME_INSTANT);
            assertThat(jdkInstant.returnTypeSignature.targetCQLDataType).isEqualTo("timestamp");

            final FunctionSignature jdkLocalDate = udfSignatures.get(20);
            assertThat(jdkLocalDate.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_TIME_LOCAL_DATE);
            assertThat(jdkLocalDate.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_TIME_LOCAL_DATE);
            assertThat(jdkLocalDate.returnTypeSignature.targetCQLDataType).isEqualTo("date");

            final FunctionSignature jdkLocalTime = udfSignatures.get(21);
            assertThat(jdkLocalTime.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_TIME_LOCAL_TIME);
            assertThat(jdkLocalTime.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_TIME_LOCAL_TIME);
            assertThat(jdkLocalTime.returnTypeSignature.targetCQLDataType).isEqualTo("time");

            final FunctionSignature jdkZonedDateTime = udfSignatures.get(22);
            assertThat(jdkZonedDateTime.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_TIME_ZONED_DATE_TME);
            assertThat(jdkZonedDateTime.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_TIME_ZONED_DATE_TME);
            assertThat(jdkZonedDateTime.returnTypeSignature.targetCQLDataType).isEqualTo("tuple<timestamp, varchar>");

            final FunctionSignature jdkOptional = udfSignatures.get(23);
            assertThat(jdkOptional.returnTypeSignature.sourceTypeName).isEqualTo(genericType(OPTIONAL, STRING));
            assertThat(jdkOptional.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(jdkOptional.returnTypeSignature.targetCQLDataType).isEqualTo("text");
        });
        launchTest();

    }

    @Ignore
    @Test
    public void should_parse_functions_with_complex_return_types_ecj() throws Exception {
        /**
         * Eclipse compiler orders method by their name:
         *
         * 0 complicated
         * 1 doubleArray
         * 2 enumeratedParam
         * 3 floatArray
         * 4 intArray
         * 5 intToStringCodec
         * 6 jdkInstant
         * 7 jdkLocalDate
         * 8 jdkLocalTime
         * 9 jdkOptional
         * 10 jdkZonedDateTime
         * 11 json
         * 12 listOfMap
         * 13 listUDT
         * 14 localDate
         * 15 longArray
         * 16 mapUDT
         * 17 objectByteArray
         * 18 primitiveByteArray
         * 19 setEnum
         * 20 timeuuid
         * 21 tuple1
         * 22 tuple2
         * 23 udf
         */
        //Given
        setExec(aptUtils -> {
            final ClassName testUDTType = ClassName.get(TestUDT.class);
            final String className = TestFunctionRegistryWithComplexReturnTypes.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final List<FunctionSignature> udfSignatures = FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);

            assertThat(udfSignatures).hasSize(24);
            final FunctionSignature enumeratedParam = udfSignatures.get(2);
            assertThat(enumeratedParam.getFunctionName()).isEqualTo("enumeratedParam");
            assertThat(enumeratedParam.parameterSignatures).hasSize(0);
            assertThat(enumeratedParam.returnTypeSignature.sourceTypeName).isEqualTo(CONSISTENCY_LEVEL);
            assertThat(enumeratedParam.returnTypeSignature.targetCQLTypeName).isEqualTo(OBJECT_INT);
            assertThat(enumeratedParam.returnTypeSignature.targetCQLDataType).isEqualTo("int");

            final FunctionSignature json = udfSignatures.get(11);
            assertThat(json.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_UTIL_DATE);
            assertThat(json.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(json.returnTypeSignature.targetCQLDataType).isEqualTo("text");

            final FunctionSignature primitiveByteArray = udfSignatures.get(18);
            assertThat(primitiveByteArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(byte[].class));
            assertThat(primitiveByteArray.returnTypeSignature.targetCQLTypeName).isEqualTo(BYTE_BUFFER);
            assertThat(primitiveByteArray.returnTypeSignature.targetCQLDataType).isEqualTo("blob");

            final FunctionSignature objectByteArray = udfSignatures.get(17);
            assertThat(objectByteArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(Byte[].class));
            assertThat(objectByteArray.returnTypeSignature.targetCQLTypeName).isEqualTo(BYTE_BUFFER);
            assertThat(objectByteArray.returnTypeSignature.targetCQLDataType).isEqualTo("blob");

            final FunctionSignature intToStringCodec = udfSignatures.get(5);
            assertThat(intToStringCodec.returnTypeSignature.sourceTypeName).isEqualTo(OBJECT_INT);
            assertThat(intToStringCodec.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(intToStringCodec.returnTypeSignature.targetCQLDataType).isEqualTo("text");

            final FunctionSignature udf = udfSignatures.get(23);
            assertThat(udf.returnTypeSignature.sourceTypeName).isEqualTo(testUDTType);
            assertThat(udf.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_DRIVER_UDT_VALUE_TYPE);
            assertThat(udf.returnTypeSignature.targetCQLDataType).isEqualTo("my_type");

            final FunctionSignature listUDT = udfSignatures.get(13);
            assertThat(listUDT.returnTypeSignature.sourceTypeName).isEqualTo(genericType(LIST, testUDTType));
            assertThat(listUDT.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(LIST, JAVA_DRIVER_UDT_VALUE_TYPE));
            assertThat(listUDT.returnTypeSignature.targetCQLDataType).isEqualTo("list<my_type>");

            final FunctionSignature mapUDT = udfSignatures.get(16);
            assertThat(mapUDT.returnTypeSignature.sourceTypeName).isEqualTo(genericType(MAP, OBJECT_INT, testUDTType));
            assertThat(mapUDT.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(MAP, OBJECT_INT, JAVA_DRIVER_UDT_VALUE_TYPE));
            assertThat(mapUDT.returnTypeSignature.targetCQLDataType).isEqualTo("map<int, my_type>");

            final FunctionSignature setEnum = udfSignatures.get(19);
            assertThat(setEnum.returnTypeSignature.sourceTypeName).isEqualTo(genericType(SET, CONSISTENCY_LEVEL));
            assertThat(setEnum.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(SET, STRING));
            assertThat(setEnum.returnTypeSignature.targetCQLDataType).isEqualTo("set<text>");

            final FunctionSignature listOfMap = udfSignatures.get(12);
            assertThat(listOfMap.returnTypeSignature.sourceTypeName).isEqualTo(genericType(LIST, genericType(MAP, OBJECT_INT, STRING)));
            assertThat(listOfMap.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(LIST, genericType(MAP, OBJECT_INT, STRING)));
            assertThat(listOfMap.returnTypeSignature.targetCQLDataType).isEqualTo("list<map<int, text>>");

            final FunctionSignature tuple1 = udfSignatures.get(21);
            assertThat(tuple1.returnTypeSignature.sourceTypeName).isEqualTo(genericType(TUPLE1, CONSISTENCY_LEVEL));
            assertThat(tuple1.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_DRIVER_TUPLE_VALUE_TYPE);
            assertThat(tuple1.returnTypeSignature.targetCQLDataType).isEqualTo("frozen<tuple<text>>");

            final FunctionSignature tuple2 = udfSignatures.get(22);
            assertThat(tuple2.returnTypeSignature.sourceTypeName).isEqualTo(genericType(TUPLE2, OBJECT_INT, genericType(LIST, OBJECT_INT)));
            assertThat(tuple2.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_DRIVER_TUPLE_VALUE_TYPE);
            assertThat(tuple2.returnTypeSignature.targetCQLDataType).isEqualTo("frozen<tuple<int, list<text>>>");

            final FunctionSignature complicated = udfSignatures.get(0);
            assertThat(complicated.returnTypeSignature.sourceTypeName).isEqualTo(genericType(MAP, testUDTType, genericType(MAP, OBJECT_INT, genericType(TUPLE3, OBJECT_INT, OBJECT_INT, CONSISTENCY_LEVEL))));
            assertThat(complicated.returnTypeSignature.targetCQLTypeName).isEqualTo(genericType(MAP, STRING, genericType(MAP, OBJECT_INT, JAVA_DRIVER_TUPLE_VALUE_TYPE)));
            assertThat(complicated.returnTypeSignature.targetCQLDataType).isEqualTo("map<text, map<int, frozen<tuple<text, int, int>>>>");

            final FunctionSignature timeuuid = udfSignatures.get(20);
            assertThat(timeuuid.returnTypeSignature.sourceTypeName).isEqualTo(UUID);
            assertThat(timeuuid.returnTypeSignature.targetCQLTypeName).isEqualTo(UUID);
            assertThat(timeuuid.returnTypeSignature.targetCQLDataType).isEqualTo("timeuuid");

            final FunctionSignature longArray = udfSignatures.get(15);
            assertThat(longArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(long[].class));
            assertThat(longArray.returnTypeSignature.targetCQLTypeName).isEqualTo(TypeName.get(long[].class));
            assertThat(longArray.returnTypeSignature.targetCQLDataType).isEqualTo("list<bigint>");

            final FunctionSignature intArray = udfSignatures.get(4);
            assertThat(intArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(int[].class));
            assertThat(intArray.returnTypeSignature.targetCQLTypeName).isEqualTo(TypeName.get(int[].class));
            assertThat(intArray.returnTypeSignature.targetCQLDataType).isEqualTo("list<int>");

            final FunctionSignature doubleArray = udfSignatures.get(1);
            assertThat(doubleArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(double[].class));
            assertThat(doubleArray.returnTypeSignature.targetCQLTypeName).isEqualTo(TypeName.get(double[].class));
            assertThat(doubleArray.returnTypeSignature.targetCQLDataType).isEqualTo("list<double>");

            final FunctionSignature floatArray = udfSignatures.get(3);
            assertThat(floatArray.returnTypeSignature.sourceTypeName).isEqualTo(TypeName.get(float[].class));
            assertThat(floatArray.returnTypeSignature.targetCQLTypeName).isEqualTo(TypeName.get(float[].class));
            assertThat(floatArray.returnTypeSignature.targetCQLDataType).isEqualTo("list<float>");

            final FunctionSignature localDate = udfSignatures.get(14);
            assertThat(localDate.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_DRIVER_LOCAL_DATE);
            assertThat(localDate.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_DRIVER_LOCAL_DATE);
            assertThat(localDate.returnTypeSignature.targetCQLDataType).isEqualTo("date");

            final FunctionSignature jdkInstant = udfSignatures.get(6);
            assertThat(jdkInstant.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_TIME_INSTANT);
            assertThat(jdkInstant.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_TIME_INSTANT);
            assertThat(jdkInstant.returnTypeSignature.targetCQLDataType).isEqualTo("timestamp");

            final FunctionSignature jdkLocalDate = udfSignatures.get(7);
            assertThat(jdkLocalDate.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_TIME_LOCAL_DATE);
            assertThat(jdkLocalDate.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_TIME_LOCAL_DATE);
            assertThat(jdkLocalDate.returnTypeSignature.targetCQLDataType).isEqualTo("date");

            final FunctionSignature jdkLocalTime = udfSignatures.get(8);
            assertThat(jdkLocalTime.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_TIME_LOCAL_TIME);
            assertThat(jdkLocalTime.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_TIME_LOCAL_TIME);
            assertThat(jdkLocalTime.returnTypeSignature.targetCQLDataType).isEqualTo("time");

            final FunctionSignature jdkZonedDateTime = udfSignatures.get(10);
            assertThat(jdkZonedDateTime.returnTypeSignature.sourceTypeName).isEqualTo(JAVA_TIME_ZONED_DATE_TME);
            assertThat(jdkZonedDateTime.returnTypeSignature.targetCQLTypeName).isEqualTo(JAVA_TIME_ZONED_DATE_TME);
            assertThat(jdkZonedDateTime.returnTypeSignature.targetCQLDataType).isEqualTo("tuple<timestamp, varchar>");

            final FunctionSignature jdkOptional = udfSignatures.get(9);
            assertThat(jdkOptional.returnTypeSignature.sourceTypeName).isEqualTo(genericType(OPTIONAL, STRING));
            assertThat(jdkOptional.returnTypeSignature.targetCQLTypeName).isEqualTo(STRING);
            assertThat(jdkOptional.returnTypeSignature.targetCQLDataType).isEqualTo("text");
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestFunctionRegistryWithComplexReturnTypes.class))
//                .withCompiler(new EclipseCompiler())
                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();
    }

    @Test
    public void should_fail_parsing_function_with_blank_keyspace() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithBlankKeyspace.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("The declared keyspace for function registry 'TestFunctionRegistryWithBlankKeyspace' should not be blank");
    }

    @Test
    public void should_fail_parsing_function_with_unsupported_param_type() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithUnsupportedParamType.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("Type 'java.lang.Exception' in method 'unsupportedParam' return type/parameter on class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithUnsupportedParamType' is not a valid native Java type for Cassandra");
    }

    @Test
    public void should_fail_parsing_function_with_unsupported_return_type() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithUnsupportedReturnType.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("Type 'java.lang.Exception' in method 'unsupportedReturn' return type/parameter on class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithUnsupportedReturnType' is not a valid native Java type for Cassandra");
    }

    @Test
    public void should_fail_parsing_function_with_void_return_type() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithVoidReturn.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("The return type for the method 'voidReturnType(java.lang.String)' on class 'TestFunctionRegistryWithVoidReturn' should not be VOID");
    }

    @Test
    public void should_fail_parsing_function_with_primitive_return_type() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithPrimitiveReturnType.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("Due to internal JDK API limitations, UDF/UDA return types cannot be primitive. Use their Object counterpart instead");
    }

    @Test
    public void should_fail_parsing_function_forbidden_keyspace_name() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithForbiddenKeyspaceName.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("The provided keyspace 'System' on function registry class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithForbiddenKeyspaceName' is forbidden because it is a system keyspace");
    }

    @Test
    public void should_fail_parsing_function_OpsCenter_keyspace_name() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithOpsCenterKeyspaceName.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("he provided keyspace '\"OpsCenter\"' on function registry class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithOpsCenterKeyspaceName' is forbidden because it is a system keyspace");
    }

    @Test
    public void should_fail_parsing_function_forbidden_function_name() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithForbiddenFunctionName.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("The name of the function 'writetime(int)' in class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithForbiddenFunctionName' is reserved for system functions");
    }
    
    @Test
    public void should_fail_parsing_duplicate_function_declaration() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement1 = aptUtils.elementUtils.getTypeElement(TestFunctionRegistry.class.getCanonicalName());
            final TypeElement typeElement2 = aptUtils.elementUtils.getTypeElement(TestFunctionRegistryWithDuplicateFunction.class.getCanonicalName());
            final List<FunctionSignature> udfSignatures1 = FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement1, context);
            final List<FunctionSignature> udfSignatures2 = FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement2, context);

            FunctionParser.validateNoDuplicateDeclaration(aptUtils, CollectionsHelper.appendAll(udfSignatures1, udfSignatures2));
        });
        failTestWithMessage("Functions 'UDFSignature{keyspace=Optional.empty, sourceClass=info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistry, methodName='toString', name='toString', returnType=java.lang.String, sourceParameterTypes=[int]}' and 'UDFSignature{keyspace=Optional.empty, sourceClass=info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithDuplicateFunction, methodName='toString', name='toString', returnType=java.lang.String, sourceParameterTypes=[int]}' have same signature. Duplicate function declaration is not allowed");
    
    }

    @Test
    public void should_fail_parsing_validating_nested_types() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithNotFrozenNestedCollection.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            FunctionParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement, context);
        });
        failTestWithMessage("Nested collections/array type/UDT 'java.util.Map<java.lang.Integer,java.lang.String>' in 'listOUnfrozenfMap(java.util.List<java.util.Map<java.lang.Integer,java.lang.String>>)' of class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithNotFrozenNestedCollection' should be annotated with @Frozen");

    }
}