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

import static info.archinnov.achilles.internals.parser.TypeUtils.*;
import static org.assertj.core.api.Assertions.*;

import java.util.List;
import javax.lang.model.element.TypeElement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.metamodel.functions.UDFSignature;
import info.archinnov.achilles.internals.sample_classes.functions.*;
import info.archinnov.achilles.internals.utils.CollectionsHelper;

@RunWith(MockitoJUnitRunner.class)
public class UDFParserTest extends AbstractTestProcessor {

    @Test
    public void should_parse_function() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistry.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final List<UDFSignature> UDFSignatures = UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);

            /**
             * String toString(int val);
             * String toString(long val);
             * String noArg();
             */

            assertThat(UDFSignatures).hasSize(3);
            final UDFSignature signature1 = UDFSignatures.get(0);
            assertThat(signature1.keyspace.isPresent()).isFalse();
            assertThat(signature1.name).isEqualTo("toString");
            assertThat(signature1.returnType).isEqualTo(STRING);
            assertThat(signature1.parameterTypes).containsExactly(NATIVE_INT);

            final UDFSignature signature2 = UDFSignatures.get(1);
            assertThat(signature2.keyspace.isPresent()).isFalse();
            assertThat(signature2.name).isEqualTo("toString");
            assertThat(signature2.returnType).isEqualTo(STRING);
            assertThat(signature2.parameterTypes).containsExactly(NATIVE_LONG);

            final UDFSignature signature3 = UDFSignatures.get(2);
            assertThat(signature3.keyspace.isPresent()).isFalse();
            assertThat(signature3.name).isEqualTo("noArg");
            assertThat(signature3.returnType).isEqualTo(STRING);
            assertThat(signature3.parameterTypes).isNotNull().isEmpty();
        });
        launchTest();
    }

    @Test
    public void should_parse_function_with_keyspace() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithKeyspaceName.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final List<UDFSignature> UDFSignatures = UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);

            /**
             * long toLong(String val);
             * int toInt(String val);
             */

            assertThat(UDFSignatures).hasSize(2);
            final UDFSignature signature1 = UDFSignatures.get(0);
            assertThat(signature1.keyspace.get()).isEqualTo("ks");
            assertThat(signature1.name).isEqualTo("toLong");
            assertThat(signature1.returnType).isEqualTo(OBJECT_LONG);
            assertThat(signature1.parameterTypes).containsExactly(STRING);

            final UDFSignature signature2 = UDFSignatures.get(1);
            assertThat(signature2.keyspace.get()).isEqualTo("ks");
            assertThat(signature2.name).isEqualTo("toInt");
            assertThat(signature2.returnType).isEqualTo(OBJECT_INT);
            assertThat(signature2.parameterTypes).containsExactly(STRING);
        });
        launchTest();
    }

    @Test
    public void should_fail_parsing_function_with_blank_keyspace() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithBlankKeyspace.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);
        });
        failTestWithMessage("The declared keyspace for function registry 'TestFunctionRegistryWithBlankKeyspace' should not be blank");
    }

    @Test
    public void should_fail_parsing_function_with_unsupported_param_type() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithUnsupportedParamType.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);
        });
        failTestWithMessage("Type 'java.lang.Exception' in method 'unsupportedParam(java.util.List<java.lang.Exception>)' argument on class 'TestFunctionRegistryWithUnsupportedParamType' is not a valid native Java type for Cassandra");
    }

    @Test
    public void should_fail_parsing_function_with_unsupported_return_type() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithUnsupportedReturnType.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);
        });
        failTestWithMessage("Type 'java.lang.Exception' in method 'unsupportedReturn(int)' return type on class 'TestFunctionRegistryWithUnsupportedReturnType' is not a valid native Java type for Cassandra");
    }

    @Test
    public void should_fail_parsing_function_with_void_return_type() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithVoidReturn.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);
        });
        failTestWithMessage("The return type for the method 'unsupportedParam(java.lang.String)' on class 'TestFunctionRegistryWithVoidReturn' should not be VOID");
    }

    @Test
    public void should_fail_parsing_function_forbidden_keyspace_name() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithForbiddenKeyspaceName.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);
        });
        failTestWithMessage("The provided keyspace 'System' on function registry class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithForbiddenKeyspaceName' is forbidden because it is a system keyspace");
    }

    @Test
    public void should_fail_parsing_function_OpsCenter_keyspace_name() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithOpsCenterKeyspaceName.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);
        });
        failTestWithMessage("he provided keyspace '\"OpsCenter\"' on function registry class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithOpsCenterKeyspaceName' is forbidden because it is a system keyspace");
    }

    @Test
    public void should_fail_parsing_function_forbidden_function_name() throws Exception {
        setExec(aptUtils -> {
            final String className = TestFunctionRegistryWithForbiddenFunctionName.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
            UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement);
        });
        failTestWithMessage("The name of the function 'writetime(int)' in class 'info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithForbiddenFunctionName' is reserved for system functions");
    }
    
    @Test
    public void should_fail_parsing_duplicate_function_declaration() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement1 = aptUtils.elementUtils.getTypeElement(TestFunctionRegistry.class.getCanonicalName());
            final TypeElement typeElement2 = aptUtils.elementUtils.getTypeElement(TestFunctionRegistryWithDuplicateFunction.class.getCanonicalName());
            final List<UDFSignature> udfSignatures1 = UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement1);
            final List<UDFSignature> udfSignatures2 = UDFParser.parseFunctionRegistryAndValidateTypes(aptUtils, typeElement2);

            UDFParser.validateNoDuplicateDeclaration(aptUtils, CollectionsHelper.appendAll(udfSignatures1, udfSignatures2));
        });
        failTestWithMessage("Functions " +
            "'UDFSignature{keyspace=Optional.empty, sourceClass=info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistry, " +
                "methodName='toString', name='toString', returnType=java.lang.String, parameterTypes=[int]}' and " +
            "'UDFSignature{keyspace=Optional.empty, sourceClass=info.archinnov.achilles.internals.sample_classes.functions.TestFunctionRegistryWithDuplicateFunction, " +
                "methodName='toString', name='toString', returnType=java.lang.String, parameterTypes=[int]}' " +
            "have same signature. Duplicate function declaration is not allowed");
    
    }
}