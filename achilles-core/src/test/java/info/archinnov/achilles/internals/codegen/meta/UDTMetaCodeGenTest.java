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

package info.archinnov.achilles.internals.codegen.meta;

import static info.archinnov.achilles.internals.codegen.TypeParsingResultConsumer.getTypeParsingResults;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.lang.model.element.TypeElement;

import org.junit.Test;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.codegen.TypeParsingResultConsumer;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.context.AccessorsExclusionContext;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDTWithCustomConstructor;
import info.archinnov.achilles.internals.strategy.naming.LowerCaseNaming;

public class UDTMetaCodeGenTest extends AbstractTestProcessor
        implements TypeParsingResultConsumer {

    @Test
    public void should_generate_udt_property_class() throws Exception {
        setExec(aptUtils -> {
            final String className = TestUDT.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final UDTMetaCodeGen builder = new UDTMetaCodeGen(aptUtils);

            final GlobalParsingContext globalContext = GlobalParsingContext.defaultContext();
            final EntityParsingContext context = new EntityParsingContext(typeElement,
                    ClassName.get(TestUDT.class), new LowerCaseNaming(), globalContext);
            final List<FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, globalContext);

            final TypeSpec typeSpec = builder.buildUDTClassProperty(typeElement, context, parsingResults, Collections.emptyList());

            assertThat(typeSpec.toString().trim()).isEqualTo(
                    readCodeBlockFromFile("expected_code/udt_meta_builder/should_generate_udt_property_class.txt"));

        });
        launchTest(TestUDT.class);
    }

    @Test
    public void should_generate_udt_with_custom_constructor_property_class() throws Exception {
        setExec(aptUtils -> {
            final String className = TestUDTWithCustomConstructor.class.getCanonicalName();
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);

            final UDTMetaCodeGen builder = new UDTMetaCodeGen(aptUtils);

            final GlobalParsingContext globalContext = GlobalParsingContext.defaultContext();
            final EntityParsingContext context = new EntityParsingContext(typeElement,
                    ClassName.get(TestUDT.class), new LowerCaseNaming(), globalContext);
            final List<AccessorsExclusionContext> exclusionContexts = Arrays.asList(
                    new AccessorsExclusionContext("name", false, true),
                    new AccessorsExclusionContext("list", false, true));
            final List<FieldMetaSignature> fieldMetaSignatures = getTypeParsingResults(aptUtils, typeElement, exclusionContexts, globalContext);

            final List<FieldMetaSignature> constructorInjectedFieldMetaSignatures = fieldMetaSignatures
                    .stream()
                    .filter(fieldMeta -> !fieldMeta.context.fieldName.equals("date"))
                    .collect(Collectors.toList());

            final TypeSpec typeSpec = builder.buildUDTClassProperty(typeElement, context, fieldMetaSignatures, constructorInjectedFieldMetaSignatures);

            assertThat(typeSpec.toString().trim()).isEqualTo(
                    readCodeBlockFromFile("expected_code/udt_meta_builder/should_generate_udt_with_custom_constructor_property_class.txt"));

        });
        launchTest(TestUDTWithCustomConstructor.class);
    }
}