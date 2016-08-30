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

package info.archinnov.achilles.internals.codegen.meta;

import static info.archinnov.achilles.internals.codegen.TypeParsingResultConsumer.getTypeParsingResults;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import javax.lang.model.element.TypeElement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeSpec;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.codegen.TypeParsingResultConsumer;
import info.archinnov.achilles.internals.parser.FieldParser;
import info.archinnov.achilles.internals.parser.context.EntityParsingContext;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestUDT;
import info.archinnov.achilles.internals.strategy.naming.LowerCaseNaming;

@RunWith(MockitoJUnitRunner.class)
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
            final List<FieldParser.FieldMetaSignature> parsingResults = getTypeParsingResults(aptUtils, typeElement, globalContext);

            final TypeSpec typeSpec = builder.buildUDTClassProperty(typeElement, context, parsingResults);

            assertThat(typeSpec.toString().trim()).isEqualTo(
                    readCodeBlockFromFile("expected_code/udt_meta_builder/should_generate_udt_property_class.txt"));

        });
        launchTest(TestUDT.class);
    }
}