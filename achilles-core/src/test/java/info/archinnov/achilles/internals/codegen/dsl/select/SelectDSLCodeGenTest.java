/*
 * Copyright (C) 2012-2015 DuyHai DOAN
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

package info.archinnov.achilles.internals.codegen.dsl.select;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.codegen.TypeParsingResultConsumer;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;


@RunWith(MockitoJUnitRunner.class)
public class SelectDSLCodeGenTest extends AbstractTestProcessor implements TypeParsingResultConsumer {

    private GlobalParsingContext context = new GlobalParsingContext();

    @Test
    public void should_generate_select_for_entity_with_complex_types() throws Exception {
//        setExec(aptUtils -> {
//            final String className = TestEntityWithComplexTypes.class.getCanonicalName();
//            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(className);
//
//            new EntityMetaBuilder.EntityMetaSignature(null, className, )
//            final SelectDSLBuilder builder = new SelectDSLBuilder(aptUtils, typeElement);
//            final List<FieldParser.TypeParsingResult> parsingResults = getTypeParsingResults(aptUtils, typeElement, context);
//
//            final TypeSpec actual = builder.buildSelectColumnMethod(parsingResults);
//
//            assertThat(actual.toString().trim()).isEqualTo(readCodeBlockFromFile(
//                    "expected_code/select_dsl/should_generate_select_for_entity_with_complex_types.txt"));
//
//        });
//        launchTest(TestEntityWithComplexTypes.class);
    }

}