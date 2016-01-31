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

import static com.google.auto.common.MoreTypes.isTypeOf;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.Set;
import javax.lang.model.element.TypeElement;
import javax.lang.model.element.VariableElement;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;

import org.eclipse.jdt.internal.compiler.tool.EclipseCompiler;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;
import org.truth0.Truth;

import com.google.testing.compile.JavaSourceSubjectFactory;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.apt_utils.AptAssertOK;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.parser.entity.TestEntityWithComplexTypes;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForAnnotationTree;
import jdk.nashorn.internal.ir.annotations.Ignore;

@RunWith(MockitoJUnitRunner.class)
public class EntityParserTest extends AbstractTestProcessor {

    @Test
    public void should_generate_meta_signature_for_complex_types_javac() throws Exception {
        //Given
        AptAssertOK exec = aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithComplexTypes.class.getCanonicalName());
                final EntityMetaCodeGen.EntityMetaSignature metaSignature = parser.parseEntity(typeElement, new GlobalParsingContext());

                assertThat(metaSignature).isNotNull();
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        };

        setExec(exec);

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_generate_meta_signature_for_complex_types_ecj() throws Exception {
        //Given
        AptAssertOK exec = aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithComplexTypes.class.getCanonicalName());
                final EntityMetaCodeGen.EntityMetaSignature metaSignature = parser.parseEntity(typeElement, new GlobalParsingContext());

                assertThat(metaSignature).isNotNull();
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        };

        setExec(exec);

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }
}