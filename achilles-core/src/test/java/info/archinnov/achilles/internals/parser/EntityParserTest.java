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

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import javax.lang.model.element.TypeElement;
import javax.tools.Diagnostic;

import org.junit.Ignore;
import org.junit.Test;
import org.truth0.Truth;

import com.google.testing.compile.JavaSourceSubjectFactory;
import com.google.testing.compile.JavaSourcesSubjectFactory;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.codegen.meta.EntityMetaCodeGen;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.parser.entity.*;
import info.archinnov.achilles.internals.sample_classes.parser.field.TestEntityForAnnotationTree;
import info.archinnov.achilles.internals.sample_classes.parser.view.TestViewSensorByType;

public class EntityParserTest extends AbstractTestProcessor {

    private final GlobalParsingContext globalParsingContext = GlobalParsingContext.defaultContext();

    @Test
    public void should_generate_meta_signature_for_complex_types_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithComplexTypes.class.getCanonicalName());
                final EntityMetaCodeGen.EntityMetaSignature metaSignature = parser.parseEntity(typeElement, globalParsingContext);

                assertThat(metaSignature).isNotNull();
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_generate_meta_signature_for_view_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestViewSensorByType.class.getCanonicalName());
                final EntityMetaCodeGen.EntityMetaSignature metaSignature = parser.parseView(typeElement, globalParsingContext);

                assertThat(metaSignature).isNotNull();
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Arrays.asList(loadClass(TestViewSensorByType.class), loadClass(TestEntitySensor.class)))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_generate_meta_signature_for_immutable_entity_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestImmutableEntity.class.getCanonicalName());
                final EntityMetaCodeGen.EntityMetaSignature metaSignature = parser.parseEntity(typeElement, globalParsingContext);

                assertThat(metaSignature).isNotNull();
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Arrays.asList(loadClass(TestImmutableUDT.class), loadClass(TestImmutableEntity.class)))
                .processedWith(this)
                .compilesWithoutError();

    }

    @Test
    public void should_fail_because_no_matching_field_name_for_constructor_param_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithNoMatchingFieldForParamInConstructor.class.getCanonicalName());
                parser.parseEntity(typeElement, globalParsingContext);
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        failTestWithMessage("Cannot find matching field name for parameter 'myValue' of @EntityCreator constructor on entity 'TestEntityWithNoMatchingFieldForParamInConstructor'");
    }

    @Test
    public void should_fail_because_no_matching_field_name_for_declared_constructor_param_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithNoMatchingFieldForDeclaredParamInConstructor.class.getCanonicalName());
                parser.parseEntity(typeElement, globalParsingContext);
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        failTestWithMessage("Cannot find matching field name for declared field 'my_value' on @EntityCreator annotation on entity 'TestEntityWithNoMatchingFieldForDeclaredParamInConstructor'");
    }

    @Test
    public void should_fail_because_no_matching_field_name_for_immutable_constructor_param_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestImmutableEntityWithNoMatchingFieldForParamInConstructor.class.getCanonicalName());
                parser.parseEntity(typeElement, globalParsingContext);
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        failTestWithMessage("Cannot find matching field name for parameter 'partitionKey' of constructor on @Immutable entity 'TestImmutableEntityWithNoMatchingFieldForParamInConstructor'");
    }

    @Test
    public void should_fail_because_incorrect_field_type_for_constructor_param_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithWrongParamTypeInConstructor.class.getCanonicalName());
                parser.parseEntity(typeElement, globalParsingContext);
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        failTestWithMessage("The type of parameter 'value' of @EntityCreator constructor on entity 'TestEntityWithWrongParamTypeInConstructor' is wrong, it should be 'java.lang.String'");
    }

    @Test
    public void should_fail_because_incorrect_field_type_for_declared_constructor_param_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithWrongDeclaredParamTypeInConstructor.class.getCanonicalName());
                parser.parseEntity(typeElement, globalParsingContext);
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        failTestWithMessage("The type of declared parameter 'value' on @EntityCreator annotation of entity 'TestEntityWithWrongDeclaredParamTypeInConstructor' is wrong, it should be 'java.lang.String'");
    }

    @Test
    public void should_fail_because_incorrect_field_type_for_immutable_constructor_param_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestImmutableEntityWithWrongParamTypeInConstructor.class.getCanonicalName());
                parser.parseEntity(typeElement, globalParsingContext);
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        failTestWithMessage("The type of parameter 'partition' of constructor on @Immutable entity 'TestImmutableEntityWithWrongParamTypeInConstructor' is wrong, it should be 'java.lang.Long'");
    }

    @Test
    public void should_fail_because_not_public_final_field_type_for_immutable_constructor_param_javac() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestImmutableEntityWithNonPublicFinalField.class.getCanonicalName());
                parser.parseEntity(typeElement, globalParsingContext);
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        failTestWithMessage("Field 'partition' in entity 'TestImmutableEntityWithNonPublicFinalField' should have 'public final' modifier because it is an @Immutable entity");
    }

    @Test
    @Ignore
    public void should_generate_meta_signature_for_view_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestViewSensorByType.class.getCanonicalName());
                final EntityMetaCodeGen.EntityMetaSignature metaSignature = parser.parseView(typeElement, globalParsingContext);

                assertThat(metaSignature).isNotNull();
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourcesSubjectFactory.javaSources())
                .that(Arrays.asList(loadClass(TestViewSensorByType.class), loadClass(TestEntitySensor.class)))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }

    @Ignore
    @Test
    public void should_generate_meta_signature_for_complex_types_ecj() throws Exception {
        //Given
        setExec(aptUtils -> {
            try {
                final EntityParser parser = new EntityParser(aptUtils);
                final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithComplexTypes.class.getCanonicalName());
                final EntityMetaCodeGen.EntityMetaSignature metaSignature = parser.parseEntity(typeElement, globalParsingContext);

                assertThat(metaSignature).isNotNull();
            } catch (Exception ex) {
                ex.printStackTrace();
                aptUtils.messager.printMessage(Diagnostic.Kind.ERROR, ex.getMessage());
            }
        });

        Truth.ASSERT.about(JavaSourceSubjectFactory.javaSource())
                .that(loadClass(TestEntityForAnnotationTree.class))
//                .withCompiler(new EclipseCompiler())
//                .withCompilerOptions("-nowarn", "-1.8")
                .processedWith(this)
                .compilesWithoutError();

    }
}