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

package info.archinnov.achilles.internals.parser.validator;

import static info.archinnov.achilles.internals.parser.context.ConstructorInfo.ConstructorType.ENTITY_CREATOR;
import static info.archinnov.achilles.internals.parser.context.ConstructorInfo.ConstructorType.IMMUTABLE;
import static org.assertj.core.api.Assertions.assertThat;

import javax.lang.model.element.TypeElement;

import org.junit.Test;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.parser.context.ConstructorInfo;
import info.archinnov.achilles.internals.sample_classes.parser.validator.*;

public class BeanValidatorTest extends AbstractTestProcessor {

    private BeanValidator beanValidator = new BeanValidator() {};


    @Test
    public void should_fail_validating_an_interface() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityAsInterface.class.getCanonicalName());
            beanValidator.validateIsAConcreteClass(aptUtils, typeElement);
        });
        failTestWithMessage("Bean type 'info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityAsInterface' should be a class",
                TestEntityAsInterface.class);
    }


    @Test
    public void should_fail_validating_an_abstract_class() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityAsAbstract.class.getCanonicalName());
            beanValidator.validateIsAConcreteClass(aptUtils, typeElement);
        });
        failTestWithMessage("Bean type 'info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityAsAbstract' should not be abstract",
                TestEntityAsAbstract.class);
    }

    @Test
    public void should_fail_validating_class_without_default_constructor() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithNoPublicConstructor.class.getCanonicalName());
            final TypeName typeName = ClassName.get(TestEntityWithNoPublicConstructor.class);
            beanValidator.validateConstructor(aptUtils, typeName, typeElement);
        });
        failTestWithMessage("Entity type 'info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityWithNoPublicConstructor'", TestEntityWithNoPublicConstructor.class);
    }

    @Test
    public void should_fail_validating_class_with_two_custom_constructors() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithNoPublicConstructor.class.getCanonicalName());
            final TypeName typeName = ClassName.get(TestEntityWithTwoCustomConstructors.class);
            beanValidator.validateConstructor(aptUtils, typeName, typeElement);
        });
        failTestWithMessage("Entity type 'info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityWithTwoCustomConstructors' should", TestEntityWithTwoCustomConstructors.class);
    }

    @Test
    public void should_validate_immutable_entity() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestImmutableEntity.class.getCanonicalName());
            final TypeName typeName = ClassName.get(TestImmutableEntity.class);
            final ConstructorInfo constructorInfo = beanValidator.validateConstructor(aptUtils, typeName, typeElement);

            assertThat(constructorInfo).isNotNull();
            assertThat(constructorInfo.type).isSameAs(IMMUTABLE);
        });
        launchTest();
    }

    @Test
    public void should_validate_entity_with_EntityCreator_constructor() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithEntityCreatorConstructor.class.getCanonicalName());
            final TypeName typeName = ClassName.get(TestEntityWithEntityCreatorConstructor.class);
            final ConstructorInfo constructorInfo = beanValidator.validateConstructor(aptUtils, typeName, typeElement);

            assertThat(constructorInfo).isNotNull();
            assertThat(constructorInfo.type).isSameAs(ENTITY_CREATOR);
        });
        launchTest();
    }
}