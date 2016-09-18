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

package info.archinnov.achilles.internals.parser.validator;

import javax.lang.model.element.TypeElement;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.squareup.javapoet.ClassName;
import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.apt_utils.AbstractTestProcessor;
import info.archinnov.achilles.internals.parser.context.GlobalParsingContext;
import info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityAsAbstract;
import info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityAsFinal;
import info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityAsInterface;
import info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityWithNoPublicConstructor;

@RunWith(MockitoJUnitRunner.class)
public class BeanValidatorTest extends AbstractTestProcessor {

    private GlobalParsingContext context = GlobalParsingContext.defaultContext();
    private BeanValidator beanValidator = new BeanValidator() {};

    @Test
    public void should_fail_validating_an_interface() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityAsInterface.class.getCanonicalName());
            beanValidator.validateIsAConcreteNonFinalClass(aptUtils, typeElement);
        });
        failTestWithMessage("Bean type 'info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityAsInterface' should be a class",
                TestEntityAsInterface.class);
    }


    @Test
    public void should_fail_validating_an_abstract_class() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityAsAbstract.class.getCanonicalName());
            beanValidator.validateIsAConcreteNonFinalClass(aptUtils, typeElement);
        });
        failTestWithMessage("Bean type 'info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityAsAbstract' should not be abstract",
                TestEntityAsAbstract.class);
    }

    @Test
    public void should_fail_validating_a_final_class() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityAsFinal.class.getCanonicalName());
            beanValidator.validateIsAConcreteNonFinalClass(aptUtils, typeElement);
        });
        failTestWithMessage("Bean type 'info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityAsFinal' should not be final",
                TestEntityAsFinal.class);
    }

    @Test
    public void should_fail_validating_class_without_default_constructor() throws Exception {
        setExec(aptUtils -> {
            final TypeElement typeElement = aptUtils.elementUtils.getTypeElement(TestEntityWithNoPublicConstructor.class.getCanonicalName());
            final TypeName typeName = ClassName.get(TestEntityWithNoPublicConstructor.class);
            beanValidator.validateHasPublicConstructor(aptUtils, typeName, typeElement);
        });
        failTestWithMessage("Bean type 'info.archinnov.achilles.internals.sample_classes.parser.validator.TestEntityWithNoPublicConstructor' should have a public no-args constructor", TestEntityWithNoPublicConstructor.class);
    }
}