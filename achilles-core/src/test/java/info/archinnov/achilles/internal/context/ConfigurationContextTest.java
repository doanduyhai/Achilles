/*
 * Copyright (C) 2012-2014 DuyHai DOAN
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package info.archinnov.achilles.internal.context;

import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.when;
import javax.validation.Validator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import com.fasterxml.jackson.databind.ObjectMapper;
import info.archinnov.achilles.json.JacksonMapperFactory;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;
import info.archinnov.achilles.test.parser.entity.BeanWithFieldLevelConstraint;

@RunWith(MockitoJUnitRunner.class)
public class ConfigurationContextTest {

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private Validator validator;

    @Mock
    private JacksonMapperFactory factory;

    @Test
    public void should_detect_constrained_class() throws Exception {
        // Given
        ConfigurationContext context = new ConfigurationContext();
        context.setBeanValidator(validator);

        // When
        when(validator.getConstraintsForClass(BeanWithFieldLevelConstraint.class).isBeanConstrained()).thenReturn(true);

        // Then
        assertThat(context.isClassConstrained(BeanWithFieldLevelConstraint.class)).isTrue();
    }

    @Test
    public void should_not_detect_constrained_class_if_bean_validation_disabled() throws Exception {
        // Given
        ConfigurationContext context = new ConfigurationContext();

        // Then
        assertThat(context.isClassConstrained(BeanWithFieldLevelConstraint.class)).isFalse();

    }

    @Test
    public void should_get_mapper_for_type() throws Exception {
        //Given
        ConfigurationContext context = new ConfigurationContext();
        context.setJacksonMapperFactory(factory);
        ObjectMapper mapper = new ObjectMapper();

        when(factory.getMapper(String.class)).thenReturn(mapper);

        //When
        final ObjectMapper actual = context.getMapperFor(String.class);

        //Then
        assertThat(actual).isSameAs(mapper);
    }

    @Test
    public void should_select_classloader_from_entity() throws Exception {
        //Given
        ConfigurationContext context = new ConfigurationContext();

        //When
        final ClassLoader classLoader = context.selectClassLoader(CompleteBean.class);

        //Then
        assertThat(classLoader).isSameAs(this.getClass().getClassLoader());
    }

    @Test
    public void should_select_classloader_from_osgi() throws Exception {
        //Given
        ConfigurationContext context = new ConfigurationContext();
        final ClassLoader osgiClassLoader = new ClassLoader() {
            @Override
            public Class<?> loadClass(String name) throws ClassNotFoundException {
                return super.loadClass(name);
            }
        };
        context.setOSGIClassLoader(osgiClassLoader);

        //When
        final ClassLoader classLoader = context.selectClassLoader(CompleteBean.class);

        //Then
        assertThat(classLoader).isSameAs(osgiClassLoader);
    }

    @Test
    public void should_select_classloader_from_current() throws Exception {
        //Given
        ConfigurationContext context = new ConfigurationContext();

        //When
        final ClassLoader actual = context.selectClassLoader();

        //Then
        assertThat(actual).isSameAs(this.getClass().getClassLoader());
    }
}
