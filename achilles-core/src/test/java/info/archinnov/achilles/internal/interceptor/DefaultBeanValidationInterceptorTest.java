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

package info.archinnov.achilles.internal.interceptor;

import static com.google.common.collect.ImmutableMap.of;
import static info.archinnov.achilles.interceptor.Event.*;
import static org.fest.assertions.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.google.common.collect.ImmutableMap;
import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;
import info.archinnov.achilles.test.mapping.entity.CompleteBean;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.validation.ConstraintViolation;
import javax.validation.ElementKind;
import javax.validation.Path;
import javax.validation.Validator;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.Sets;

@RunWith(MockitoJUnitRunner.class)
public class DefaultBeanValidationInterceptorTest {

	@Mock
	private Validator validator;

	@Mock(answer = Answers.RETURNS_DEEP_STUBS)
	private ConstraintViolation<CompleteBean> violation;

    @Mock(answer = Answers.RETURNS_DEEP_STUBS)
    private EntityProxifier proxifier;

	private DefaultBeanValidationInterceptor interceptor;

	@Before
	public void setUp() {
		interceptor = new DefaultBeanValidationInterceptor(validator);
        interceptor.proxifier = proxifier;
    }

	@Test
	public void should_list_intercepted_events() throws Exception {
		assertThat(interceptor.events()).containsExactly(PRE_INSERT, PRE_UPDATE);
	}

	@Test
	public void should_validate_entity() throws Exception {
		// Given
		CompleteBean entity = new CompleteBean();
		when(validator.validate(entity)).thenReturn(new HashSet<ConstraintViolation<CompleteBean>>());

		// When
		interceptor.onEvent(entity);
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void should_raise_exception_when_bean_validation_error_for_property() throws Exception {
		// Given
		boolean exceptionRaised = false;
		CompleteBean entity = new CompleteBean();
        when(proxifier.getRealObject(entity)).thenReturn(entity);
        Path propertyPath = mock(Path.class,RETURNS_DEEP_STUBS);
        Path.Node node = mock(Path.Node.class);

        final List<Path.Node> nodes = Arrays.asList(node);
        when(validator.validate(entity)).thenReturn(Sets.newHashSet(violation));
		when(violation.getLeafBean().getClass().getCanonicalName()).thenReturn("className");
		when(violation.getPropertyPath()).thenReturn(propertyPath);
		when(propertyPath.toString()).thenReturn("property");
        when(propertyPath.iterator()).thenReturn(nodes.iterator());
        when(node.getKind()).thenReturn(ElementKind.PROPERTY);
		when(violation.getMessage()).thenReturn("violation");

		try {
			// When
			interceptor.onEvent(entity);
		} catch (AchillesBeanValidationException ex) {
			// Then
			assertThat(ex.getMessage()).isEqualTo(
					"Bean validation error : \n\tproperty 'property' of class 'java.lang.String' violation\n");
			exceptionRaised = true;
		}

		assertThat(exceptionRaised).isTrue();
	}

    @Test
    public void should_validate_only_dirty_fields_on_proxy() throws Exception {
        //Given
        boolean exceptionRaised = false;

        CompleteBean entity = new CompleteBean();
        when(proxifier.isProxy(entity)).thenReturn(true);
        Method method = Object.class.getDeclaredMethod("toString");
        DirtyChecker dirtyChecker = mock(DirtyChecker.class, RETURNS_DEEP_STUBS);
        when(dirtyChecker.getPropertyMeta().getPropertyName()).thenReturn("field");
        when(proxifier.getInterceptor(entity).getDirtyMap()).thenReturn(of(method, dirtyChecker));
        when(proxifier.getRealObject(entity)).thenReturn(entity);

        when(validator.validate(entity)).thenReturn(Sets.newHashSet(violation));

        Path propertyPath = mock(Path.class,RETURNS_DEEP_STUBS);
        Path.Node node = mock(Path.Node.class);
        when(violation.getLeafBean().getClass().getCanonicalName()).thenReturn("className");
        when(violation.getPropertyPath()).thenReturn(propertyPath);
        when(propertyPath.toString()).thenReturn("property");
        final List<Path.Node> nodes = Arrays.asList(node);
        when(propertyPath.iterator()).thenReturn(nodes.iterator());
        when(node.getKind()).thenReturn(ElementKind.BEAN);
        when(violation.getMessage()).thenReturn("violation");

        try {
            // When
            interceptor.onEvent(entity);
        } catch (AchillesBeanValidationException ex) {
            // Then
            assertThat(ex.getMessage()).isEqualTo(
                    "Bean validation error : \n\tproperty 'property' of class 'java.lang.String' violation\n");
            exceptionRaised = true;
        }

        assertThat(exceptionRaised).isTrue();
    }
}
