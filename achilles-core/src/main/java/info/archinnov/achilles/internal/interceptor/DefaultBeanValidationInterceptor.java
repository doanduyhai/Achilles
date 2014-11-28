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

import static info.archinnov.achilles.interceptor.Event.*;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;
import info.archinnov.achilles.internal.persistence.operations.EntityProxifier;
import info.archinnov.achilles.internal.proxy.ProxyInterceptor;
import info.archinnov.achilles.internal.proxy.dirtycheck.DirtyChecker;

import java.lang.reflect.Method;
import java.util.*;

import javax.validation.ConstraintViolation;
import javax.validation.ElementKind;
import javax.validation.Path;
import javax.validation.Validator;



public class DefaultBeanValidationInterceptor implements AchillesInternalInterceptor<Object> {

    private static final Set<String> ALWAYS_TRUE_CONTAINS_SET = new AlwaysTrueContainsSet();

    private static final Function<DirtyChecker,String> DIRTY_CHECKER_TO_FIELD_NAME = new Function<DirtyChecker, String>() {
        @Override
        public String apply(DirtyChecker dirtyChecker) {
            return dirtyChecker.getPropertyMeta().getPropertyName();
        }
    };

	private Validator validator;
    protected EntityProxifier proxifier = EntityProxifier.Singleton.INSTANCE.get();


    public DefaultBeanValidationInterceptor(Validator validator) {
        this.validator = validator;
	}

	@Override
	public void onEvent(Object entity) {
        final Set<String> dirtyFieldNames = getDirtyFiedNames(entity);
        final Object realObject = proxifier.getRealObject(entity);
        Set<ConstraintViolation<Object>> violations = validator.validate(realObject);
        boolean raiseError = false;
        if (violations.size() > 0) {
            StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
            for (ConstraintViolation<Object> violation : violations) {
                raiseError |= buildValidationErrorMessage(errorMessage, violation, dirtyFieldNames);
            }
            if (raiseError) {
                throw new AchillesBeanValidationException(errorMessage.toString());
            }
        }
    }

    private Set<String> getDirtyFiedNames(Object entity) {
        if (proxifier.isProxy(entity)) {
            final Map<Method, DirtyChecker> dirtyMap = proxifier.getInterceptor(entity).getDirtyMap();
            return FluentIterable.from(dirtyMap.values()).transform(DIRTY_CHECKER_TO_FIELD_NAME).toSet();
        } else {
            return ALWAYS_TRUE_CONTAINS_SET;
        }

    }

    private boolean buildValidationErrorMessage(StringBuilder errorMessage, ConstraintViolation<Object> violation, Set<String> dirtyFieldNames) {
		String className = violation.getLeafBean().getClass().getCanonicalName();
		Path propertyPath = violation.getPropertyPath();
        final ElementKind elementKind = propertyPath.iterator().next().getKind();
        final String propertyName = propertyPath.toString();

        boolean raiseError = false;

        /**
         * Throw error only when
         *   1. it's a proxy and the dirty field names do not validate the constraints
         *   2. it's a proxy and the validation is not on the fields so we cannot filter out null fields
         *   3. it's a transient entity (dirtyFieldNames = ALWAYS_TRUE_CONTAINS_SET)
         */
        if (dirtyFieldNames.contains(propertyName) || elementKind != ElementKind.PROPERTY) {
            errorMessage.append("\t");
            raiseError = true;
            if (propertyPath != null && isNotBlank(propertyPath.toString())) {
                errorMessage.append("property '").append(propertyPath).append("'") //
                        .append(" of class '").append(className).append("' ") //
                        .append(violation.getMessage()).append("\n");
            } else {
                errorMessage.append(violation.getMessage()).append(" for class '").append(className).append("'");
            }
        }

        return raiseError;
    }

	@Override
	public List<Event> events() {
		return asList(PRE_INSERT, PRE_UPDATE);
	}


    private static class AlwaysTrueContainsSet implements Set<String> {

        @Override
        public int size() {
            return 0;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return true;
        }

        @Override
        public Iterator<String> iterator() {
            return null;
        }

        @Override
        public Object[] toArray() {
            return new Object[0];
        }

        @Override
        public <T> T[] toArray(T[] a) {
            return null;
        }

        @Override
        public boolean add(String s) {
            return false;
        }

        @Override
        public boolean remove(Object o) {
            return false;
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean addAll(Collection<? extends String> c) {
            return false;
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            return false;
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            return false;
        }

        @Override
        public void clear() {

        }
    }
}
