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

package info.archinnov.achilles.internals.interceptor;


import static info.archinnov.achilles.type.interceptor.Event.PRE_INSERT;
import static info.archinnov.achilles.type.interceptor.Event.PRE_UPDATE;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.type.interceptor.Event;


public class DefaultPreMutateBeanValidationInterceptor implements AchillesInternalInterceptor<Object> {

    private static final String LOGGER_NAME = "info.archinnov.achilles.internals.interceptor.DefaultBeanValidatorInterceptor";
    private static final Logger LOGGER = LoggerFactory.getLogger(LOGGER_NAME);

    private final Map<Class<?>, Boolean> constrainedClasses = new HashMap<>();
    private Validator validator;

    public DefaultPreMutateBeanValidationInterceptor(Validator validator) {
        this.validator = validator;
    }


    @Override
    public boolean acceptEntity(Class<?> entityClass) {
        if (!constrainedClasses.containsKey(entityClass)) {
            constrainedClasses.put(entityClass, validator.getConstraintsForClass(entityClass).isBeanConstrained());
        }
        final Boolean acceptEntity = constrainedClasses.get(entityClass);
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Accept entity %s for bean validation ? %s", entityClass.getCanonicalName(), acceptEntity));
        }
        return acceptEntity;
    }

    @Override
    public void onEvent(Object entity, Event event) {
        info.archinnov.achilles.validation.Validator.validateNotNull(entity, "Entity passed to bean validation interceptor should not be null on event %s", event.name());
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Trigger bean validation interceptor for entity of class %s and event %s",
                    entity.getClass().getCanonicalName(), event.name()));
        }
        Set<ConstraintViolation<Object>> violations = validator.validate(entity);
        if (violations.size() > 0) {
            StringBuilder errorMessage = new StringBuilder("Bean validation error on event '" + event.name() + "' : \n");
            for (ConstraintViolation<Object> violation : violations) {
                buildValidationErrorMessage(errorMessage, violation);
            }
            throw new AchillesBeanValidationException(errorMessage.toString());
        }
    }

    @Override
    public List<Event> interceptOnEvents() {
        return asList(PRE_INSERT, PRE_UPDATE);
    }


    private void buildValidationErrorMessage(StringBuilder errorMessage, ConstraintViolation<Object> violation) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Building validation error message for violation %s", violation.getMessage()));
        }
        String className = violation.getLeafBean().getClass().getCanonicalName();
        Path propertyPath = violation.getPropertyPath();
        errorMessage.append("\t");
        if (propertyPath != null && isNotBlank(propertyPath.toString())) {
            errorMessage.append("property '").append(propertyPath).append("'") //
                    .append(" of class '").append(className).append("' ") //
                    .append(violation.getMessage()).append("\n");
        } else {
            errorMessage.append(violation.getMessage()).append(" for class '").append(className).append("'");
        }
    }


}
