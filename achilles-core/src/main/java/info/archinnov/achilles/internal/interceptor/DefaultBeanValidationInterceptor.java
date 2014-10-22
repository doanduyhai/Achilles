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
import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;

import java.util.List;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validator;

public class DefaultBeanValidationInterceptor implements Interceptor<Object> {

	private Validator validator;

	public DefaultBeanValidationInterceptor(Validator validator) {
		this.validator = validator;
	}

	@Override
	public void onEvent(Object entity) {
		Set<ConstraintViolation<Object>> violations = validator.validate(entity);
		if (violations.size() > 0) {
			StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
			for (ConstraintViolation<Object> violation : violations) {
				buildValidationErrorMessage(errorMessage, violation);
			}
			throw new AchillesBeanValidationException(errorMessage.toString());
		}
	}

	private void buildValidationErrorMessage(StringBuilder errorMessage, ConstraintViolation<Object> violation) {
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

	@Override
	public List<Event> events() {
		return asList(PRE_INSERT, PRE_UPDATE);
	}

}
