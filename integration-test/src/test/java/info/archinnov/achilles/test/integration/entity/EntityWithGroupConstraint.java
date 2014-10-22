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
package info.archinnov.achilles.test.integration.entity;

import static info.archinnov.achilles.interceptor.Event.*;
import static info.archinnov.achilles.test.integration.entity.EntityWithGroupConstraint.TABLE_NAME;
import static java.util.Arrays.asList;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.interceptor.Event;
import info.archinnov.achilles.interceptor.Interceptor;

import java.util.List;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Path;
import javax.validation.Validator;
import javax.validation.constraints.NotNull;

@Entity(table = TABLE_NAME)
public class EntityWithGroupConstraint {
	public static final String TABLE_NAME = "entity_group_constrained";

	@Id
	private Long id;

	@Column
	@NotNull(groups = CustomValidationGroup.class)
	private String name;

    public EntityWithGroupConstraint() {
    }

    public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public static interface CustomValidationGroup {
	}

	public static class CustomValidationInterceptor implements Interceptor<EntityWithGroupConstraint> {

		private Validator validator;

		public CustomValidationInterceptor() {
			this.validator = javax.validation.Validation.buildDefaultValidatorFactory().getValidator();
		}

		@Override
		public void onEvent(EntityWithGroupConstraint entity) {
			Set<ConstraintViolation<EntityWithGroupConstraint>> violations = validator.validate(entity,
					CustomValidationGroup.class);
			if (violations.size() > 0) {
				StringBuilder errorMessage = new StringBuilder("Bean validation error : \n");
				for (ConstraintViolation<EntityWithGroupConstraint> violation : violations) {
					buildValidationErrorMessage(errorMessage, violation);
				}
				throw new AchillesBeanValidationException(errorMessage.toString());
			}
		}

		private void buildValidationErrorMessage(StringBuilder errorMessage,
				ConstraintViolation<EntityWithGroupConstraint> violation) {
			String className = violation.getLeafBean().getClass().getCanonicalName();
			Path propertyPath = violation.getPropertyPath();

			errorMessage.append("\t");
			errorMessage.append("property '").append(propertyPath).append("'") //
					.append(" of class '").append(className).append("' ") //
					.append(violation.getMessage()).append("\n");
		}

		@Override
		public List<Event> events() {
			return asList(PRE_INSERT, PRE_UPDATE);
		}

	}
}
