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

import static info.archinnov.achilles.test.integration.entity.EntityWithClassLevelConstraint.TABLE_NAME;
import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.Entity;
import info.archinnov.achilles.annotations.Id;
import info.archinnov.achilles.test.integration.entity.EntityWithClassLevelConstraint.ValidEntity;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

import org.apache.commons.lang3.StringUtils;

@Entity(table = TABLE_NAME)
@ValidEntity
public class EntityWithClassLevelConstraint {
	public static final String TABLE_NAME = "entity_class_constrained";

	@Id
	private Long id;

	@Column
	private String firstname;

	@Column
	private String lastname;

    public EntityWithClassLevelConstraint() {
    }

    public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getFirstname() {
		return firstname;
	}

	public void setFirstname(String firstname) {
		this.firstname = firstname;
	}

	public String getLastname() {
		return lastname;
	}

	public void setLastname(String lastname) {
		this.lastname = lastname;
	}

	@Target({ TYPE, ANNOTATION_TYPE })
	@Retention(RUNTIME)
	@Constraint(validatedBy = { CustomValidator.class })
	@Documented
	public static @interface ValidEntity {

		String message() default "firstname and lastname should not be blank";

		Class<?>[] groups() default {};

		Class<? extends Payload>[] payload() default {};
	}

	public static class CustomValidator implements ConstraintValidator<ValidEntity, EntityWithClassLevelConstraint> {

		@Override
		public void initialize(ValidEntity constraintAnnotation) {

		}

		@Override
		public boolean isValid(EntityWithClassLevelConstraint entity, ConstraintValidatorContext context) {
			return entity != null && StringUtils.isNotBlank(entity.firstname)
					&& StringUtils.isNotBlank(entity.lastname);
		}

	}
}
