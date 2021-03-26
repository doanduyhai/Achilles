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

package info.archinnov.achilles.internals.constraints;

import static org.apache.commons.collections.CollectionUtils.isEmpty;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import org.apache.commons.collections.MapUtils;

import info.archinnov.achilles.internals.entities.TestUDT;

public class UDTValidator implements ConstraintValidator<ValidUDT, TestUDT> {

    @Override
    public void initialize(ValidUDT constraintAnnotation) {

    }

    @Override
    public boolean isValid(TestUDT value, ConstraintValidatorContext context) {
        final boolean emptyList = isEmpty(value.getList());
        final boolean emptyMap = MapUtils.isEmpty(value.getMap());

        if (emptyList) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("UDT list should not be empty")
                    .addPropertyNode("list")
                    .addConstraintViolation();
        }

        if (emptyMap) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate("UDT map should not be empty")
                    .addPropertyNode("map")
                    .addConstraintViolation();
        }

        return !emptyList && !emptyMap;
    }
}
