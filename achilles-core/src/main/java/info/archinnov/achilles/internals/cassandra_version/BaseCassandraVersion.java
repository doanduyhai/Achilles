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

package info.archinnov.achilles.internals.cassandra_version;

import info.archinnov.achilles.internals.parser.validator.BeanValidator;
import info.archinnov.achilles.internals.parser.validator.FieldValidator;
import info.archinnov.achilles.internals.parser.validator.NestedTypesValidator;
import info.archinnov.achilles.internals.parser.validator.TypeValidator;
import info.archinnov.achilles.internals.parser.validator.cassandra_2_1.BeanValidator2_1;
import info.archinnov.achilles.internals.parser.validator.cassandra_2_1.FieldValidator2_1;
import info.archinnov.achilles.internals.parser.validator.cassandra_2_1.NestedTypeValidator2_1;
import info.archinnov.achilles.internals.parser.validator.cassandra_2_1.TypeValidator2_1;

public interface BaseCassandraVersion {
    BeanValidator BEAN_VALIDATOR = new BeanValidator2_1();
    FieldValidator FIELD_VALIDATOR = new FieldValidator2_1();
    TypeValidator TYPE_VALIDATOR = new TypeValidator2_1();
    NestedTypesValidator NESTED_TYPES_VALIDATOR = new NestedTypeValidator2_1();

    default BeanValidator beanValidator() {
        return BEAN_VALIDATOR;
    }

    default FieldValidator fieldValidator() {
        return FIELD_VALIDATOR;
    }

    default TypeValidator typeValidator() {
        return TYPE_VALIDATOR;
    }

    default NestedTypesValidator nestedTypesValidator() {
        return NESTED_TYPES_VALIDATOR;
    }
}
