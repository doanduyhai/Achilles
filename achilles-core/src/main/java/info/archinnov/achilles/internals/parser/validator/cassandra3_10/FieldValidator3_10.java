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

package info.archinnov.achilles.internals.parser.validator.cassandra3_10;

import java.util.List;

import com.squareup.javapoet.TypeName;

import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.FieldValidator2_1;

public class FieldValidator3_10 extends FieldValidator2_1 {

    @Override
    public List<TypeName> getAllowedTypes() {
        return TypeUtils.ALLOWED_TYPES_3_10;
    }

}
