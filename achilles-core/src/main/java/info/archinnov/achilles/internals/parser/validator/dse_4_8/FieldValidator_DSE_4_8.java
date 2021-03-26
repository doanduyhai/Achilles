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

package info.archinnov.achilles.internals.parser.validator.dse_4_8;

import info.archinnov.achilles.internals.apt.AptUtils;
import info.archinnov.achilles.internals.parser.FieldParser.FieldMetaSignature;
import info.archinnov.achilles.internals.parser.TypeUtils;
import info.archinnov.achilles.internals.parser.validator.cassandra2_1.FieldValidator2_1;

public class FieldValidator_DSE_4_8 extends FieldValidator2_1 {

    @Override
    public void validateDSESearchIndex(AptUtils aptUtils, FieldMetaSignature fieldMetaSignature) {
        aptUtils.validateFalse(fieldMetaSignature.isCollection(),
            "@DSE_Search annotation is not supported yet on target collection type %s on field % of entity %s",
            fieldMetaSignature.targetType, fieldMetaSignature.context.fieldName, fieldMetaSignature.context.entityRawType);

        aptUtils.validateFalse(fieldMetaSignature.isUDT(),
            "@DSE_Search annotation is not supported on target UDT type %s on field % of entity %s",
            fieldMetaSignature.targetType, fieldMetaSignature.context.fieldName, fieldMetaSignature.context.entityRawType);

        aptUtils.validateFalse(fieldMetaSignature.targetType.equals(TypeUtils.JAVA_DRIVER_LOCAL_DATE),
            "@DSE_Search annotation is not supported on target type %s for DSE 4.8.X on field % of entity %s",
            fieldMetaSignature.targetType, fieldMetaSignature.context.fieldName, fieldMetaSignature.context.entityRawType);
    }
}
