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

package info.archinnov.achilles.internal.persistence.operations;

import java.util.Map;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.Options;

public class OptionsValidator {

    protected EntityValidator entityValidator = new EntityValidator();

    public void validateOptionsForInsert(Object entity, Map<Class<?>, EntityMeta> entityMetaMap, Options options) {
        validateNoTtlForClusteredCounter(entity, entityMetaMap, options);
        validateNoCasConditionsAndTtl(options);
    }

    public void validateOptionsForUpdate(Object entity, Map<Class<?>, EntityMeta> entityMetaMap, Options options) {
        validateNoTtlForClusteredCounter(entity, entityMetaMap, options);
        validateNoCasConditionsAndTtl(options);
    }

    private void validateNoCasConditionsAndTtl(Options options) {
        Validator.validateFalse(options.isIfNotExists() && options.getTimestamp().isPresent(), "Cannot provide custom timestamp for CAS insert operations");
        Validator.validateFalse(options.hasCasConditions() && options.getTimestamp().isPresent(), "Cannot provide custom timestamp for CAS update operations");
    }

    private void validateNoTtlForClusteredCounter(Object entity, Map<Class<?>, EntityMeta> entityMetaMap, Options options) {
        if (options.getTtl().isPresent()) {
            entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
        }
    }

}
