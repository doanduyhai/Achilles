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

    protected EntityValidator entityValidator = EntityValidator.Singleton.INSTANCE.get();

    public void validateOptionsForUpsert(Object entity, Map<Class<?>, EntityMeta> entityMetaMap, Options options) {
        validateNoTtlForClusteredCounter(entity, entityMetaMap, options);
        validateNoLWTConditionsAndTimestamp(options);
    }

    public void validateNoLWTConditionsAndTimestamp(Options options) {
        Validator.validateFalse(options.isIfNotExists() && options.getTimestamp().isPresent(), "Cannot provide custom timestamp for insert operations using Light Weight Transaction");
        Validator.validateFalse(options.hasLWTConditions() && options.getTimestamp().isPresent(), "Cannot provide custom timestamp for update operations using Light Weight Transaction");
    }

    public boolean isOptionsValidForBatch(Options options) {
        boolean valid = true;
        if (options.getConsistencyLevel().isPresent() || options.hasAsyncListeners()) {
            valid = false;
        }
        return valid;
    }

    public void validateNoAsyncListener(Options options) {
        Validator.validateFalse(options.hasAsyncListeners(),"Cannot provide custom asynchronous listener for this operation");
    }

    private void validateNoTtlForClusteredCounter(Object entity, Map<Class<?>, EntityMeta> entityMetaMap, Options options) {
        if (options.getTtl().isPresent()) {
            entityValidator.validateNotClusteredCounter(entity, entityMetaMap);
        }
    }



    public static enum Singleton {
        INSTANCE;

        private final OptionsValidator instance = new OptionsValidator();

        public OptionsValidator get() {
            return instance;
        }
    }
}
