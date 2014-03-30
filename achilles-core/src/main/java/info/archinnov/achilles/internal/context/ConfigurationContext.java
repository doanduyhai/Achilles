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
package info.archinnov.achilles.internal.context;

import javax.validation.Validator;
import info.archinnov.achilles.internal.interceptor.DefaultBeanValidationInterceptor;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.json.ObjectMapperFactory;
import info.archinnov.achilles.type.ConsistencyLevel;

public class ConfigurationContext {
    private boolean forceColumnFamilyCreation;

    private ObjectMapperFactory objectMapperFactory;

    private ConsistencyLevel defaultReadConsistencyLevel;

    private ConsistencyLevel defaultWriteConsistencyLevel;

    private Validator beanValidator;

    private DefaultBeanValidationInterceptor beanValidationInterceptor;

    private int preparedStatementLRUCacheSize = 5000;

    private boolean forceBatchStatementsOrdering = true;

    public boolean isForceColumnFamilyCreation() {
        return forceColumnFamilyCreation;
    }

    public void setForceColumnFamilyCreation(boolean forceColumnFamilyCreation) {
        this.forceColumnFamilyCreation = forceColumnFamilyCreation;
    }

    public ObjectMapperFactory getObjectMapperFactory() {
        return objectMapperFactory;
    }

    public void setObjectMapperFactory(ObjectMapperFactory objectMapperFactory) {
        this.objectMapperFactory = objectMapperFactory;
    }

    public ConsistencyLevel getDefaultReadConsistencyLevel() {
        return defaultReadConsistencyLevel;
    }

    public void setDefaultReadConsistencyLevel(ConsistencyLevel defaultReadConsistencyLevel) {
        this.defaultReadConsistencyLevel = defaultReadConsistencyLevel;
    }

    public ConsistencyLevel getDefaultWriteConsistencyLevel() {
        return defaultWriteConsistencyLevel;
    }

    public void setDefaultWriteConsistencyLevel(ConsistencyLevel defaultWriteConsistencyLevel) {
        this.defaultWriteConsistencyLevel = defaultWriteConsistencyLevel;
    }

    public Validator getBeanValidator() {
        return beanValidator;
    }

    public void setBeanValidator(Validator beanValidator) {
        this.beanValidator = beanValidator;
    }

    public int getPreparedStatementLRUCacheSize() {
        return preparedStatementLRUCacheSize;
    }

    public void setPreparedStatementLRUCacheSize(int preparedStatementLRUCacheSize) {
        this.preparedStatementLRUCacheSize = preparedStatementLRUCacheSize;
    }

    public boolean isForceBatchStatementsOrdering() {
        return forceBatchStatementsOrdering;
    }

    public void setForceBatchStatementsOrdering(boolean forceBatchStatementsOrdering) {
        this.forceBatchStatementsOrdering = forceBatchStatementsOrdering;
    }

    public boolean isClassConstrained(Class<?> clazz) {
        if (beanValidator != null) {
            return beanValidator.getConstraintsForClass(clazz).isBeanConstrained();
        } else {
            return false;
        }
    }

    public void addBeanValidationInterceptor(EntityMeta meta) {
        if (beanValidationInterceptor == null) {
            beanValidationInterceptor = new DefaultBeanValidationInterceptor(beanValidator);
        }
        meta.addInterceptor(beanValidationInterceptor);
    }
}
