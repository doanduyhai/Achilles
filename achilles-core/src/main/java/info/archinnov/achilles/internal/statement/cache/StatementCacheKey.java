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
package info.archinnov.achilles.internal.statement.cache;

import static info.archinnov.achilles.type.Options.CasCondition;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import com.google.common.base.Optional;
import info.archinnov.achilles.type.Options;

public class StatementCacheKey {
    private CacheType type;

    private Set<String> fields;

    private Class<?> entityClass;

    private OptionsCacheKey optionsCacheKey;

    public StatementCacheKey(CacheType type, Set<String> fields, Class<?> entityClass, Options options) {
        this.type = type;
        this.entityClass = entityClass;
        this.fields = fields;
        this.optionsCacheKey = OptionsCacheKey.fromOptions(options);
    }

    public CacheType getType() {
        return type;
    }

    public Set<String> getFields() {
        return fields;
    }

    @SuppressWarnings("unchecked")
    public <T> Class<T> getEntityClass() {
        return (Class<T>) entityClass;
    }

    @Override
    public int hashCode() {
        return Objects.hash(entityClass, fields, type, optionsCacheKey);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }

        StatementCacheKey other = (StatementCacheKey) o;

        return Objects.equals(this.entityClass, other.entityClass) &&
                Objects.equals(this.fields, other.fields) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.optionsCacheKey, other.optionsCacheKey);
    }

    private static class OptionsCacheKey {
        private boolean hasConsistencyLevel;
        private boolean hasTimestamp;
        private boolean ifNotExists;
        private List<CasCondition> casConditions;

        private OptionsCacheKey(boolean hasConsistencyLevel, boolean hasTimestamp, boolean ifNotExists, List<CasCondition> casConditions) {
            this.hasConsistencyLevel = hasConsistencyLevel;
            this.hasTimestamp = hasTimestamp;
            this.ifNotExists = ifNotExists;
            this.casConditions = casConditions;
        }

        private static OptionsCacheKey fromOptions(Options options) {
            boolean hasConsistencyLevel = options.getConsistencyLevel().isPresent();
            boolean hasTimestamp = options.getTimestamp().isPresent();
            boolean ifNotExists = options.isIfNotExists();
            List<CasCondition> casConditions = Optional.fromNullable(options.getCasConditions()).or(Collections.<CasCondition>emptyList());
            return new OptionsCacheKey(hasConsistencyLevel, hasTimestamp, ifNotExists, casConditions);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null) {
                return false;
            }
            if (getClass() != o.getClass()) {
                return false;
            }

            final OptionsCacheKey other = (OptionsCacheKey) o;
            return Objects.equals(this.hasConsistencyLevel, other.hasConsistencyLevel) &&
                    Objects.equals(this.hasTimestamp, other.hasTimestamp) &&
                    Objects.equals(this.ifNotExists, other.ifNotExists) &&
                    Objects.equals(this.casConditions, other.casConditions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.hasConsistencyLevel, this.hasTimestamp, this.ifNotExists, this.casConditions);
        }
    }
}
