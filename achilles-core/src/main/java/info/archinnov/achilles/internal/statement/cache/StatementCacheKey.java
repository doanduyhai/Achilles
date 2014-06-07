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

import static info.archinnov.achilles.type.Options.CASCondition;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import com.google.common.base.Optional;
import info.archinnov.achilles.query.slice.SliceQueryProperties;
import info.archinnov.achilles.type.Options;

public class StatementCacheKey {
    private CacheType type;

    private Set<String> fields = new LinkedHashSet<>();

    private Class<?> entityClass;

    private OptionsCacheKey optionsCacheKey;

    private Optional<SliceQueryProperties> sliceQueryPropertiesO = Optional.absent();

    public StatementCacheKey(CacheType type, Set<String> fields, Class<?> entityClass, Options options) {
        this.type = type;
        this.entityClass = entityClass;
        this.fields = fields;
        this.optionsCacheKey = OptionsCacheKey.fromOptions(options);
    }

    public StatementCacheKey(CacheType type, SliceQueryProperties sliceQueryProperties) {
        this.type = type;
        this.entityClass = sliceQueryProperties.getEntityMeta().getEntityClass();
        this.sliceQueryPropertiesO = Optional.fromNullable(sliceQueryProperties);
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
        return Objects.hash(entityClass, fields, type, optionsCacheKey, sliceQueryPropertiesO);
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
                Objects.equals(this.optionsCacheKey, other.optionsCacheKey) &&
                Objects.equals(this.sliceQueryPropertiesO, other.sliceQueryPropertiesO);
    }

    @Override
    public String toString() {
        return "StatementCacheKey{" +
                "type=" + Objects.toString(type) +
                ", fields=" + Objects.toString(fields) +
                ", entityClass=" + Objects.toString(entityClass) +
                ", optionsCacheKey=" + Objects.toString(optionsCacheKey) +
                '}';
    }

    private static class OptionsCacheKey {
        private boolean hasTimestamp;
        private boolean ifNotExists;
        private List<CASCondition> CASConditions;

        private OptionsCacheKey(boolean hasTimestamp, boolean ifNotExists, List<CASCondition> CASConditions) {
            this.hasTimestamp = hasTimestamp;
            this.ifNotExists = ifNotExists;
            this.CASConditions = CASConditions;
        }

        private static OptionsCacheKey fromOptions(Options options) {
            boolean hasTimestamp = options.getTimestamp().isPresent();
            boolean ifNotExists = options.isIfNotExists();
            List<CASCondition> CASConditions = Optional.fromNullable(options.getCASConditions()).or(Collections.<CASCondition>emptyList());
            return new OptionsCacheKey(hasTimestamp, ifNotExists, CASConditions);
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
            return Objects.equals(this.hasTimestamp, other.hasTimestamp) &&
                    Objects.equals(this.ifNotExists, other.ifNotExists) &&
                    Objects.equals(this.CASConditions, other.CASConditions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.hasTimestamp, this.ifNotExists, this.CASConditions);
        }

        @Override
        public String toString() {
            return "OptionsCacheKey{" +
                    "hasTimestamp=" + Objects.toString(hasTimestamp) +
                    ", ifNotExists=" + Objects.toString(ifNotExists) +
                    ", CASConditions=" + Objects.toString(CASConditions) +
                    '}';
        }
    }
}
