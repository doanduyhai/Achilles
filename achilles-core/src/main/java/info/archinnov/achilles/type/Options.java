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
package info.archinnov.achilles.type;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.listener.LWTResultListener;
import org.apache.commons.collections.CollectionUtils;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.internal.validation.Validator;

public class Options {

    private static final Predicate<LWTPredicate> FILTER_LWT_CONDITION = new Predicate<LWTPredicate>() {
        @Override
        public boolean apply(LWTPredicate predicate) {
            return predicate.type() == LWTPredicate.LWTType.EQUAL_CONDITION;
        }
    };

    ConsistencyLevel consistency;

    Integer ttl;

    Long timestamp;

    List<LWTPredicate> lwtPredicates = new ArrayList<>();

    Optional<LWTResultListener> LWTResultListenerO = Optional.absent();

    List<FutureCallback<Object>> asyncListeners;

    Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyO = Optional.absent();

    Options() {}

    public Optional<ConsistencyLevel> getConsistencyLevel() {
        return Optional.fromNullable(consistency);
    }

    public Optional<Integer> getTtl() {
        return Optional.fromNullable(ttl);
    }

    public Optional<Long> getTimestamp() {
        return Optional.fromNullable(timestamp);
    }

    public boolean isIfNotExists() {
        return lwtPredicates.size() == 1 && lwtPredicates.get(0).type() == LWTPredicate.LWTType.IF_NOT_EXISTS;
    }

    public boolean isIfExists() {
        return lwtPredicates.size() == 1 && lwtPredicates.get(0).type() == LWTPredicate.LWTType.IF_EXISTS;
    }

    public List<LWTCondition> getLwtPredicates() {
        final List<?> lwtPredicates1 = FluentIterable.from(lwtPredicates).filter(FILTER_LWT_CONDITION).toList();
        return (List<LWTCondition>)lwtPredicates1;
    }

    public boolean hasLWTConditions() {
        return CollectionUtils.isNotEmpty(lwtPredicates);
    }

    public Optional<LWTResultListener> getLWTResultListener() {
        return LWTResultListenerO;
    }

    public List<FutureCallback<Object>> getAsyncListeners() {
        return asyncListeners;
    }

    public boolean hasAsyncListeners() {
        return CollectionUtils.isNotEmpty(asyncListeners);
    }

    public Optional<com.datastax.driver.core.ConsistencyLevel> getSerialConsistency() {
       return serialConsistencyO;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(Options.class)
                .add("Consistency Level", this.consistency)
                .add("Time to live", this.ttl)
                .add("Timestamp", this.timestamp)
                .add("IF NOT EXISTS ? ", this.isIfNotExists())
                .add("IF EXISTS ? ", this.isIfExists())
                .add("CAS conditions", this.lwtPredicates)
                .add("CAS result listener optional", this.LWTResultListenerO)
                .add("Async listeners", this.asyncListeners)
				.add("Serial consistency", this.serialConsistencyO)
                .toString();
    }

    public Options duplicateWithoutTtlAndTimestamp() {
        return OptionsBuilder.withConsistency(consistency)
                .lwtPredicates(lwtPredicates)
                .LWTResultListener(LWTResultListenerO.orNull())
                .LWTLocalSerial(serialConsistencyO.isPresent());
    }

    public Options duplicateWithNewConsistencyLevel(ConsistencyLevel consistencyLevel) {
        return OptionsBuilder.withConsistency(consistencyLevel)
                .withTtl(ttl).withTimestamp(timestamp)
                .lwtPredicates(lwtPredicates)
                .LWTResultListener(LWTResultListenerO.orNull())
                .LWTLocalSerial(serialConsistencyO.isPresent());
    }

    public Options duplicateWithNewTimestamp(Long timestamp) {
        return OptionsBuilder.withConsistency(consistency)
                .withTtl(ttl).withTimestamp(timestamp)
                .lwtPredicates(lwtPredicates)
                .LWTResultListener(LWTResultListenerO.orNull())
                .LWTLocalSerial(serialConsistencyO.isPresent());
    }

    public static abstract class LWTPredicate {
        public abstract LWTType type();
        public static enum LWTType {
            IF_NOT_EXISTS, IF_EXISTS, EQUAL_CONDITION;
        }
    }

    public static class LWTIfNotExists extends LWTPredicate {
        @Override
        public LWTType type() {
            return LWTType.IF_NOT_EXISTS;
        }

        private LWTIfNotExists(){};

        public static enum Singleton {
            INSTANCE;

            private final LWTIfNotExists instance = new LWTIfNotExists();

            public LWTIfNotExists get() {
                return instance;
            }
        }

    }

    public static class LWTIfExists extends LWTPredicate {
        @Override
        public LWTType type() {
            return LWTType.IF_EXISTS;
        }

        private LWTIfExists(){};

        public static enum Singleton {
            INSTANCE;

            private final LWTIfExists instance = new LWTIfExists();

            public LWTIfExists get() {
                return instance;
            }
        }
    }

    public static class LWTCondition extends LWTPredicate {

        private String columnName;
        private Object value;

        @Override
        public LWTType type() {
          return LWTType.EQUAL_CONDITION;
        }

        public LWTCondition(String columnName, Object value) {
            Validator.validateNotBlank(columnName, "Lightweight Transaction condition column cannot be blank");
            this.columnName = columnName;
            this.value = value;
        }

        public void encodedValue(Object encodedValue) {
            this.value = encodedValue;
        }

        public String getColumnName() {
            return columnName;
        }

        public Object getValue() {
            return this.value;
        }

        public Clause toClause() {
            return eq(columnName, value);
        }

        public Clause toClauseForPreparedStatement() {
            return eq(columnName, bindMarker(columnName));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            LWTCondition that = (LWTCondition) o;

            return Objects.equal(this.type(), that.type())
                && Objects.equal(this.columnName, that.columnName)
                && Objects.equal(this.value, that.value);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(this.type(), this.columnName, this.value);
        }


        @Override
        public String toString() {
            return Objects.toStringHelper(Options.class)
                    .add("columnName", this.columnName)
                    .add("value", this.value)
                    .toString();
        }

    }

}
