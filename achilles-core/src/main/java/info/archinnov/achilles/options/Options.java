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
package info.archinnov.achilles.options;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.querybuilder.NotEqualCQLClause;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import info.archinnov.achilles.listener.LWTResultListener;
import info.archinnov.achilles.type.ConsistencyLevel;
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
            return !predicate.type().existentialCondition();
        }
    };

    Optional<ConsistencyLevel> consistency = Optional.absent();

    Optional<Integer> ttl = Optional.absent();

    Optional<Long> timestamp = Optional.absent();

    List<LWTPredicate> lwtPredicates = new ArrayList<>();

    Optional<LWTResultListener> lwtResultListenerO = Optional.absent();

    List<FutureCallback<Object>> asyncListeners = new ArrayList<>();

    Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyO = Optional.absent();

    boolean createProxy = false;

    Options() {}

    public Optional<ConsistencyLevel> getConsistencyLevel() {
        return consistency;
    }

    public Optional<Integer> getTtl() {
        return ttl;
    }

    public Optional<Long> getTimestamp() {
        return timestamp;
    }

    public boolean hasConsistencyLevel() {
        return consistency.isPresent();
    }

    public boolean hasTTL() {
        return ttl.isPresent();
    }

    public boolean hasTimestamp() {
        return timestamp.isPresent();
    }

    public boolean isIfNotExists() {
        return lwtPredicates.size() == 1 && lwtPredicates.get(0).type() == LWTPredicate.LWTType.IF_NOT_EXISTS;
    }

    public boolean isIfExists() {
        return lwtPredicates.size() == 1 && lwtPredicates.get(0).type() == LWTPredicate.LWTType.IF_EXISTS;
    }

    public List<LWTCondition> getLwtConditions() {
        final List<?> lwtPredicates1 = FluentIterable.from(lwtPredicates).filter(FILTER_LWT_CONDITION).toList();
        return (List<LWTCondition>)lwtPredicates1;
    }

    public List<LWTPredicate> getLwtPredicates() {
        return lwtPredicates;
    }

    public boolean hasLWTConditions() {
        return CollectionUtils.isNotEmpty(lwtPredicates);
    }

    public Optional<LWTResultListener> getLWTResultListener() {
        return lwtResultListenerO;
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

    public boolean shouldCreateProxy() {
        return createProxy;
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
                .add("CAS result listener optional", this.lwtResultListenerO)
                .add("Async listeners", this.asyncListeners)
		        .add("Serial consistency", this.serialConsistencyO)
		        .add("Should create proxy", this.createProxy)
                .toString();
    }

    public Options duplicateWithoutTtlAndTimestamp() {
        return OptionsBuilder.withConsistencyO(consistency)
                .lwtPredicates(lwtPredicates)
                .lwtResultListener(lwtResultListenerO.orNull())
                .lwtLocalSerial(serialConsistencyO.isPresent())
                .withAsyncListeners(asyncListeners)
                .withProxy(createProxy);

    }

    public Options duplicateWithNewConsistencyLevel(ConsistencyLevel consistencyLevel) {
        return OptionsBuilder.withConsistency(consistencyLevel)
                .withTtlO(ttl).withTimestampO(timestamp)
                .lwtPredicates(lwtPredicates)
                .lwtResultListener(lwtResultListenerO.orNull())
                .lwtLocalSerial(serialConsistencyO.isPresent())
                .withAsyncListeners(asyncListeners)
                .withProxy(createProxy);
    }

    public Options duplicateWithNewTimestamp(Long timestamp) {
        return OptionsBuilder.withConsistencyO(consistency)
                .withTtlO(ttl).withTimestamp(timestamp)
                .lwtPredicates(lwtPredicates)
                .lwtResultListener(lwtResultListenerO.orNull())
                .lwtLocalSerial(serialConsistencyO.isPresent())
                .withAsyncListeners(asyncListeners)
                .withProxy(createProxy);
    }

    public static abstract class LWTPredicate {
        public abstract LWTType type();
        public static enum LWTType {
            IF_NOT_EXISTS() {
                @Override
                public boolean existentialCondition() {
                    return true;
                }

                @Override
                public Clause buildCQLClauseForPreparedStatement(String columnName) {
                    return null;
                }

                @Override
                public Clause buildCQLClause(String columnName, Object value) {
                    return null;
                }
            },
            IF_EXISTS() {
                @Override
                public boolean existentialCondition() {
                    return true;
                }

                @Override
                public Clause buildCQLClauseForPreparedStatement(String columnName) {
                    return null;
                }

                @Override
                public Clause buildCQLClause(String columnName, Object value) {
                    return null;
                }
            },
            EQUAL_CONDITION() {
                @Override
                public boolean existentialCondition() {
                    return false;
                }

                @Override
                public Clause buildCQLClauseForPreparedStatement(String columnName) {
                    return eq(columnName, bindMarker(columnName));
                }

                @Override
                public Clause buildCQLClause(String columnName, Object value) {
                    return eq(columnName,value);
                }
            },
            GT_CONDITION() {
                @Override
                public boolean existentialCondition() {
                    return false;
                }

                @Override
                public Clause buildCQLClauseForPreparedStatement(String columnName) {
                    return gt(columnName, bindMarker(columnName));
                }

                @Override
                public Clause buildCQLClause(String columnName, Object value) {
                    return gt(columnName, value);
                }
            },
            GTE_CONDITION() {
                @Override
                public boolean existentialCondition() {
                    return false;
                }

                @Override
                public Clause buildCQLClauseForPreparedStatement(String columnName) {
                    return gte(columnName, bindMarker(columnName));
                }

                @Override
                public Clause buildCQLClause(String columnName, Object value) {
                    return gte(columnName, value);
                }
            },
            LT_CONDITION() {
                @Override
                public boolean existentialCondition() {
                    return false;
                }

                @Override
                public Clause buildCQLClauseForPreparedStatement(String columnName) {
                    return lt(columnName, bindMarker(columnName));
                }

                @Override
                public Clause buildCQLClause(String columnName, Object value) {
                    return lt(columnName, value);
                }
            },
            LTE_CONDITION() {
                @Override
                public boolean existentialCondition() {
                    return false;
                }

                @Override
                public Clause buildCQLClauseForPreparedStatement(String columnName) {
                    return lte(columnName, bindMarker(columnName));
                }

                @Override
                public Clause buildCQLClause(String columnName, Object value) {
                    return lte(columnName, value);
                }
            },
            NOT_EQUAL_CONDITION() {
                @Override
                public boolean existentialCondition() {
                    return false;
                }

                @Override
                public Clause buildCQLClauseForPreparedStatement(String columnName) {
                    return NotEqualCQLClause.build(columnName);
                }

                @Override
                public Clause buildCQLClause(String columnName, Object value) {
                    return NotEqualCQLClause.build(columnName, value);
                }
            };

            public abstract boolean existentialCondition();

            public abstract Clause buildCQLClauseForPreparedStatement(String columnName);

            public abstract Clause buildCQLClause(String columnName, Object value);

        }

        public abstract LWTPredicate duplicate();
    }

    public static class LWTIfNotExists extends LWTPredicate {
        @Override
        public LWTType type() {
            return LWTType.IF_NOT_EXISTS;
        }

        @Override
        public LWTPredicate duplicate() {
            return Singleton.INSTANCE.instance;
        }

        private LWTIfNotExists(){}

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

        private LWTIfExists(){}

        @Override
        public LWTPredicate duplicate() {
            return Singleton.INSTANCE.instance;
        }

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
        private LWTType type;

        @Override
        public LWTType type() {
          return type;
        }

        @Override
        public LWTPredicate duplicate() {
            return new LWTCondition(type,columnName);
        }

        private LWTCondition(LWTType type,String columnName) {
            this.type = type;
            this.columnName = columnName;
        }

        public LWTCondition(LWTType type, String columnName, Object value) {
            Validator.validateNotNull(type, "Lightweight Transaction condition type cannot be null");
            Validator.validateNotBlank(columnName, "Lightweight Transaction condition column cannot be blank");
            this.type = type;
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
            return type().buildCQLClause(columnName,value);
        }

        public Clause toClauseForPreparedStatement() {
            return type().buildCQLClauseForPreparedStatement(columnName);
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

            return Objects.equal(this.type, that.type)
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
