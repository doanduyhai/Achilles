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
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import com.datastax.driver.core.querybuilder.Clause;
import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.listener.CASResultListener;

public class Options {

    ConsistencyLevel consistency;

    Integer ttl;

    Long timestamp;

    boolean ifNotExists;

    List<CASCondition> CASConditions;

    Optional<CASResultListener> CASResultListenerO = Optional.absent();

    List<FutureCallback<Object>> asyncListeners;

    Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyO = Optional.absent();

    Options() {
    }

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
        return ifNotExists;
    }

    public List<CASCondition> getCASConditions() {
        return CASConditions;
    }

    public boolean hasCASConditions() {
        return CollectionUtils.isNotEmpty(CASConditions);
    }

    public Optional<CASResultListener> getCasResultListener() {
        return CASResultListenerO;
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
                .add("IF NOT EXISTS ? ", this.ifNotExists)
                .add("CAS conditions", this.CASConditions)
                .add("CAS result listener optional", this.CASResultListenerO)
                .add("Async listeners", this.asyncListeners)
				.add("Serial consistency", this.serialConsistencyO)
                .toString();
    }

    public Options duplicateWithoutTtlAndTimestamp() {
        return OptionsBuilder.withConsistency(consistency)
                .ifNotExists(ifNotExists).ifConditions(CASConditions)
                .casResultListener(CASResultListenerO.orNull())
                .casLocalSerial(serialConsistencyO.isPresent());
    }

    public Options duplicateWithNewConsistencyLevel(ConsistencyLevel consistencyLevel) {
        return OptionsBuilder.withConsistency(consistencyLevel)
                .withTtl(ttl).withTimestamp(timestamp)
                .ifNotExists(ifNotExists).ifConditions(CASConditions)
                .casResultListener(CASResultListenerO.orNull())
                .casLocalSerial(serialConsistencyO.isPresent());
    }

    public Options duplicateWithNewTimestamp(Long timestamp) {
        return OptionsBuilder.withConsistency(consistency)
                .withTtl(ttl).withTimestamp(timestamp)
                .ifNotExists(ifNotExists).ifConditions(CASConditions)
                .casResultListener(CASResultListenerO.orNull())
                .casLocalSerial(serialConsistencyO.isPresent());
    }


    public static class CASCondition {

        private String columnName;
        private Object value;

        public CASCondition(String columnName, Object value) {
            Validator.validateNotBlank(columnName, "CAS condition column cannot be blank");
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

            CASCondition that = (CASCondition) o;

            return columnName.equals(that.columnName) && value.equals(that.value);

        }

        @Override
        public int hashCode() {
            int result = columnName.hashCode();
            result = 31 * result + value.hashCode();
            return result;
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
