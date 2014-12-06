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

import static info.archinnov.achilles.type.Options.LWTCondition;
import java.util.Arrays;
import java.util.List;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.listener.LWTResultListener;

/**
 * <p>
 * Simple class to build Achilles options
 *
 * <pre class="code"><code class="java">
 *
 * Options options;
 *
 * // Consistency
 * options = OptionsBuilder.withConsistency(QUORUM);
 *
 * // TTL
 * options = OptionsBuilder.withTtl(10);
 *
 * // Timestamp
 * options = OptionsBuilder.withTimestamp(100L);
 *
 *
 * // LWT 'IF NOT EXISTS'
 * options = OptionsBuilder.ifNotExists();
 *
 * // LWT update conditions
 * options = OptionsBuilder
 *              .ifEqualCondition("name","John")
 *              .ifEqualCondition("age_in_years",33L);
 *
 * // LWT result listener
 * options = OptionsBuilder.lwtResultListener(listener);
 *
 * // LWT LOCAL_SERIAL instead of the default SERIAL value
 * options = OptionsBuilder.lwtLocalSerial();
 *
 * // Multiple options at a time
 * options = OptionsBuilder.withTtl(11)
 *                 .withConsistency(ANY)
 *                 .withTimestamp(111L);
 *
 * </code></pre>
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Custom-Types#optionsbuilder" target="_blank">OptionsBuilder</a>
 */
public class OptionsBuilder {

    private static final NoOptions NO_OPTIONS = new NoOptions();
    private static final Options.LWTIfExists IF_EXISTS = Options.LWTIfExists.Singleton.INSTANCE.get();
    private static final Options.LWTIfNotExists IF_NOT_EXISTS = Options.LWTIfNotExists.Singleton.INSTANCE.get();

    /**
     * Build an empty option
     * @return NoOptions
     */
    public static NoOptions noOptions() {
        return NO_OPTIONS;
    }

    /**
     * Use provided consistency level
     * @param consistencyLevel
     * @return BuiltOptions
     */
    public static BuiltOptions withConsistency(ConsistencyLevel consistencyLevel) {
        return new BuiltOptions(consistencyLevel);
    }

    /**
     * Use provided time to live <strong>in seconds</strong>
     * @param ttl
     * @return BuiltOptions
     */
    public static BuiltOptions withTtl(Integer ttl) {
        return new BuiltOptions(ttl);
    }

    /**
     * Use provided timestamp <strong>in micro seconds</strong>
     * @param timestamp
     * @return BuiltOptions
     */
    public static BuiltOptions withTimestamp(Long timestamp) {
        return new BuiltOptions(timestamp);
    }

    /**
     * Use IF NOT EXISTS clause for INSERT operations. This has no effect on statements other than INSERT
     * @return BuiltOptions
     */
    public static BuiltOptions ifNotExists() {
        return new BuiltOptions(IF_NOT_EXISTS);
    }

    /**
     * Use IF EXISTS clause for DELETE operations. This has no effect on statements other than DELETE
     * @return BuiltOptions
     */
    public static BuiltOptions ifExists() {
        return new BuiltOptions(IF_EXISTS);
    }

    /**
     * Use ifEqualCondition(String columnName, Object value)
     */
    @Deprecated
    public static BuiltOptions ifConditions(LWTCondition... lwtConditions) {
        return new BuiltOptions(lwtConditions);
    }

    /**
     * Use LWT conditions for UPDATE operations. This has no effect on statements other than UPDATE
     *
     * <pre class="code"><code class="java">
     *
     * Options options = OptionsBuilder.ifEqualCondition("name","John");
     * </code></pre>
     *
     * @param columnName name of the column to be checked for LWT
     * @param value expected value of the column to be checked for LWT
     *
     * @return BuiltOptions
     */
    public static BuiltOptions ifEqualCondition(String columnName, Object value) {
        return new BuiltOptions(new LWTCondition(columnName, value));
    }

    /**
     * Inject a LWT result listener for all LWT operations
     *
     * <pre class="code"><code class="java">
     *
     * LWTResultListener LWTListener = new LWTResultListener() {
     *
     *     public void onLWTSuccess() {
     *         // Do something on success
     *     }
     *
     *     public void onLWTError(LWTResult LWTResult) {
     *
     *         //Get type of LWT operation that fails
     *         LWTResult.Operation operation = LWTResult.operation();
     *
     *         // Print out current values
     *         TypedMap currentValues = LWTResult.currentValues();
     *         for(Entry<String,Object> entry: currentValues.entrySet()) {
     *             System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue()));
     *         }
     *     }
     * };
     *
     * persistenceManager.update(user, OptionsBuilder.
     *         .ifEqualCondition("login","jdoe")
     *         .lwtResultListener(lwtListener));
     * </code></pre>
     *
     * @param listener LWTResultListener
     * @return BuiltOptions
     */	
    public static BuiltOptions lwtResultListener(LWTResultListener listener) {
        return new BuiltOptions(listener);
    }

    public static BuiltOptions withAsyncListeners(FutureCallback<Object>... listeners) {
        return new BuiltOptions(listeners);
    }

    /**
     * Force LOCAL_SERIAL consistency for all LWT operations.
     * By default LWT operations are performed using SERIAL serial consistency level
     * @return BuiltOptions
     */
    public static BuiltOptions lwtLocalSerial() {
        return new BuiltOptions(Optional.fromNullable(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL));
    }

    /**
     * Empty options
     */
    public static class NoOptions extends Options {
        protected NoOptions() {
        }

        @Override
        public NoOptions duplicateWithoutTtlAndTimestamp() {
            return this;
        }
    }

    /**
     * Built options
     */
    public static class BuiltOptions extends Options {
        protected BuiltOptions(ConsistencyLevel consistencyLevel) {
            super.consistency = consistencyLevel;
        }

        protected BuiltOptions(Integer ttl) {
            super.ttl = ttl;
        }

        protected BuiltOptions(Long timestamp) {
            super.timestamp = timestamp;
        }

        protected BuiltOptions(LWTPredicate... lwtPredicates) {
            super.lwtPredicates.addAll(Arrays.asList(lwtPredicates));
        }

        protected BuiltOptions(LWTResultListener listener) {
            super.LWTResultListenerO = Optional.fromNullable(listener);
        }

        protected BuiltOptions(Optional<com.datastax.driver.core.ConsistencyLevel> serialConsistencyO) {
            super.serialConsistencyO = serialConsistencyO;
        }

        protected BuiltOptions(FutureCallback<Object>... listeners) {
            super.asyncListeners = Arrays.asList(listeners);
        }

        /**
         * Use provided consistency level
         * @param consistencyLevel
         * @return BuiltOptions
         */
        public BuiltOptions withConsistency(ConsistencyLevel consistencyLevel) {
            super.consistency = consistencyLevel;
            return this;
        }

        /**
         * Use provided time to live <strong>in seconds</strong>
         * @param ttl
         * @return BuiltOptions
         */
        public BuiltOptions withTtl(Integer ttl) {
            super.ttl = ttl;
            return this;
        }


        /**
         * Use provided timestamp <strong>in micro seconds</strong>
         * @param timestamp
         * @return BuiltOptions
         */
        public BuiltOptions withTimestamp(Long timestamp) {
            super.timestamp = timestamp;
            return this;
        }

        /**
         * Use IF NOT EXISTS clause for INSERT operations. This has no effect on statements other than INSERT
         * @return BuiltOptions
         */
        public BuiltOptions ifNotExists() {
            Validator.validateEmpty(super.lwtPredicates, "There is already existing Lightweight Transaction predicate : '%s', cannot add IF NOT EXISTS", super.lwtPredicates.toString());
            super.lwtPredicates.add(IF_NOT_EXISTS);
            return this;
        }

        /**
         * Use IF NOT EXISTS clause for INSERT operations. This has no effect on statements other than INSERT
         *
         * @param ifNotExists whether to use IF NOT EXISTS clause
         * @return BuiltOptions
         */
        public BuiltOptions ifNotExists(boolean ifNotExists) {
            if (ifNotExists) {
                Validator.validateEmpty(super.lwtPredicates, "There is already existing Lightweight Transaction predicate : '%s', cannot add IF NOT EXISTS", super.lwtPredicates.toString());
                super.lwtPredicates.add(IF_NOT_EXISTS);
            }
            return this;
        }


        /**
         * Use IF EXISTS clause for DELETE operations. This has no effect on statements other than DELETE
         * @return BuiltOptions
         */
        public BuiltOptions ifExists() {
            Validator.validateEmpty(super.lwtPredicates, "There is already existing Lightweight Transaction predicate : '%s', cannot add IF EXISTS", super.lwtPredicates.toString());
            super.lwtPredicates.add(IF_EXISTS);
            return this;
        }

        /**
         * Use IF EXISTS clause for DELETE operations. This has no effect on statements other than DELETE
         *
         * @param ifExists whether to use IF NOT EXISTS clause
         * @return BuiltOptions
         */
        public BuiltOptions ifExists(boolean ifExists) {
            if (ifExists) {
                Validator.validateEmpty(super.lwtPredicates, "There is already existing Lightweight Transaction predicate : '%s', cannot add IF EXISTS", super.lwtPredicates.toString());
                super.lwtPredicates.add(IF_EXISTS);
            }
            return this;
        }

         /**
         * Use lwtResultListener()
         */
         @Deprecated
        public BuiltOptions LWTResultListener(LWTResultListener listener) {
             return lwtResultListener(listener);
         }

        /**
         * Inject a LWT result listener for all LWT operations
         *
         * <pre class="code"><code class="java">
         *
         * LWTResultListener LWTListener = new LWTResultListener() {
         *
         *     public void onLWTSuccess() {
         *         // Do something on success
         *     }
         *
         *     public void onLWTError(LWTResult LWTResult) {
         *
         *         //Get type of LWT operation that fails
         *         LWTResult.Operation operation = LWTResult.operation();
         *
         *         // Print out current values
         *         TypedMap currentValues = LWTResult.currentValues();
         *         for(Entry<String,Object> entry: currentValues.entrySet()) {
         *             System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue()));
         *         }
         *     }
         * };
         *
         * persistenceManager.update(user, OptionsBuilder
         *         .ifEqualCondition("login","jdoe")
         *         .lwtResultListener(lwtListener));
         * </code></pre>
         *
         * @param listener LWTResultListener
         * @return BuiltOptions
         */
        public BuiltOptions lwtResultListener(LWTResultListener listener) {
            super.LWTResultListenerO = Optional.fromNullable(listener);
            return this;
        }

        /**
         * Use ifEqualCondition(String columnName, Object value) instead. Call this method again for multiple conditions
         */
        @Deprecated
        public BuiltOptions ifConditions(LWTCondition... lwtConditions) {
            Validator.validateFalse(lwtPredicates.contains(IF_EXISTS), "Cannot add IF = XXX with IF EXISTS");
            Validator.validateFalse(lwtPredicates.contains(IF_NOT_EXISTS), "Cannot add IF = XXX with IF NOT EXISTS");
            super.lwtPredicates.addAll(Arrays.asList(lwtConditions));
            return this;
        }

        /**
         Use ifEqualCondition(String columnName, Object value) instead. Call this method again for multiple conditions
         */
        @Deprecated
        public BuiltOptions ifConditions(List<LWTCondition> lwtConditions) {
            super.lwtPredicates.addAll(lwtConditions);
            return this;
        }

        /**
         * Use LWT conditions for UPDATE operations. This has no effect on statements other than UPDATE. For multiple equal conditions, just call this method again
         *
         * <pre class="code"><code class="java">
         *
         * Options options = OptionsBuilder
         *              .ifEqualCondition("name","John")
         *              .ifEqualCondition("age_in_years",33L);
         * </code></pre>
         *
         * @param columnName name of the column to be checked for LWT
         * @param value expected value of the column to be checked for LWT
         * @return BuiltOptions
         */
        public BuiltOptions ifEqualCondition(String columnName, Object value) {
            Validator.validateFalse(lwtPredicates.contains(IF_EXISTS), "Cannot add IF = XXX with IF EXISTS");
            Validator.validateFalse(lwtPredicates.contains(IF_NOT_EXISTS), "Cannot add IF = XXX with IF NOT EXISTS");
            super.lwtPredicates.add(new LWTCondition(columnName, value));
            return this;
        }

        BuiltOptions lwtPredicates(List<LWTPredicate> lwtPredicates) {
            super.lwtPredicates = lwtPredicates;
            return this;
        }

        /**
         * Use lwtLocalSerial()
         */
        @Deprecated
        public BuiltOptions LWTLocalSerial() {
            return lwtLocalSerial();
        }

        /**
         * Use lwtLocalSerial(boolean localSerial)
         */
        @Deprecated
        BuiltOptions LWTLocalSerial(boolean localSerial) {
            return lwtLocalSerial(localSerial);
        }

        /**
         * Force LOCAL_SERIAL consistency for all LWT operations.
         * By default LWT operations are performed using SERIAL serial consistency level
         * @return BuiltOptions
         */
        public BuiltOptions lwtLocalSerial() {
            super.serialConsistencyO = Optional.fromNullable(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);
            return this;
        }

        /**
         * Force LOCAL_SERIAL consistency for all LWT operations.
         * By default LWT operations are performed using SERIAL serial consistency level
         *
         * @param localSerial whether to use LOCAL_SERIAL
         * @return BuiltOptions
         */
        BuiltOptions lwtLocalSerial(boolean localSerial) {
            if (localSerial) {
                super.serialConsistencyO = Optional.fromNullable(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);
            }
            return this;
        }
    }

}
