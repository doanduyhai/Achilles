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
 * options = OptionsBuilder.ifConditions(Arrays.asList(
 *              new LWTCondition("name","John"),
 *              new LWTCondition("age_in_years",33L));
 *
 * // LWT result listener
 * options = OptionsBuilder.LWTResultListener(listener);
 *
 * // LWT LOCAL_SERIAL instead of the default SERIAL value
 * options = OptionsBuilder.LWTLocalSerial();
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

    private static final NoOptions noOptions = new NoOptions();

    /**
     * Build an empty option
     * @return NoOptions
     */
    public static NoOptions noOptions() {
        return noOptions;
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
        return new BuiltOptions(true);
    }

    /**
     * Use LWT conditions for UPDATE operations. This has no effect on statements other than UPDATE
     *
     * <pre class="code"><code class="java">
     *
     * Options options = OptionsBuilder.ifConditions(Arrays.asList(
     *              new LWTCondition("name","John"),
     *              new LWTCondition("age_in_years",33L));
     * </code></pre>
     *
     * @param  LWTConditions list of LWTConditions
     * @return BuiltOptions
     */
    public static BuiltOptions ifConditions(LWTCondition... LWTConditions) {
        return new BuiltOptions(LWTConditions);
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
     *         ifConditions(Arrays.asList(
     *             new LWTCondition("login","jdoe")))
     *         .LWTResultListener(casListener));
     * </code></pre>
     *
     * @param listener LWTResultListener
     * @return BuiltOptions
     */	
    public static BuiltOptions LWTResultListener(LWTResultListener listener) {
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
    public static BuiltOptions LWTLocalSerial() {
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

        protected BuiltOptions(boolean ifNotExists) {
            super.ifNotExists = ifNotExists;
        }

        protected BuiltOptions(LWTCondition... LWTConditions) {
            super.LWTConditions = Arrays.asList(LWTConditions);
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
            super.ifNotExists = true;
            return this;
        }

        /**
         * Use IF NOT EXISTS clause for INSERT operations. This has no effect on statements other than INSERT
         *
         * @param ifNotExists whether to use IF NOT EXISTS clause
         * @return BuiltOptions
         */
        public BuiltOptions ifNotExists(boolean ifNotExists) {
            super.ifNotExists = ifNotExists;
            return this;
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
         *         ifConditions(Arrays.asList(
         *             new LWTCondition("login","jdoe")))
         *         .LWTResultListener(casListener));
         * </code></pre>
         *
         * @param listener LWTResultListener
         * @return BuiltOptions
         */
        public BuiltOptions LWTResultListener(LWTResultListener listener) {
            super.LWTResultListenerO = Optional.fromNullable(listener);
            return this;
        }

        /**
         * Use LWT conditions for UPDATE operations. This has no effect on statements other than UPDATE
         *
         * <pre class="code"><code class="java">
         *
         * Options options = OptionsBuilder.ifConditions(Arrays.asList(
         *              new LWTCondition("name","John"),
         *              new LWTCondition("age_in_years",33L));
         * </code></pre>
         *
         * @param LWTConditions varargs of LWTConditions
         * @return BuiltOptions
         */
        public BuiltOptions ifConditions(LWTCondition... LWTConditions) {
            super.LWTConditions = Arrays.asList(LWTConditions);
            return this;
        }

        /**
         * Use LWT conditions for UPDATE operations. This has no effect on statements other than UPDATE
         *
         * <pre class="code"><code class="java">
         *
         * Options options = OptionsBuilder.ifConditions(Arrays.asList(
         *              new LWTCondition("name","John"),
         *              new LWTCondition("age_in_years",33L));
         * </code></pre>
         *
         * @param LWTConditions list of LWTConditions
         * @return BuiltOptions
         */
        public BuiltOptions ifConditions(List<LWTCondition> LWTConditions) {
            super.LWTConditions = LWTConditions;
            return this;
        }

        /**
         * Force LOCAL_SERIAL consistency for all LWT operations.
         * By default LWT operations are performed using SERIAL serial consistency level
         * @return BuiltOptions
         */
        public BuiltOptions LWTLocalSerial() {
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
        BuiltOptions LWTLocalSerial(boolean localSerial) {
            if (localSerial) {
                super.serialConsistencyO = Optional.fromNullable(com.datastax.driver.core.ConsistencyLevel.LOCAL_SERIAL);
            }
            return this;
        }
    }

}
