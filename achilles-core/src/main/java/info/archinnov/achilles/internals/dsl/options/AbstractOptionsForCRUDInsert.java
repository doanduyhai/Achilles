/*
 * Copyright (C) 2012-2021 DuyHai DOAN
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package info.archinnov.achilles.internals.dsl.options;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.types.OverridingOptional;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;
import info.archinnov.achilles.type.strategy.InsertStrategy;

public abstract class AbstractOptionsForCRUDInsert<T extends AbstractOptionsForCRUDInsert<T>>
        extends AbstractOptionsForSelect<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractOptionsForCRUDInsert.class);

    protected Optional<InsertStrategy> insertStrategy = Optional.empty();
    protected Optional<Boolean> ifNotExists = Optional.empty();
    protected Optional<List<LWTResultListener>> lwtResultListeners = Optional.empty();

    /**
     * Generate a ... <strong>IF NOT EXISTS</strong> if true
     */
    public T ifNotExists(boolean ifNotExists) {
        this.ifNotExists = Optional.of(ifNotExists);
        return getThis();
    }

    /**
     * Generate a ... <strong>IF NOT EXISTS</strong>
     */
    public T ifNotExists() {
        this.ifNotExists = Optional.of(true);
        return getThis();
    }

    /**
     * Add a list of LWT result listeners. Example of usage:
     * <pre class="code"><code class="java">
     * LWTResultListener lwtListener = new LWTResultListener() {
     *
     *  public void onError(LWTResult lwtResult) {
     *
     *      //Get type of LWT operation that fails
     *      LWTResult.Operation operation = lwtResult.operation();
     *
     *      // Print out current values
     *      TypedMap currentValues = lwtResult.currentValues();
     *      currentValues
     *          .entrySet()
     *          .forEach(entry -> System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue())));
     *  }
     * };
     * </code></pre>
     */
    public T withLwtResultListeners(List<LWTResultListener> lwtResultListeners) {
        this.lwtResultListeners = Optional.of(lwtResultListeners);
        return getThis();
    }

    /**
     * Add a single LWT result listeners. Example of usage:
     * <pre class="code"><code class="java">
     * LWTResultListener lwtListener = new LWTResultListener() {
     *
     *  public void onError(LWTResult lwtResult) {
     *
     *      //Get type of LWT operation that fails
     *      LWTResult.Operation operation = lwtResult.operation();
     *
     *      // Print out current values
     *      TypedMap currentValues = lwtResult.currentValues();
     *          currentValues
     *          .entrySet()
     *          .forEach(entry -> System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue())));
     *       }
     * };
     * </code></pre>
     */
    public T withLwtResultListener(LWTResultListener lwtResultListener) {
        this.lwtResultListeners = Optional.of(asList(lwtResultListener));
        return getThis();
    }

    /**
     * Generate a <strong>USING TIMESTAMP ?</strong>
     */
    public T usingTimestamp(long timestamp) {
        getOptions().setDefaultTimestamp(Optional.of(timestamp));
        return getThis();
    }

    /**
     * Generate a <strong>USING TTL ?</strong>
     */
    public T usingTimeToLive(int timeToLive) {
        getOptions().setTimeToLive(Optional.of(timeToLive));
        return getThis();
    }

    /**
     * Bind values to prepared statement and avoid null if
     * InsertStrategy.NOT_NULL_FIELDS is chosen
     */
    public T withInsertStrategy(InsertStrategy insertStrategy) {
        this.insertStrategy = Optional.of(insertStrategy);
        return getThis();
    }

    /**
     * Determine the insert strategy for the given
     * entity using static configuration and runtime option
     *
     * @param property entity property
     * @return overriden InsertStrategy
     */
    public InsertStrategy getOverridenStrategy(AbstractEntityProperty<?> property) {

        final InsertStrategy insertStrategy = OverridingOptional
                .from(this.insertStrategy)
                .defaultValue(property.insertStrategy())
                .get();

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Get runtime insert strategy for entity %s : %s",
                    property.entityClass.getCanonicalName(), insertStrategy.name()));
        }

        return insertStrategy;
    }
}
