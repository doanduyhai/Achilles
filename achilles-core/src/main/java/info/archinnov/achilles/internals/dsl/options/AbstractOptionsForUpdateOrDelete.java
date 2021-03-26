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

import static java.util.Arrays.asList;

import java.util.List;
import java.util.Optional;

import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;

public abstract class AbstractOptionsForUpdateOrDelete<T extends AbstractOptionsForUpdateOrDelete<T>>
        extends AbstractOptionsForSelect<T> {

    protected Optional<List<LWTResultListener>> lwtResultListeners = Optional.empty();

    /**
     * Generate a <strong>USING TIMESTAMP ?</strong>
     */
    public T usingTimestamp(long timestamp) {
        getOptions().setDefaultTimestamp(Optional.of(timestamp));
        return getThis();
    }

    /**
     * Add a list of LWT result listeners. Example of usage:
     * <pre class="code"><code class="java">
     * LWTResultListener lwtListener = new LWTResultListener() {
     *
     *   public void onError(LWTResult lwtResult) {
     *
     *      //Get type of LWT operation that fails
     *      LWTResult.Operation operation = lwtResult.operation();
     *
     *      // Print out current values
     *      TypedMap currentValues = lwtResult.currentValues();
     *      currentValues
     *          .entrySet()
     *          .forEach(entry -> System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue())));
     *   }
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
     *      currentValues
     *          .entrySet()
     *          .forEach(entry -> System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue())));
     *  }
     * };
     * </code></pre>
     */
    public T withLwtResultListener(LWTResultListener lwtResultListener) {
        this.lwtResultListeners = Optional.of(asList(lwtResultListener));
        return getThis();
    }

}
