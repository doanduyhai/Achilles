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

package info.archinnov.achilles.listener;

import info.archinnov.achilles.type.TypedMap;

/**
 * <p/>
 * Interface to define listener for LWT operations.
 * <br>
 * The "<em>void onSuccess()</em>" method is called when LWT operation is successful
 * <br>
 * <br>
 * The "<em>void onError(LWTResult lwtResult)</em>" method is called if LWT operation fails for some reasons.
 * The failure details can be extracted from the {@link info.archinnov.achilles.listener.LWTResultListener.LWTResult}
 * class
 * <br>
 * <br>
 * Below is an example of usage for LWT result listener
 *
 * <pre class="code"><code class="java">
 *
 * LWTResultListener lwtListener = new LWTResultListener() {
 *
 *     public void onSuccess() {
 *         // Do something on success
 *     }
 *
 *     public void onError(LWTResult lwtResult) {
 *
 *         //Get type of LWT operation that fails
 *         LWTResult.Operation operation = lwtResult.operation();
 *
 *         // Print out current values
 *         TypedMap currentValues = lwtResult.currentValues();
 *         for(Entry<String,Object> entry: currentValues.entrySet()) {
 *             System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue()));
 *         }
 *     }
 * };
 *
 *  persistenceManager.update(user, OptionsBuilder
 *         .ifEqualCondition("login","jdoe")
 *         .LWTResultListener(lwtListener));
 *
 * </code></pre>
 *
 * <p/>
 *
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Distributed-CAS#cas-result-listener" target="_blank">CAS Result Listeners</a>
 */
public interface LWTResultListener {

    void onSuccess();

    void onError(LWTResult lwtResult);


    /**
     * <p/>
     * POJO class keeping information on a LWT operation.
     * <br>
     * The "<em>public Operation operation()</em>" method returns the type of LWT operation. Possibles
     * values are {@link info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation}.INSERT
     * and {@link info.archinnov.achilles.listener.LWTResultListener.LWTResult.Operation}.UPDATE
     * <br>
     * <br>
     * The "<em>public TypedMap currentValues()</em>" method returns a {@link info.archinnov.achilles.type.TypedMap}
     * of current value for each column involved in the LWT operation
     * <br>
     * <br>
     * Below is an example of usage for LWTResult
     *
     * <pre class="code"><code class="java">
     *
     * LWTResultListener LWTListener = new LWTResultListener() {
     *
     *     public void onSuccess() {
     *         // Do something on success
     *     }
     *
     *     public void onError(LWTResult lwtResult) {
     *
     *         //Get type of LWT operation that fails
     *         LWTResult.Operation operation = lwtResult.operation();
     *
     *         // Print out current values
     *         TypedMap currentValues = lwtResult.currentValues();
     *         for(Entry<String,Object> entry: currentValues.entrySet()) {
     *             System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue()));
     *         }
     *     }
     * };
     * <p/>
     *
     *
     * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Custom-Types#casresultlistener" target="_blank">CASResult</a>
     */
    public static class LWTResult {
        private final Operation operation;
        private final TypedMap currentValues;

        public LWTResult(Operation operation, TypedMap currentValues) {
            this.operation = operation;
            this.currentValues = currentValues;
        }

        public Operation operation() {
            return operation;
        }

        public TypedMap currentValues() {
            return currentValues;
        }

        @Override
        public String toString() {
            return String.format("CAS operation %s cannot be applied. Current values are: %s", operation, currentValues);
        }

        public static enum Operation {INSERT, UPDATE}
    }
}
