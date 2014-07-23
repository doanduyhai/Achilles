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
 * Interface to define listener for CAS operations.
 * <br>
 * The "<em>void onCASSuccess()</em>" method is called when CAS operation is successful
 * <br>
 * <br>
 * The "<em>void onCASError(CASResult casResult)</em>" method is called if CAS operation fails for some reasons.
 * The failure details can be extracted from the {@link info.archinnov.achilles.listener.CASResultListener.CASResult}
 * class
 * <br>
 * <br>
 * Below is an example of usage for CAS result listener
 *
 * <pre class="code"><code class="java">
 *
 * CASResultListener casListener = new CASResultListener() {
 *
 *     public void onCASSuccess() {
 *         // Do something on success
 *     }
 *
 *     public void onCASError(CASResult casResult) {
 *
 *         //Get type of CAS operation that fails
 *         CASResult.Operation operation = casResult.operation();
 *
 *         // Print out current values
 *         TypedMap currentValues = casResult.currentValues();
 *         for(Entry<String,Object> entry: currentValues.entrySet()) {
 *             System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue()));
 *         }
 *     }
 * };
 *
 *  persistenceManager.update(user, OptionsBuilder.
 *         ifConditions(Arrays.asList(
 *             new CASCondition("login","jdoe")))
 *         .casResultListener(casListener));
 *
 * </code></pre>
 *
 * <p/>
 *
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Distributed-CAS#cas-result-listener" target="_blank">CAS Result Listeners</a>
 */
public interface CASResultListener {

    void onCASSuccess();

    void onCASError(CASResult casResult);


    /**
     * <p/>
     * POJO class keeping information on a CAS operation.
     * <br>
     * The "<em>public Operation operation()</em>" method returns the type of CAS operation. Possibles
     * values are {@link info.archinnov.achilles.listener.CASResultListener.CASResult.Operation}.INSERT
     * and {@link info.archinnov.achilles.listener.CASResultListener.CASResult.Operation}.UPDATE
     * <br>
     * <br>
     * The "<em>public TypedMap currentValues()</em>" method returns a {@link info.archinnov.achilles.type.TypedMap}
     * of current value for each column involved in the CAS operation
     * <br>
     * <br>
     * Below is an example of usage for CASResult
     *
     * <pre class="code"><code class="java">
     *
     * CASResultListener casListener = new CASResultListener() {
     *
     *     public void onCASSuccess() {
     *         // Do something on success
     *     }
     *
     *     public void onCASError(CASResult casResult) {
     *
     *         //Get type of CAS operation that fails
     *         CASResult.Operation operation = casResult.operation();
     *
     *         // Print out current values
     *         TypedMap currentValues = casResult.currentValues();
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
    public static class CASResult {
        private final Operation operation;
        private final TypedMap currentValues;

        public CASResult(Operation operation, TypedMap currentValues) {
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
