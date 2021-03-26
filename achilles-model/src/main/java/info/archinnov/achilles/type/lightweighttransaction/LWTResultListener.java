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

package info.archinnov.achilles.type.lightweighttransaction;

import static java.lang.String.format;

import info.archinnov.achilles.type.TypedMap;

/**

 * Interface to define listener for LWT operations.
 * <br>
 * The "<em>default void onSuccess(){}</em>" method is called when LWT operation is successful. By default, it does nothing.
 * Override it to implement your own behavior
 * <br>
 * <br>
 * The "<em>void onError(LWTResult lwtResult)</em>" method is called if LWT operation fails for some reasons.
 * The failure details can be extracted from the {@link LWTResult}
 * class
 * <br>
 * <br>
 * Below is an example of usage for LWT result listener
 * <pre class="code"><code class="java">
 * LWTResultListener lwtListener = new LWTResultListener() {

 * public void onError(LWTResult lwtResult) {

 * //Get type of LWT operation that fails
 * LWTResult.Operation operation = lwtResult.operation();

 * // Print out current values
 * TypedMap currentValues = lwtResult.currentValues();
 * currentValues
 * .entrySet()
 * .forEach(entry -> System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue())));
 * }
 * };

 * manager
 * .dsl()
 * .update()
 * .fromBaseTable()
 * .firstname_Set("new firstname")
 * .where()
 * .login_Eq("jdoe")
 * <strong>.ifFirstname_Eq("old firstname")</strong>
 * <strong>.withLwtResultListener(lwtListener)</strong>
 * .execute();
 * </code></pre>
 *
 * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Distributed-CAS#cas-result-listener" target="_blank">CAS Result Listeners</a>
 */
public interface LWTResultListener {

    default void onSuccess() {
        // Do nothing
    }

    void onError(LWTResult lwtResult);


    /**

     * POJO class keeping information on a LWT operation.
     * <br>
     * The "<em>public Operation operation()</em>" method returns the type of LWT operation. Possibles
     * values are {@link LWTOperation}.INSERT
     * and {@link LWTOperation}.UPDATE
     * <br>
     * <br>
     * The "<em>public TypedMap currentValues()</em>" method returns a {@link TypedMap}
     * of current value for each column involved in the LWT operation
     * <br>
     * <br>
     * Below is an example of usage for LWTResult
     * <pre class="code"><code class="java">

     * LWTResultListener LWTListener = new LWTResultListener() {

     * public void onSuccess() {
     * // Do something on success
     * }

     * public void onError(LWTResult lwtResult) {

     * //Get type of LWT operation that fails
     * LWTResult.Operation operation = lwtResult.operation();

     * // Print out current values
     * TypedMap currentValues = lwtResult.currentValues();
     * currentValues
     * .entrySet()
     * .forEach(entry -> System.out.println(String.format("%s = %s",entry.getKey(), entry.getValue())));
     * }
     * }
     *
     * @see <a href="https://github.com/doanduyhai/Achilles/wiki/Achilles-Custom-Types#casresultlistener" target="_blank">CASResult</a>
     */
    class LWTResult {
        private final LWTOperation lwtOperation;
        private final TypedMap currentValues;

        public LWTResult(LWTOperation lwtOperation, TypedMap currentValues) {
            this.lwtOperation = lwtOperation;
            this.currentValues = currentValues;
        }

        public LWTOperation operation() {
            return lwtOperation;
        }

        public TypedMap currentValues() {
            return currentValues;
        }

        @Override
        public String toString() {
            return format("LightWeight Transaction operation %s cannot be applied. Current values are: %s", lwtOperation, currentValues);
        }

        public enum LWTOperation {INSERT, UPDATE}
    }
}
