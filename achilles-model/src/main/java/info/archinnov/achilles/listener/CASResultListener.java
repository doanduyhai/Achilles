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

public interface CASResultListener {

    public void onCASSuccess();

    public void onCASError(CASResult casResult);

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
