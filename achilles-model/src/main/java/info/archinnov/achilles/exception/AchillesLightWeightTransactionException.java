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
package info.archinnov.achilles.exception;

import static info.archinnov.achilles.type.lightweighttransaction.LWTResultListener.LWTResult.LWTOperation;

import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.lightweighttransaction.LWTResultListener;

public class AchillesLightWeightTransactionException extends AchillesException {
    private static final long serialVersionUID = 1L;
    private final LWTResultListener.LWTResult LWTResult;

    public AchillesLightWeightTransactionException(LWTResultListener.LWTResult LWTResult) {
        super(LWTResult.toString());
        this.LWTResult = LWTResult;
    }

    public LWTOperation operation() {
        return LWTResult.operation();
    }

    public TypedMap currentValues() {
        return LWTResult.currentValues();
    }

    @Override
    public String toString() {
        return LWTResult.toString();
    }
}


