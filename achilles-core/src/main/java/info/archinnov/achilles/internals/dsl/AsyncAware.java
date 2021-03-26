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

package info.archinnov.achilles.internals.dsl;

import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;

import info.archinnov.achilles.exception.AchillesBeanValidationException;
import info.archinnov.achilles.exception.AchillesException;
import info.archinnov.achilles.exception.AchillesInvalidTableException;
import info.archinnov.achilles.exception.AchillesLightWeightTransactionException;

public interface AsyncAware {

    default RuntimeException extractCauseFromExecutionException(ExecutionException e) {
        final Throwable cause = e.getCause();
        if (cause instanceof DriverException) {
            return ((DriverException) cause).copy();
        } else if (cause instanceof DriverInternalError) {
            return (DriverInternalError) cause;
        } else if (cause instanceof AchillesLightWeightTransactionException) {
            return (AchillesLightWeightTransactionException) cause;
        } else if (cause instanceof AchillesBeanValidationException) {
            return (AchillesBeanValidationException) cause;
        } else if (cause instanceof AchillesInvalidTableException) {
            return (AchillesInvalidTableException) cause;
        } else if (RuntimeException.class.isAssignableFrom(cause.getClass())) {
            return (RuntimeException) cause;
        } else if (cause instanceof AchillesException) {
            return (AchillesException) cause;
        } else {
            return new AchillesException(cause);
        }
    }
}
