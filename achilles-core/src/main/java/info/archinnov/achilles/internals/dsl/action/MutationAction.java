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

package info.archinnov.achilles.internals.dsl.action;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ExecutionInfo;
import com.google.common.util.concurrent.Uninterruptibles;

import info.archinnov.achilles.internals.dsl.AsyncAware;
import info.archinnov.achilles.type.Empty;

public interface MutationAction extends AsyncAware {

    /**
     * Execute the INSERT/UPDATE/DELETE action
     */
    default void execute() {
        try {
            Uninterruptibles.getUninterruptibly(executeAsync());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the INSERT/UPDATE/DELETE action
     * and return an {@link com.datastax.driver.core.ExecutionInfo} object
     */
    default ExecutionInfo executeWithStats() {
        try {
            return Uninterruptibles.getUninterruptibly(executeAsyncWithStats());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the INSERT/UPDATE/DELETE action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture}
     * of {@link info.archinnov.achilles.type.Empty} object.
     * The Empty object is here to avoid returning <strong>null</strong>
     */
    default CompletableFuture<Empty> executeAsync() {
        return executeAsyncWithStats()
                .thenApply(x -> Empty.INSTANCE);
    }

    /**
     * Execute the INSERT/UPDATE/DELETE action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture}
     * of {@link com.datastax.driver.core.ExecutionInfo} object.
     */
    CompletableFuture<ExecutionInfo> executeAsyncWithStats();

}
