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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import com.datastax.driver.core.ExecutionInfo;
import com.google.common.util.concurrent.Uninterruptibles;

import info.archinnov.achilles.internals.dsl.AsyncAware;
import info.archinnov.achilles.type.tuples.Tuple2;

public interface SelectJSONAction extends AsyncAware {

    /**
     * Execute the SELECT JSON * action
     * and return an {@link java.util.Iterator}&lt;String&gt; of JSON values
     * <br/>
     * WARNING: <strong>this method performs a blocking call to the underlying async query</strong>
     */
    Iterator<String> iterator();

    /**
     * Execute the SELECT JSON * action
     * and return a {@link info.archinnov.achilles.type.tuples.Tuple2}<{@link java.util.Iterator}&lt;String&gt;, {@link com.datastax.driver.core.ExecutionInfo}>
     * <br/>
     * WARNING: <strong>this method performs a blocking call to the underlying async query</strong>
     */
    Tuple2<Iterator<String>, ExecutionInfo> iteratorWithExecutionInfo();

    /**
     * Execute the SELECT JSON * action
     * and return the first row value as JSON
     */
    default String getJSON() {
        try {
            return Uninterruptibles.getUninterruptibly(getJSONAsync());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT JSON * action
     * and return the first row as JSON with {@link com.datastax.driver.core.ExecutionInfo}
     */
    default Tuple2<String, ExecutionInfo> getJSONWithStats() {
        try {
            return Uninterruptibles.getUninterruptibly(getJSONAsyncWithStats());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT JSON * action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture} of
     * the first row as JSON
     */
    default CompletableFuture<String> getJSONAsync() {
        return getListJSONAsync().thenApply(list -> list.size() > 0 ? list.get(0) : null);
    }

    /**
     * Execute the SELECT JSON * action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture} of
     * the first row as JSON with {@link com.datastax.driver.core.ExecutionInfo}
     */
    default CompletableFuture<Tuple2<String, ExecutionInfo>> getJSONAsyncWithStats() {
        return getListJSONAsyncWithStats().thenApply(tuple2 -> tuple2._1().size() > 0
                ? Tuple2.of(tuple2._1().get(0), tuple2._2())
                : Tuple2.of(null, tuple2._2()));
    }

    /**
     * Execute the SELECT JSON * action
     * and return a list of rows as JSON
     */
    default List<String> getListJSON() {
        try {
            return Uninterruptibles.getUninterruptibly(getListJSONAsync());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT JSON * action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture} of
     * a list of rows as JSON
     */
    default CompletableFuture<List<String>> getListJSONAsync() {
        return getListJSONAsyncWithStats().thenApply(Tuple2::_1);
    }


    /**
     * Execute the SELECT JSON * action
     * and return a list of rows as JSON with {@link com.datastax.driver.core.ExecutionInfo}
     */
    default Tuple2<List<String>, ExecutionInfo> getListJSONWithStats() {
        try {
            return Uninterruptibles.getUninterruptibly(getListJSONAsyncWithStats());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT JSON * action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture} of
     * a list of entity instances with {@link com.datastax.driver.core.ExecutionInfo}
     */
    CompletableFuture<Tuple2<List<String>, ExecutionInfo>> getListJSONAsyncWithStats();
}
