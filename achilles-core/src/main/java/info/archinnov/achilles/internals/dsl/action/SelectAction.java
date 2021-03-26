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

public interface SelectAction<ENTITY> extends AsyncAware {

    /**
     * Execute the SELECT action
     * and return an {@link java.util.Iterator}&lt;ENTITY&gt; of entity instances
     * <br/>
     * WARNING: <strong>this method performs a blocking call to the underlying async query</strong>
     */
    Iterator<ENTITY> iterator();

    /**
     * Execute the SELECT action
     * and return a {@link info.archinnov.achilles.type.tuples.Tuple2}<{@link java.util.Iterator}&lt;ENTITY&gt;, {@link com.datastax.driver.core.ExecutionInfo}>
     * <br/>
     * WARNING: <strong>this method performs a blocking call to the underlying async query</strong>
     */
    Tuple2<Iterator<ENTITY>, ExecutionInfo> iteratorWithExecutionInfo();

    /**
     * Execute the SELECT action
     * and return the first entity instance
     */
    default ENTITY getOne() {
        try {
            return Uninterruptibles.getUninterruptibly(getOneAsync());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT action
     * and return the first entity instance with {@link com.datastax.driver.core.ExecutionInfo}
     */
    default Tuple2<ENTITY, ExecutionInfo> getOneWithStats() {
        try {
            return Uninterruptibles.getUninterruptibly(getOneAsyncWithStats());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture} of
     * the first entity instance
     */
    default CompletableFuture<ENTITY> getOneAsync() {
        return getListAsync().thenApply(list -> list.size() > 0 ? list.get(0) : null);
    }

    /**
     * Execute the SELECT action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture} of
     * the first entity instance with {@link com.datastax.driver.core.ExecutionInfo}
     */
    default CompletableFuture<Tuple2<ENTITY, ExecutionInfo>> getOneAsyncWithStats() {
        return getListAsyncWithStats().thenApply(tuple2 -> tuple2._1().size() > 0
                ? Tuple2.of(tuple2._1().get(0), tuple2._2())
                : Tuple2.of(null, tuple2._2()));
    }

    /**
     * Execute the SELECT action
     * and return a list of entity instances
     */
    default List<ENTITY> getList() {
        try {
            return Uninterruptibles.getUninterruptibly(getListAsync());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture} of
     * a list of entity instances
     */
    default CompletableFuture<List<ENTITY>> getListAsync() {
        return getListAsyncWithStats().thenApply(Tuple2::_1);
    }

    default Tuple2<List<ENTITY>, ExecutionInfo> getListWithStats() {
        try {
            return Uninterruptibles.getUninterruptibly(getListAsyncWithStats());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT action asynchronously
     * and return a {@link java.util.concurrent.CompletableFuture} of
     * a list of entity instances with {@link com.datastax.driver.core.ExecutionInfo}
     */
    CompletableFuture<Tuple2<List<ENTITY>, ExecutionInfo>> getListAsyncWithStats();
}
