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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.Uninterruptibles;

import info.archinnov.achilles.type.TypedMap;
import info.archinnov.achilles.type.tuples.Tuple2;

public interface TypedMapAware extends AsyncAware {

    /**
     * Execute the SELECT action and return an {@link java.util.Iterator}<{@link info.archinnov.achilles.type.TypedMap}>
     * <br/>
     * WARNING: <strong>this method performs a blocking call to the underlying async query</strong>
     */
    Iterator<TypedMap> typedMapIterator();

    /**
     * Execute the SELECT action and return a {@link info.archinnov.achilles.type.tuples.Tuple2}&lt;{@link java.util.Iterator}&lt;{@link info.archinnov.achilles.type.TypedMap}&gt;, {@link com.datastax.driver.core.ExecutionInfo}&gt;
     * <br/>
     * WARNING: <strong>this method performs a blocking call to the underlying async query</strong>
     */
    Tuple2<Iterator<TypedMap>, ExecutionInfo> typedMapIteratorWithExecutionInfo();

    /**
     * Execute the SELECT action and return a {@link java.util.concurrent.CompletableFuture}&lt;{@link info.archinnov.achilles.type.tuples.Tuple2}&lt;
     * {@link java.util.List}&lt;{@link info.archinnov.achilles.type.TypedMap}&gt;, {@link com.datastax.driver.core.ExecutionInfo}&gt;&gt;
     * <br/>
     */
    CompletableFuture<Tuple2<List<TypedMap>, ExecutionInfo>> getTypedMapsAsyncWithStats();

    /**
     * Execute the SELECT action and return an {@link java.util.concurrent.CompletableFuture}&lt;
     * {@link java.util.List}&lt;{@link info.archinnov.achilles.type.TypedMap}&gt;&gt;
     * <br/>
     */

    default CompletableFuture<List<TypedMap>> getTypedMapsAsync() {
        return getTypedMapsAsyncWithStats()
                .thenApply(Tuple2::_1);
    }

    /**
     * Execute the SELECT action and return a {@link info.archinnov.achilles.type.tuples.Tuple2}&lt;
     * {@link java.util.List}&lt;{@link info.archinnov.achilles.type.TypedMap}&gt;, {@link com.datastax.driver.core.ExecutionInfo}&gt;
     * <br/>
     */
    default Tuple2<List<TypedMap>, ExecutionInfo> getTypedMapsWithStats() {
        try {
            return Uninterruptibles.getUninterruptibly(getTypedMapsAsyncWithStats());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT action and return a
     * {@link java.util.List}&lt;{@link info.archinnov.achilles.type.TypedMap}&gt;
     * <br/>
     */
    default List<TypedMap> getTypedMaps() {
        try {
            return Uninterruptibles.getUninterruptibly(getTypedMapsAsync());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT action and return a {@link java.util.concurrent.CompletableFuture}&lt;{@link info.archinnov.achilles.type.tuples.Tuple2}&lt;
     * {@link info.archinnov.achilles.type.TypedMap}, {@link com.datastax.driver.core.ExecutionInfo}&gt;&gt;
     * <br/>
     */
    CompletableFuture<Tuple2<TypedMap, ExecutionInfo>> getTypedMapAsyncWithStats();


    default CompletableFuture<TypedMap> getTypedMapAsync() {
        return getTypedMapAsyncWithStats()
                .thenApply(Tuple2::_1);
    }

    /**
     * Execute the SELECT action and return a {@link info.archinnov.achilles.type.tuples.Tuple2}&lt;
     * {@link info.archinnov.achilles.type.TypedMap}, {@link com.datastax.driver.core.ExecutionInfo}&gt;
     * <br/>
     */
    default Tuple2<TypedMap, ExecutionInfo> getTypedMapWithStats() {
        try {
            return Uninterruptibles.getUninterruptibly(getTypedMapAsyncWithStats());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Execute the SELECT action and return a {@link info.archinnov.achilles.type.TypedMap}
     * <br/>
     */
    default TypedMap getTypedMap() {
        try {
            return Uninterruptibles.getUninterruptibly(getTypedMapAsync());
        } catch (ExecutionException e) {
            throw extractCauseFromExecutionException(e);
        }
    }

    /**
     * Map a list of {@link com.datastax.driver.core.ResultSet} objects to a list
     * of {@link info.archinnov.achilles.type.TypedMap}
     */
    default List<TypedMap> mapResultSetToTypedMaps(ResultSet resultSet) {
        final List<TypedMap> result = new ArrayList<>();

        IntStream.range(0, resultSet.getAvailableWithoutFetching())
                .forEach(index -> result.add(mapRowToTypedMap(resultSet.one())));
        return result;
    }

    /**
     * Map the {@link com.datastax.driver.core.ResultSet} object to an instance
     * of {@link info.archinnov.achilles.type.TypedMap}
     */
    default TypedMap mapRowToTypedMap(Row row) {
        final TypedMap typedMap = new TypedMap();
        if (row != null) {
            for (ColumnDefinitions.Definition def : row.getColumnDefinitions().asList()) {
                final String cqlColumn = def.getName();
                typedMap.put(cqlColumn, row.getObject(cqlColumn));
            }
        }
        return typedMap;
    }
}
