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

package info.archinnov.achilles.internals.metamodel.functions;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Utility interface for type-safe function
 * call handling with Achilles. Not meant to be used
 * by end-users
 */
public interface FunctionCall {

    List<Object> EMPTY = new ArrayList<>();

    default String functionName() {
        return null;
    }

    default List<Object> parameters() {
        return EMPTY;
    }

    default Object buildRecursive() {
        final List<Object> transformed = parameters()
                .stream()
                .map(x -> x instanceof FunctionCall ? ((FunctionCall) x).buildRecursive() : x)
                .collect(Collectors.toList());

        if (functionName().equals("cast") && transformed.size()==1) {
            return QueryBuilder.cast(QueryBuilder.raw(transformed.get(0).toString()), this.targetCQLTypeName());
        } else {
            return QueryBuilder.fcall(functionName(), transformed.toArray());
        }
    }

    boolean isFunctionCall();

    default DataType targetCQLTypeName() {
        throw new IllegalStateException("Unexpected call. This method should only be called by system 'cast' functions");
    }

    default void addToSelect(Select.Selection selection, String alias) {
        final List<Object> parameters = parameters();

        if (functionName().equals("cast") && parameters.size()==1) {
            selection.cast(QueryBuilder.raw(parameters.get(0).toString()), targetCQLTypeName()).as(alias);

        } else {
            selection.fcall(functionName(), parameters.toArray()).as(alias);
        }
    }
}
