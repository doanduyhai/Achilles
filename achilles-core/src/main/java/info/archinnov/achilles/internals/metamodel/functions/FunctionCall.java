/*
 * Copyright (C) 2012-2016 DuyHai DOAN
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

        //TODO Ugly hack for https://datastax-oss.atlassian.net/projects/JAVA/issues/JAVA-1086
        if (functionName().equals("cast") && transformed.size()==1) {
            return QueryBuilder.raw("cast(" + transformed.get(0).toString() + " as " + this.targetCQLTypeName()+ ")");
        } else {
            return QueryBuilder.fcall(functionName(), transformed.toArray());
        }
    }

    boolean isFunctionCall();

    default String targetCQLTypeName() {
        throw new IllegalStateException("Unexpected call. This method should only be called by system 'cast' functions");
    }

    default void addToSelect(Select.Selection selection, String alias) {
        final List<Object> parameters = parameters();

        //TODO Ugly hack for https://datastax-oss.atlassian.net/projects/JAVA/issues/JAVA-1086
        if (functionName().equals("cast") && parameters.size()==1) {
            ((Select.SelectionOrAlias)selection).raw("cast(" + parameters.get(0).toString() + " as " + this.targetCQLTypeName()+ ")").as(alias);
        } else {
            selection.fcall(functionName(), parameters.toArray()).as(alias);
        }
    }
}
