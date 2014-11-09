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
package info.archinnov.achilles.query.slice;

import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.base.Function;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;

import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.google.common.collect.FluentIterable.from;

public enum BoundingMode {

	INCLUSIVE_BOUNDS(true, true),
    EXCLUSIVE_BOUNDS(false, false),
    INCLUSIVE_START_BOUND_ONLY(true, false),
    INCLUSIVE_END_BOUND_ONLY(false, true);

    private static final Function<String, Object> FROM_NAME_TO_BIND_MARKER = new Function<String, Object>() {
        @Override
        public BindMarker apply(String name) {
            return bindMarker(name);
        }
    };

    private final boolean inclusiveStart;
    private final boolean inclusiveEnd;

    BoundingMode(boolean inclusiveStart, boolean inclusiveEnd) {
        this.inclusiveStart = inclusiveStart;
        this.inclusiveEnd = inclusiveEnd;
    }

    public void buildFromClusteringKeys(Where where,ClusteringOrder clusteringOrder, List<String> fromClusteringKeysName) {
        if (clusteringOrder.getSorting() == Sorting.ASC) {
            if (inclusiveStart) {
                where.and(gte(fromClusteringKeysName, from(fromClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
            } else {
                where.and(gt(fromClusteringKeysName, from(fromClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
            }
        } else {
            if (inclusiveStart) {
                where.and(lte(fromClusteringKeysName, from(fromClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
            } else {
                where.and(lt(fromClusteringKeysName, from(fromClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
            }
        }
    }

    public void buildToClusteringKeys(Where where,ClusteringOrder clusteringOrder, List<String> toClusteringKeysName) {
        if (clusteringOrder.getSorting() == Sorting.ASC) {
            if (inclusiveEnd) {
                where.and(lte(toClusteringKeysName, from(toClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
            } else {
                where.and(lt(toClusteringKeysName, from(toClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
            }
        } else {
            if (inclusiveEnd) {
                where.and(gte(toClusteringKeysName, from(toClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
            } else {
                where.and(gt(toClusteringKeysName, from(toClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
            }
        }
    }
}
