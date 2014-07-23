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

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.desc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.google.common.collect.FluentIterable.from;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang.StringUtils.isNotBlank;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import info.archinnov.achilles.internal.metadata.holder.EntityMeta;
import info.archinnov.achilles.internal.validation.Validator;
import info.archinnov.achilles.type.ConsistencyLevel;

public class SliceQueryProperties<T> {

    private static final Logger log = LoggerFactory.getLogger(SliceQueryProperties.class);

    public static final int DEFAULT_LIMIT = 100;
    public static final int DEFAULT_BATCH_SIZE = 100;

    private static final Function<String, Object> FROM_NAME_TO_BIND_MARKER = new Function<String, Object>() {
        @Override
        public BindMarker apply(String name) {
            return bindMarker(name);
        }
    };

    private final EntityMeta entityMeta;
    private final Class<T> entityClass;
    private final SliceType sliceType;

    private Optional<Integer> limitO = Optional.fromNullable(DEFAULT_LIMIT);
    protected Optional<Integer> fetchSizeO = Optional.absent();
    private BoundingMode boundingMode = BoundingMode.INCLUSIVE_BOUNDS;
    private Optional<OrderingMode> orderingModeO = Optional.fromNullable(OrderingMode.ASCENDING);

    private Optional<ConsistencyLevel> consistencyLevelO = Optional.absent();

    private List<Object> partitionKeys = new LinkedList<>();
    private List<String> partitionKeysName = new LinkedList<>();
    private List<Object> partitionKeysIn = new LinkedList<>();
    private String lastPartitionKeyName;
    private List<Object> fromClusteringKeys = new LinkedList<>();
    private List<String> fromClusteringKeysName = new LinkedList<>();
    private List<Object> toClusteringKeys = new LinkedList<>();
    private List<String> toClusteringKeysName = new LinkedList<>();
    private List<Object> withClusteringKeys = new LinkedList<>();
    private List<String> withClusteringKeysName = new LinkedList<>();
    private List<Object> clusteringsKeysIn = new LinkedList<>();
    private String lastClusteringKeyName;

    private ClusteringOrder clusteringOrder;

    private SliceQueryProperties(EntityMeta entityMeta, Class<T> entityClass, SliceType sliceType) {
        this.entityMeta = entityMeta;
        this.entityClass = entityClass;
        this.sliceType = sliceType;
        this.clusteringOrder = entityMeta.getClusteringOrders().get(0);
    }

    public static <T> SliceQueryProperties<T> builder(EntityMeta entityMeta, Class<T> entityClass, SliceType sliceType) {
        return new SliceQueryProperties<>(entityMeta, entityClass, sliceType);
    }

    protected SliceQueryProperties limit(int limit) {
        Validator.validateTrue(limit > 0, "The limit '%s' should be strictly positive", limit);
        this.limitO = Optional.fromNullable(limit);
        return this;
    }

    protected SliceQueryProperties disableLimit() {
        this.limitO = Optional.absent();
        return this;
    }

    protected SliceQueryProperties fetchSize(int fetchSize) {
        Validator.validateTrue(fetchSize > 0, "The fetchSize '%s' should be strictly positive", fetchSize);
        this.fetchSizeO = Optional.fromNullable(fetchSize);
        if (CollectionUtils.isNotEmpty(partitionKeysIn)) {
            this.orderingModeO = Optional.absent();
            log.warn("Cannot page queries with both ORDER BY and a IN restriction on the partition key; you must either remove the ORDER BY or the IN and sort client side, or disable paging for this query");
        }
        return this;
    }

    protected SliceQueryProperties bounding(BoundingMode boundingMode) {
        this.boundingMode = boundingMode;
        return this;
    }

    protected SliceQueryProperties ordering(OrderingMode orderingMode) {
        this.orderingModeO = Optional.fromNullable(orderingMode);
        return this;
    }

    protected SliceQueryProperties consistency(ConsistencyLevel consistencyLevel) {
        Validator.validateNotNull(consistencyLevel, "The consistency level should not be null");
        this.consistencyLevelO = Optional.fromNullable(consistencyLevel);
        return this;
    }

    protected SliceQueryProperties partitionKeys(List<Object> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    protected SliceQueryProperties partitionKeysName(List<String> partitionKeysName) {
        this.partitionKeysName = partitionKeysName;
        return this;
    }

    protected SliceQueryProperties partitionKeysIn(List<Object> partitionKeysIn) {
        this.partitionKeysIn = partitionKeysIn;
        return this;
    }

    protected SliceQueryProperties lastPartitionKeyName(String lastPartitionKeyName) {
        this.lastPartitionKeyName = lastPartitionKeyName;
        return this;
    }

    protected SliceQueryProperties fromClusteringKeys(List<Object> fromClusteringKeys) {
        this.fromClusteringKeys = fromClusteringKeys;
        return this;
    }

    protected SliceQueryProperties fromClusteringKeysName(List<String> fromClusteringKeysName) {
        this.fromClusteringKeysName = fromClusteringKeysName;
        return this;
    }

    protected SliceQueryProperties toClusteringKeys(List<Object> toClusteringKeys) {
        this.toClusteringKeys = toClusteringKeys;
        return this;
    }

    protected SliceQueryProperties toClusteringKeysName(List<String> toClusteringKeysName) {
        this.toClusteringKeysName = toClusteringKeysName;
        return this;
    }

    protected SliceQueryProperties withClusteringKeys(List<Object> withClusteringKeys) {
        this.withClusteringKeys = withClusteringKeys;
        return this;
    }

    protected SliceQueryProperties withClusteringKeysName(List<String> withClusteringKeysName) {
        this.withClusteringKeysName = withClusteringKeysName;
        return this;
    }

    protected SliceQueryProperties andClusteringKeysIn(List<Object> clusteringsKeysIn) {
        this.clusteringsKeysIn = clusteringsKeysIn;
        return this;
    }

    protected SliceQueryProperties lastClusteringKeyName(String lastClusteringKeyName) {
        this.lastClusteringKeyName = lastClusteringKeyName;
        return this;
    }

    // Public access

    public RegularStatement generateWhereClauseForSelect(Select from) {
        final Select.Where where = from.where();

        // Partition keys
        for (String partitionKeyName : partitionKeysName) {
            where.and(eq(partitionKeyName, bindMarker(partitionKeyName)));
        }

        if (isNotBlank(lastPartitionKeyName)) {
            where.and(in(lastPartitionKeyName, bindMarker("partitionComponentsIn")));
        }

        // Clustering keys
        if (isNotEmpty(withClusteringKeys)) {
            for (String withClusteringName : withClusteringKeysName) {
                where.and(eq(withClusteringName, bindMarker(withClusteringName)));
            }

            if (isNotBlank(lastClusteringKeyName)) {
                where.and(in(lastClusteringKeyName, bindMarker("clusteringKeysIn")));
            }
        } else {
            if (isNotEmpty(fromClusteringKeys)) {
                if (boundingMode.isInclusiveStart()) {
                    where.and(gte(fromClusteringKeysName, from(fromClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
                } else {
                    where.and(gt(fromClusteringKeysName, from(fromClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
                }
            }

            if (isNotEmpty(toClusteringKeys)) {
                if (boundingMode.isInclusiveEnd()) {
                    where.and(lte(toClusteringKeysName, from(toClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
                } else {
                    where.and(lt(toClusteringKeysName, from(toClusteringKeysName).transform(FROM_NAME_TO_BIND_MARKER).toList()));
                }
            }
        }

        // ORDER BY
        if (orderingModeO.isPresent()) {

            final Select statement;
            final OrderingMode orderingMode = orderingModeO.get();
            if (orderingMode.isReverse()) {
                statement = where.orderBy(desc(clusteringOrder.getClusteringColumnName()));
            } else {
                statement = where.orderBy(asc(clusteringOrder.getClusteringColumnName()));
            }
            // LIMIT
            if (limitO.isPresent()) {
                statement.limit(bindMarker("limitSize"));
            }

            return statement;
        } else {
            // LIMIT
            if (limitO.isPresent()) {
                where.limit(bindMarker("limitSize"));
            }

            return where;

        }
    }

    public Delete.Where generateWhereClauseForDelete(Delete delete) {
        final Delete.Where where = delete.where();

        // Partition keys
        for (String partitionKeyName : partitionKeysName) {
            where.and(eq(partitionKeyName, bindMarker(partitionKeyName)));
        }

        if (isNotBlank(lastPartitionKeyName)) {
            where.and(in(lastPartitionKeyName, bindMarker("partitionComponentsIn")));
        }

        // Clustering keys
        if (isNotEmpty(withClusteringKeys)) {
            for (String withClusteringName : withClusteringKeysName) {
                where.and(eq(withClusteringName, bindMarker(withClusteringName)));
            }
        }
        return where;
    }

    public Object[] getBoundValues() {
        List<Object> boundValues = new LinkedList<>();
        // Partition keys
        boundValues.addAll(partitionKeys);

        if (isNotEmpty(partitionKeysIn)) {
            boundValues.add(partitionKeysIn);
        }

        // Clustering keys
        if (isNotEmpty(withClusteringKeys)) {
            boundValues.addAll(withClusteringKeys);

            if (isNotEmpty(clusteringsKeysIn)) {
                boundValues.add(clusteringsKeysIn);
            }
        } else {
            if (isNotEmpty(fromClusteringKeys)) {
                boundValues.addAll(fromClusteringKeys);
            }

            if (isNotEmpty(toClusteringKeys)) {
                boundValues.addAll(toClusteringKeys);
            }
        }

        // LIMIT
        if (limitO.isPresent()) {
            boundValues.add(limitO.get());
        }

        return boundValues.toArray();
    }

    public void setFetchSizeToStatement(Statement statement) {
        if (fetchSizeO.isPresent()) {
            statement.setFetchSize(fetchSizeO.get());
        }
    }

    public ConsistencyLevel getConsistencyLevelOr(ConsistencyLevel defaultConsistencyLevel) {
        return consistencyLevelO.or(defaultConsistencyLevel);
    }

    public Class<T> getEntityClass() {
        return entityClass;
    }

    public EntityMeta getEntityMeta() {
        return entityMeta;
    }

    public List<Object> getPartitionKeys() {
        return partitionKeys;
    }

    public List<Object> getWithClusteringKeys() {
        return withClusteringKeys;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SliceQueryProperties that = (SliceQueryProperties) o;

        return Objects.equals(this.sliceType, that.sliceType) &&
                Objects.equals(this.entityClass, that.entityClass) &&
                Objects.equals(this.partitionKeysName, that.partitionKeysName) &&
                Objects.equals(this.lastPartitionKeyName, that.lastPartitionKeyName) &&
                Objects.equals(this.fromClusteringKeysName, that.fromClusteringKeysName) &&
                Objects.equals(this.toClusteringKeysName, that.toClusteringKeysName) &&
                Objects.equals(this.withClusteringKeysName, that.withClusteringKeysName) &&
                Objects.equals(this.lastClusteringKeyName, that.lastClusteringKeyName) &&
                Objects.equals(this.boundingMode, that.boundingMode) &&
                Objects.equals(this.orderingModeO, that.orderingModeO) &&
                Objects.equals(this.limitO, that.limitO);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.sliceType,
                this.entityClass,
                this.partitionKeysName,
                this.lastPartitionKeyName,
                this.fromClusteringKeysName,
                this.toClusteringKeysName,
                this.withClusteringKeysName,
                this.lastClusteringKeyName,
                this.boundingMode,
                this.orderingModeO,
                this.limitO);
    }

    public static enum SliceType {
        SELECT, ITERATE, DELETE;
    }
}
