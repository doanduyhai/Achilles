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

package info.archinnov.achilles.internals.runtime;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ArrayUtils.addAll;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.SettableData;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.metamodel.AbstractProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.internals.statements.BoundValueInfo;
import info.archinnov.achilles.internals.statements.BoundValuesWrapper;
import info.archinnov.achilles.internals.types.OverridingOptional;
import info.archinnov.achilles.type.tuples.Tuple2;

public class BeanValueExtractor {

    private static final Logger LOGGER = LoggerFactory.getLogger(BeanValueExtractor.class);

    public static <T> BoundValuesWrapper extractAllValues(T instance, AbstractEntityProperty<T> entityProperty, CassandraOptions cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Extract values from entity %s of type %s",
                    instance, entityProperty.entityClass.getCanonicalName()));
        }

        final List<BoundValueInfo> boundValues = new ArrayList<>();
        final List<BoundValueInfo> partitionKeys = entityProperty.partitionKeys
                .stream()
                .map(x -> {
                    final AbstractProperty x1 = (AbstractProperty) x;
                    final BiConsumer<Object, SettableData> lambda = x1::encodeToSettable;
                    return BoundValueInfo.of(lambda, x.getFieldValue(instance), x.encodeField(instance, Optional.ofNullable(cassandraOptions)));
                })
                .collect(toList());

        boundValues.addAll(partitionKeys);

        boundValues.addAll(entityProperty.staticColumns
                .stream()
                .map(x -> {
                    final AbstractProperty x1 = (AbstractProperty) x;
                    return BoundValueInfo.of(x1::encodeToSettable, x.getFieldValue(instance), x.encodeField(instance, Optional.ofNullable(cassandraOptions)));
                })
                .collect(toList()));

        boundValues.addAll(entityProperty.clusteringColumns
                .stream()
                .map(x -> {
                    final AbstractProperty x1 = (AbstractProperty) x;
                    return BoundValueInfo.of(x1::encodeToSettable, x.getFieldValue(instance), x.encodeField(instance, Optional.ofNullable(cassandraOptions)));
                })
                .collect(toList()));

        boundValues.addAll(entityProperty.normalColumns
                .stream()
                .map(x -> {
                    final AbstractProperty x1 = (AbstractProperty) x;
                    return BoundValueInfo.of(x1::encodeToSettable, x.getFieldValue(instance), x.encodeField(instance, Optional.ofNullable(cassandraOptions)));
                })
                .collect(toList()));

        boundValues.addAll(entityProperty.counterColumns
                .stream()
                .map(x -> {
                    final AbstractProperty x1 = (AbstractProperty) x;
                    return BoundValueInfo.of(x1::encodeToSettable, x.getFieldValue(instance), x.encodeField(instance, Optional.ofNullable(cassandraOptions)));
                })
                .collect(toList()));

        final Optional<Integer> ttl = OverridingOptional
                .from(cassandraOptions.getTimeToLive())
                .andThen(entityProperty.staticTTL)
                .getOptional();

        boundValues.add(ttl.isPresent()
                ? BoundValueInfo.of((Object value, SettableData settableData) -> settableData.setInt("ttl", ttl.get()), ttl.get(), ttl.get())
                : BoundValueInfo.of((Object value, SettableData settableData) -> settableData.setInt("ttl", 0), 0, 0));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Extracted encoded bound values : %s", boundValues));
        }
        return new BoundValuesWrapper(entityProperty, boundValues);
    }

    public static <T> Tuple2<Object[], Object[]> extractPrimaryKeyValues(T instance, AbstractEntityProperty<T> entityProperty, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Extract primary key values from entity %s of type %s",
                    instance, entityProperty.entityClass.getCanonicalName()));
        }

        final Stream<Tuple2<Object, Object>> partitionKeys = entityProperty.partitionKeys
                .stream().map(x -> Tuple2.of(x.getFieldValue(instance), x.encodeField(instance, cassandraOptions)));
        final Stream<Tuple2<Object, Object>> partitionKeysCopy = entityProperty.partitionKeys
                .stream().map(x -> Tuple2.of(x.getFieldValue(instance), x.encodeField(instance, cassandraOptions)));

        final Stream<Tuple2<Object, Object>> clusteringColumns = entityProperty.clusteringColumns
                .stream().map(x -> Tuple2.of(x.getFieldValue(instance), x.encodeField(instance, cassandraOptions)));
        final Stream<Tuple2<Object, Object>> clusteringColumnsCopy = entityProperty.clusteringColumns
                .stream().map(x -> Tuple2.of(x.getFieldValue(instance), x.encodeField(instance, cassandraOptions)));

        final Object[] boundValues = addAll(partitionKeys.map(x -> x._1()).toArray(), clusteringColumns.map(x -> x._1()).toArray());
        final Object[] encodedValues = addAll(partitionKeysCopy.map(x -> x._2()).toArray(), clusteringColumnsCopy.map(x -> x._2()).toArray());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Extracted primary key (encoded) : %s", encodedValues));
        }

        return Tuple2.of(boundValues, encodedValues);
    }

    public static <T> BoundValuesWrapper extractPartitionKeysAndStaticValues(T instance, AbstractEntityProperty<T> entityProperty, CassandraOptions cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Extract partition key values and static columns from entity %s of type %s",
                    instance, entityProperty.entityClass.getCanonicalName()));
        }

        final List<BoundValueInfo> boundValues = new ArrayList<>();
        final List<BoundValueInfo> partitionKeys = entityProperty.partitionKeys
                .stream()
                .map(x -> {
                    final AbstractProperty x1 = (AbstractProperty) x;
                    final BiConsumer<Object, SettableData> lambda = x1::encodeToSettable;
                    return BoundValueInfo.of(lambda, x.getFieldValue(instance), x.encodeField(instance, Optional.ofNullable(cassandraOptions)));
                })
                .collect(toList());

        boundValues.addAll(partitionKeys);

        boundValues.addAll(entityProperty.staticColumns
                .stream()
                .map(x -> {
                    final AbstractProperty x1 = (AbstractProperty) x;
                    return BoundValueInfo.of(x1::encodeToSettable, x.getFieldValue(instance), x.encodeField(instance, Optional.ofNullable(cassandraOptions)));
                })
                .collect(toList()));

        final Optional<Integer> ttl = OverridingOptional
                .from(cassandraOptions.getTimeToLive())
                .andThen(entityProperty.staticTTL)
                .getOptional();

        boundValues.add(ttl.isPresent()
                ? BoundValueInfo.of((Object value, SettableData settableData) -> settableData.setInt("ttl", ttl.get()), ttl.get(), ttl.get())
                : BoundValueInfo.of((Object value, SettableData settableData) -> settableData.setInt("ttl", 0), 0, 0));

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Extracted encoded bound values : %s", boundValues));
        }
        return new BoundValuesWrapper(entityProperty, boundValues);

    }
}
