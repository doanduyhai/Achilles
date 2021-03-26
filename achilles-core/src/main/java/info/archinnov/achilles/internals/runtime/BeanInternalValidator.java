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

import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import info.archinnov.achilles.internals.metamodel.AbstractEntityProperty;
import info.archinnov.achilles.internals.options.CassandraOptions;
import info.archinnov.achilles.type.tuples.Tuple2;
import info.archinnov.achilles.validation.Validator;


public class BeanInternalValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(BeanInternalValidator.class);

    public static <T> void validatePrimaryKey(T instance, AbstractEntityProperty<T> entityProperty, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Validate primary key for instance %s of type %s",
                    instance, entityProperty.entityClass.getCanonicalName()));
        }

        entityProperty
                .partitionKeys
                .stream()
                .map(x -> Tuple2.of(x.fieldName, x.encodeField(instance, cassandraOptions)))
                .filter(x -> x._2() == null)
                .forEach(tuple ->
                                Validator.validateNotNull(tuple._2(),
                                        "Field '%s' in entity of type '%s' should not be null because it is a partition key",
                                        tuple._1(), entityProperty.entityClass.getCanonicalName())
                );


        entityProperty
                .clusteringColumns
                .stream()
                .map(x -> Tuple2.of(x.fieldName, x.encodeField(instance, cassandraOptions)))
                .filter(x -> x._2() == null)
                .forEach(tuple ->
                        Validator.validateNotNull(tuple._2(),
                                "Field '%s' in entity of type '%s' should not be null because it is a clustering column",
                                tuple._1(), entityProperty.entityClass.getCanonicalName())
                );

    }

    public static <T> void validateColumnsForInsertOrUpdateStatic(T instance, AbstractEntityProperty<T> entityProperty, Optional<CassandraOptions> cassandraOptions) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(format("Validate partition columns and other columns for INSERT STATIC of instance %s of type %s",
                    instance, entityProperty.entityClass.getCanonicalName()));
        }

        entityProperty
                .partitionKeys
                .stream()
                .map(x -> Tuple2.of(x.fieldName, x.encodeField(instance, cassandraOptions)))
                .filter(x -> x._2() == null)
                .forEach(tuple ->
                        Validator.validateNotNull(tuple._2(),
                                "Field '%s' in entity of type '%s' should not be null because it is a partition key",
                                tuple._1(), entityProperty.entityClass.getCanonicalName())
                );

        final long nonNullStaticColumnsCount = entityProperty
                .staticColumns
                .stream()
                .map(x -> x.encodeField(instance, cassandraOptions))
                .filter(x -> x != null)
                .count();

        Validator.validateTrue(nonNullStaticColumnsCount > 0,
                "There should be at least one non null static column in entity of type '%s' when calling insertStatic()",
                entityProperty.entityClass.getCanonicalName());
    }
}
