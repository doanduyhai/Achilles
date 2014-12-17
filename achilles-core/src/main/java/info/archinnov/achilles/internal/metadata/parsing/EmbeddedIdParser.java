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
package info.archinnov.achilles.internal.metadata.parsing;

import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder;
import static info.archinnov.achilles.schemabuilder.Create.Options.ClusteringOrder.Sorting;
import static org.reflections.ReflectionUtils.getAllFields;

import java.lang.reflect.Field;
import java.util.*;

import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.annotations.Column;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.exception.AchillesBeanMappingException;
import info.archinnov.achilles.internal.metadata.holder.EmbeddedIdProperties;
import info.archinnov.achilles.internal.metadata.holder.EmbeddedIdPropertiesBuilder;
import info.archinnov.achilles.internal.validation.Validator;

public class EmbeddedIdParser {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedIdParser.class);
    private PropertyFilter filter = PropertyFilter.Singleton.INSTANCE.get();

    private final PropertyParsingContext context;

    public EmbeddedIdParser(PropertyParsingContext context) {
        this.context = context;
    }

    public EmbeddedIdProperties parseEmbeddedId(Class<?> embeddedIdClass, PropertyParser propertyParser) {
        log.debug("Parse embedded id class {} ", embeddedIdClass.getCanonicalName());

        final ComponentOrderingParser parser = ComponentOrderingParser.determineAppropriateParser(embeddedIdClass, context);
        final Map<Integer, Field> components = parser.extractComponentsOrdering(embeddedIdClass);
        final List<ClusteringOrder> clusteringOrders = parser.extractClusteringOrder(embeddedIdClass);
        EmbeddedIdProperties embeddedIdProperties = buildComponentMetas(propertyParser,embeddedIdClass, components, clusteringOrders);

        log.trace("Built embeddedId properties : {}", embeddedIdProperties);
        return embeddedIdProperties;
    }

    private EmbeddedIdProperties buildComponentMetas(PropertyParser propertyParser, Class<?> embeddedIdClass, Map<Integer, Field> components, List<ClusteringOrder> clusteringOrders) {

        log.debug("Build components meta data for embedded id class {}", embeddedIdClass.getCanonicalName());
        EmbeddedIdPropertiesBuilder partitionKeysBuilder = new EmbeddedIdPropertiesBuilder();
        EmbeddedIdPropertiesBuilder clusteringKeysBuilder = new EmbeddedIdPropertiesBuilder();
        clusteringKeysBuilder.setClusteringOrders(clusteringOrders);

        buildPartitionAndClusteringKeys(propertyParser, embeddedIdClass, components,partitionKeysBuilder, clusteringKeysBuilder);

        return EmbeddedIdPropertiesBuilder.buildEmbeddedIdProperties(partitionKeysBuilder.buildPartitionKeys(), clusteringKeysBuilder.buildClusteringKeys(), context.getCurrentEntityClass().getCanonicalName());
    }

    private void buildPartitionAndClusteringKeys(PropertyParser propertyParser, Class<?> embeddedIdClass, Map<Integer, Field> components,
            EmbeddedIdPropertiesBuilder partitionKeysBuilder,EmbeddedIdPropertiesBuilder clusteringKeysBuilder) {
        log.debug("Build components meta data for embedded id class {}", embeddedIdClass.getCanonicalName());

        for (Integer order : components.keySet()) {
            Field compositeKeyField = components.get(order);

            final Class<?> type = compositeKeyField.getType();
            final String propertyName = compositeKeyField.getName();

            Validator.validateBeanMappingFalse(List.class.isAssignableFrom(type), "The column '%s' cannot be a list because it belongs to the partition key", propertyName);
            Validator.validateBeanMappingFalse(Set.class.isAssignableFrom(type), "The column '%s' cannot be a set because it belongs to the partition key", propertyName);
            Validator.validateBeanMappingFalse(Map.class.isAssignableFrom(type), "The column '%s' cannot be a map because it belongs to the partition key", propertyName);

            final PropertyMeta propertyMeta = propertyParser.parseSimpleProperty(context.duplicateForField(compositeKeyField));

            if (filter.hasAnnotation(compositeKeyField, PartitionKey.class)) {
                partitionKeysBuilder.addPropertyMeta(propertyMeta);
            } else {
                clusteringKeysBuilder.addPropertyMeta(propertyMeta);
            }
        }

        // If not @PartitionKey annotation found, take the first field as default partition key
        if (partitionKeysBuilder.getPropertyMetas().isEmpty()) {
            final PropertyMeta partitionMeta = clusteringKeysBuilder.getPropertyMetas().remove(0);
            partitionKeysBuilder.addPropertyMeta(partitionMeta);
        }
    }

    private void validateNotStaticColumn(Field compositeKeyField) {
        Column column = compositeKeyField.getAnnotation(Column.class);
        if (column != null && column.staticColumn()) {
            throw new AchillesBeanMappingException(String.format("The property '%s' of class '%s' cannot be a static column because it belongs to the primary key",compositeKeyField.getName(),compositeKeyField.getDeclaringClass().getCanonicalName()));
        }
    }
}
