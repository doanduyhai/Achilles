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

import java.lang.reflect.Field;
import java.util.*;

import info.archinnov.achilles.internal.metadata.holder.CompoundPKProperties;
import info.archinnov.achilles.internal.metadata.holder.CompoundPKPropertiesBuilder;
import info.archinnov.achilles.internal.metadata.holder.PropertyMeta;
import info.archinnov.achilles.internal.metadata.parsing.context.PropertyParsingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import info.archinnov.achilles.annotations.PartitionKey;
import info.archinnov.achilles.internal.validation.Validator;

public class CompoundPKParser {

    private static final Logger log = LoggerFactory.getLogger(CompoundPKParser.class);
    private PropertyFilter filter = PropertyFilter.Singleton.INSTANCE.get();

    private final PropertyParsingContext context;

    public CompoundPKParser(PropertyParsingContext context) {
        this.context = context;
    }

    public CompoundPKProperties parseCompoundPK(Class<?> compoundPKClass, PropertyParser propertyParser) {
        log.debug("Parse compound primary key class {} ", compoundPKClass.getCanonicalName());

        final ComponentOrderingParser parser = ComponentOrderingParser.determineAppropriateParser(compoundPKClass, context);
        final Map<Integer, Field> components = parser.extractComponentsOrdering(compoundPKClass);
        final List<ClusteringOrder> clusteringOrders = parser.extractClusteringOrder(compoundPKClass);
        CompoundPKProperties compoundPKProperties = buildComponentMetas(propertyParser,compoundPKClass, components, clusteringOrders);

        log.trace("Built compound primary key properties : {}", compoundPKProperties);
        return compoundPKProperties;
    }

    private CompoundPKProperties buildComponentMetas(PropertyParser propertyParser, Class<?> compoundPKClass, Map<Integer, Field> components, List<ClusteringOrder> clusteringOrders) {

        log.debug("Build components meta data for compound primary key class {}", compoundPKClass.getCanonicalName());
        CompoundPKPropertiesBuilder partitionKeysBuilder = new CompoundPKPropertiesBuilder();
        CompoundPKPropertiesBuilder clusteringKeysBuilder = new CompoundPKPropertiesBuilder();
        clusteringKeysBuilder.setClusteringOrders(clusteringOrders);

        buildPartitionAndClusteringKeys(propertyParser, compoundPKClass, components,partitionKeysBuilder, clusteringKeysBuilder);

        return CompoundPKPropertiesBuilder.buildCompoundPKProperties(partitionKeysBuilder.buildPartitionKeys(), clusteringKeysBuilder.buildClusteringKeys(), context.getCurrentEntityClass().getCanonicalName());
    }

    private void buildPartitionAndClusteringKeys(PropertyParser propertyParser, Class<?> compoundPKClass, Map<Integer, Field> components,
            CompoundPKPropertiesBuilder partitionKeysBuilder,CompoundPKPropertiesBuilder clusteringKeysBuilder) {
        log.debug("Build components meta data for compound primary key class {}", compoundPKClass.getCanonicalName());

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
}
