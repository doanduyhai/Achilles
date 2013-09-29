/**
 *
 * Copyright (C) 2012-2013 DuyHai DOAN
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
package info.archinnov.achilles.table;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ComparatorType;

import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftComparatorTypeAliasFactory {

	private static final Logger log = LoggerFactory.getLogger(ThriftComparatorTypeAliasFactory.class);

	public String determineCompatatorTypeAliasForClusteringComponents(PropertyMeta idMeta, boolean forCreation) {
		log.debug("Determine the Comparator type alias for clustering components of field {} ",
				idMeta.getPropertyName());

		List<Class<?>> componentClasses = idMeta.getClusteringComponentClasses();
		List<String> componentNames = idMeta.getClusteringComponentNames();

		String comparatorTypesAlias = determineTypeAlias(idMeta, forCreation, componentClasses, componentNames);

		log.trace("Comparator type alias : {}", comparatorTypesAlias);

		return comparatorTypesAlias;
	}

	public Pair<String, String> determineKeyValidationAndAlias(PropertyMeta idMeta, boolean forCreation) {

		String keyValidationClass, keyValidationAlias = null;
		if (idMeta.isCompositePartitionKey()) {
			keyValidationClass = ComparatorType.COMPOSITETYPE.getTypeName();
			keyValidationAlias = this.determineCompatatorTypeAliasForPartitionComponents(idMeta, forCreation);
		} else {
			Class<?> partitionKeyClass;
			if (idMeta.isEmbeddedId())
				partitionKeyClass = idMeta.getPartitionComponentClasses().get(0);
			else
				partitionKeyClass = idMeta.getValueClass();
			Serializer<?> idSerializer = ThriftSerializerTypeInferer.getSerializer(partitionKeyClass);
			if (forCreation)
				keyValidationClass = idSerializer.getComparatorType().getTypeName();
			else
				keyValidationClass = idSerializer.getComparatorType().getClassName();

		}

		return Pair.create(keyValidationClass, keyValidationAlias);
	}

	private String determineCompatatorTypeAliasForPartitionComponents(PropertyMeta idMeta, boolean forCreation) {
		log.debug("Determine the Comparator type alias for partition components of field {} ", idMeta.getPropertyName());

		List<Class<?>> componentClasses = idMeta.getPartitionComponentClasses();
		List<String> componentNames = idMeta.getPartitionComponentNames();

		String typesAlias = determineTypeAlias(idMeta, forCreation, componentClasses, componentNames);

		log.trace("Comparator type alias : {}", typesAlias);

		return typesAlias;
	}

	private String determineTypeAlias(PropertyMeta idMeta, boolean forCreation, List<Class<?>> componentClasses,
			List<String> componentNames) {
		List<String> comparatorTypes = new ArrayList<String>();
		for (int i = 0; i < componentClasses.size(); i++) {

			Class<?> clazz = componentClasses.get(i);
			String serializerName;
			Serializer<?> srz = ThriftSerializerTypeInferer.getSerializer(clazz);
			if (clazz.equals(UUID.class) && idMeta.isComponentTimeUUID(componentNames.get(i))) {
				serializerName = "TimeUUIDType";
			} else {
				serializerName = srz.getComparatorType().getTypeName();
			}

			if (forCreation) {
				comparatorTypes.add(serializerName);
			} else {
				comparatorTypes.add("org.apache.cassandra.db.marshal." + serializerName);
			}
		}

		String comparatorTypesAlias;
		if (forCreation) {
			comparatorTypesAlias = "(" + StringUtils.join(comparatorTypes, ',') + ")";
		} else {
			comparatorTypesAlias = "CompositeType(" + StringUtils.join(comparatorTypes, ',') + ")";
		}
		return comparatorTypesAlias;
	}
}
