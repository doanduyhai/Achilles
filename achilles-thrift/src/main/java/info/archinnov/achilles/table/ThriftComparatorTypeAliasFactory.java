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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftComparatorTypeAliasFactory {

	private static final Logger log = LoggerFactory
			.getLogger(ThriftComparatorTypeAliasFactory.class);

	public String determineCompatatorTypeAliasForClusteredEntity(
			PropertyMeta idMeta, boolean forCreation) {
		log.debug(
				"Determine the Comparator type alias for composite-based column family using propertyMeta of field {} ",
				idMeta.getPropertyName());

		List<String> comparatorTypes = new ArrayList<String>();
		String comparatorTypesAlias;

		List<Class<?>> componentClasses = idMeta
				.getClusteringComponentClasses();
		List<String> componentNames = idMeta.getClusteringComponentNames();

		for (int i = 0; i < componentClasses.size(); i++) {

			Class<?> clazz = componentClasses.get(i);
			String serializerName;
			Serializer<?> srz = ThriftSerializerTypeInferer
					.getSerializer(clazz);
			if (clazz.equals(UUID.class)
					&& idMeta.isComponentTimeUUID(componentNames.get(i))) {
				serializerName = "TimeUUIDType";
			} else {
				serializerName = srz.getComparatorType().getTypeName();
			}

			if (forCreation) {
				comparatorTypes.add(serializerName);
			} else {
				comparatorTypes.add("org.apache.cassandra.db.marshal."
						+ serializerName);
			}
		}
		if (forCreation) {
			comparatorTypesAlias = "(" + StringUtils.join(comparatorTypes, ',')
					+ ")";
		} else {
			comparatorTypesAlias = "CompositeType("
					+ StringUtils.join(comparatorTypes, ',') + ")";
		}

		log.trace("Comparator type alias : {}", comparatorTypesAlias);

		return comparatorTypesAlias;
	}
}
