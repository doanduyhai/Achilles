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
package info.archinnov.achilles.composite;

import static info.archinnov.achilles.serializer.ThriftSerializerUtils.*;
import info.archinnov.achilles.compound.CompoundKeyValidator;
import info.archinnov.achilles.compound.ThriftCompoundKeyMapper;
import info.archinnov.achilles.compound.ThriftCompoundKeyValidator;
import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.type.BoundingMode;
import info.archinnov.achilles.type.OrderingMode;

import java.text.DecimalFormat;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftCompositeFactory {
	private static final Logger log = LoggerFactory.getLogger(ThriftCompositeFactory.class);
	private static final String numberFormat = "000000";

	private ComponentEqualityCalculator calculator = new ComponentEqualityCalculator();
	private ThriftCompoundKeyMapper compoundKeyMapper = new ThriftCompoundKeyMapper();
	private CompoundKeyValidator compoundKeyValidator = new ThriftCompoundKeyValidator();

	public <T> Composite createCompositeForClusteringComponents(ThriftPersistenceContext context) {

		PropertyMeta idMeta = context.getIdMeta();
		Object primaryKey = context.getPrimaryKey();
		log.trace("Creating base composite for clustering components of @EmbeddedId {}", idMeta.getPropertyName());
		return compoundKeyMapper.fromCompoundToCompositeForInsertOrGet(primaryKey, idMeta);
	}

	public Object buildRowKey(ThriftPersistenceContext context) {
		return compoundKeyMapper.buildRowKey(context);
	}

	public Composite[] createForClusteredQuery(PropertyMeta idMeta, List<Object> clusteringFrom,
			List<Object> clusteringTo, BoundingMode bounding, OrderingMode ordering) {

		compoundKeyValidator.validateComponentsForSliceQuery(idMeta, clusteringFrom, clusteringTo, ordering);
		ComponentEquality[] equalities = calculator.determineEquality(bounding, ordering);

		final Composite from = compoundKeyMapper.fromComponentsToCompositeForQuery(clusteringFrom, idMeta,
				equalities[0]);
		final Composite to = compoundKeyMapper.fromComponentsToCompositeForQuery(clusteringTo, idMeta, equalities[1]);

		return new Composite[] { from, to };

	}

	public Composite createRowKeyForCounter(String fqcn, Object key, PropertyMeta idMeta) {
		log.trace("Creating composite counter row key for entity class {} and primary key {}", fqcn, key);

		Composite comp = new Composite();
		comp.setComponent(0, fqcn, STRING_SRZ);
		comp.setComponent(1, idMeta.forceEncodeToJSON(key), STRING_SRZ);
		return comp;
	}

	public Composite createBaseForGet(PropertyMeta propertyMeta) {
		log.trace("Creating base composite for propertyMeta {} get", propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.addComponent(0, propertyMeta.type().flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyMeta.getPropertyName(), ComponentEquality.EQUAL);
		composite.addComponent(2, "0", ComponentEquality.EQUAL);
		return composite;
	}

	public Composite createBaseForClusteredGet(Object compoundKey, PropertyMeta idMeta) {
		return compoundKeyMapper.fromCompoundToCompositeForInsertOrGet(compoundKey, idMeta);
	}

	public Composite createBaseForCounterGet(PropertyMeta propertyMeta) {
		log.trace("Creating base composite for propertyMeta {} get", propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.addComponent(0, propertyMeta.getPropertyName(), ComponentEquality.EQUAL);
		return composite;
	}

	public Composite createBaseForQuery(PropertyMeta propertyMeta, ComponentEquality equality) {
		log.trace("Creating base composite for propertyMeta {} query and equality {}", propertyMeta.getPropertyName(),
				equality.name());

		Composite composite = new Composite();
		composite.addComponent(0, propertyMeta.type().flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyMeta.getPropertyName(), equality);
		return composite;
	}

	public Composite createForBatchInsertSingleValue(PropertyMeta propertyMeta) {
		log.trace("Creating base composite for propertyMeta {} for single value batch insert",
				propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());
		composite.setComponent(2, "0", STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		return composite;
	}

	public Composite createForBatchInsertSingleCounter(PropertyMeta propertyMeta) {
		log.trace("Creating base composite for propertyMeta {} for single counter value batch insert",
				propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.setComponent(0, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());
		return composite;
	}

	public Composite createForBatchInsertList(PropertyMeta propertyMeta, int position) {
		log.trace("Creating base composite for propertyMeta {} for list value batch insert with position {}",
				propertyMeta.getPropertyName(), position);

		Composite composite = new Composite();
		composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());
		String positionString = new DecimalFormat(numberFormat).format(position);
		composite.setComponent(2, positionString, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		return composite;
	}

	public Composite createForBatchInsertSetOrMap(PropertyMeta propertyMeta, String valueOrKey) {
		log.trace("Creating base composite for propertyMeta {} for set/map value batch insert {}",
				propertyMeta.getPropertyName(), valueOrKey);

		Composite composite = new Composite();
		composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());
		composite.setComponent(2, valueOrKey, STRING_SRZ, STRING_SRZ.getComparatorType().getTypeName());
		return composite;
	}

}
