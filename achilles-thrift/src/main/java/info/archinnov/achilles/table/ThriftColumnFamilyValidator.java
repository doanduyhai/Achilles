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

import static me.prettyprint.hector.api.ddl.ComparatorType.COUNTERTYPE;
import info.archinnov.achilles.counter.AchillesCounter;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.serializer.ThriftSerializerTypeInferer;
import info.archinnov.achilles.serializer.ThriftSerializerUtils;
import info.archinnov.achilles.validation.Validator;
import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftColumnFamilyValidator {
	protected static final Logger log = LoggerFactory
			.getLogger(ThriftColumnFamilyValidator.class);
	public static final String ENTITY_COMPARATOR_TYPE_CHECK = "CompositeType(org.apache.cassandra.db.marshal.BytesType,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)";
	public static final String COUNTER_KEY_CHECK = "org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type)";
	public static final String COUNTER_COMPARATOR_CHECK = "CompositeType(org.apache.cassandra.db.marshal.UTF8Type)";

	private ThriftComparatorTypeAliasFactory comparatorAliasFactory = new ThriftComparatorTypeAliasFactory();

	public void validateCFForEntity(ColumnFamilyDefinition cfDef,
			EntityMeta entityMeta) {
		log.trace(
				"Validating column family row key definition for entityMeta {}",
				entityMeta.getClassName());

		Serializer<?> idSerializer = ThriftSerializerTypeInferer
				.getSerializer(entityMeta.getIdClass());

		Validator
				.validateTableTrue(
						StringUtils
								.equals(cfDef.getKeyValidationClass(),
										idSerializer.getComparatorType()
												.getClassName()),
						"The column family '%s' key class '%s' does not correspond to the entity id class '%s'",
						entityMeta.getTableName(), cfDef
								.getKeyValidationClass(), idSerializer
								.getComparatorType().getClassName());

		log.trace(
				"Validating column family  composite comparator definition for entityMeta {}",
				entityMeta.getClassName());

		String comparatorType = (cfDef.getComparatorType() != null ? cfDef
				.getComparatorType().getTypeName() : "")
				+ cfDef.getComparatorTypeAlias();

		Validator.validateTableTrue(StringUtils.equals(comparatorType,
				ENTITY_COMPARATOR_TYPE_CHECK),
				"The column family '%s' comparator type '%s' should be '%s'",
				entityMeta.getTableName(), comparatorType,
				ENTITY_COMPARATOR_TYPE_CHECK);

	}

	public void validateCFForClusteredEntity(ColumnFamilyDefinition cfDef,
			EntityMeta meta, String tableName) {

		PropertyMeta idMeta = meta.getIdMeta();

		log.trace(
				"Validating column family composite comparator definition for clustered entity {} and column family {}",
				idMeta.getEntityClassName(), tableName);

		String valueValidationType;
		if (meta.isValueless()) {
			valueValidationType = ThriftSerializerUtils.STRING_SRZ
					.getComparatorType().getClassName();
		} else {
			PropertyMeta pm = meta.getFirstMeta();
			PropertyType type = pm.type();
			if (type.isCounter()) {
				valueValidationType = COUNTERTYPE.getClassName();
			} else if (type.isJoin()) {
				valueValidationType = ThriftSerializerTypeInferer
						.getSerializer(pm.joinIdMeta().getValueClass())
						.getComparatorType().getClassName();
			} else {
				valueValidationType = ThriftSerializerTypeInferer
						.getSerializer(pm.getValueClass()).getComparatorType()
						.getClassName();
			}
		}

		Class<?> keyClass = idMeta.getComponentClasses().get(0);
		String keyValidationType = ThriftSerializerTypeInferer
				.<Object> getSerializer(keyClass).getComparatorType()
				.getClassName();

		Validator.validateTableTrue(StringUtils.equals(
				cfDef.getKeyValidationClass(), keyValidationType),
				"The column family '%s' key validation type should be '%s'",
				tableName, keyValidationType);

		String comparatorTypeAlias = comparatorAliasFactory
				.determineCompatatorTypeAliasForClusteredEntity(idMeta, false);

		String comparatorType = (cfDef.getComparatorType() != null ? cfDef
				.getComparatorType().getTypeName() : "")
				+ cfDef.getComparatorTypeAlias();

		Validator.validateTableTrue(
				StringUtils.equals(comparatorType, comparatorTypeAlias),
				"The column family '%s' comparator type should be '%s'",
				tableName, comparatorTypeAlias);

		Validator
				.validateTableTrue(
						StringUtils.equals(cfDef.getDefaultValidationClass(),
								valueValidationType),
						"The column family '%s' default validation type should be '%s'",
						tableName, valueValidationType);

	}

	public void validateCounterCF(ColumnFamilyDefinition cfDef) {
		log.trace("Validating counter column family row key definition ");

		String keyValidation = cfDef.getKeyValidationClass()
				+ cfDef.getKeyValidationAlias();

		Validator.validateTableTrue(
				StringUtils.equals(keyValidation, COUNTER_KEY_CHECK),
				"The column family '%s' key class '%s' should be '%s'",
				AchillesCounter.THRIFT_COUNTER_CF, keyValidation,
				COUNTER_KEY_CHECK);

		String comparatorType = (cfDef.getComparatorType() != null ? cfDef
				.getComparatorType().getTypeName() : "")
				+ cfDef.getComparatorTypeAlias();

		Validator.validateTableTrue(
				StringUtils.equals(comparatorType, COUNTER_COMPARATOR_CHECK),
				"The column family '%s' comparator type '%s' should be '%s'",
				AchillesCounter.THRIFT_COUNTER_CF, comparatorType,
				COUNTER_COMPARATOR_CHECK);

		Validator.validateTableTrue(StringUtils.equals(
				cfDef.getDefaultValidationClass(), COUNTERTYPE.getClassName()),
				"The column family '%s' validation class '%s' should be '%s'",
				AchillesCounter.THRIFT_COUNTER_CF, cfDef
						.getDefaultValidationClass(), COUNTERTYPE
						.getClassName());

	}

}
