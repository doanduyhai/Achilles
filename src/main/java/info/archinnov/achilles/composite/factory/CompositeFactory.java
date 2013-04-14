package info.archinnov.achilles.composite.factory;

import static info.archinnov.achilles.serializer.SerializerUtils.*;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import info.archinnov.achilles.entity.EntityIntrospector;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.validation.Validator;

import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CompositeKeyFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompositeFactory
{
	private static final Logger log = LoggerFactory.getLogger(CompositeFactory.class);

	private CompositeHelper helper = new CompositeHelper();
	private EntityIntrospector entityIntrospector = new EntityIntrospector();

	@SuppressWarnings("unchecked")
	public <K, V, T> Composite createBaseComposite(PropertyMeta<K, V> propertyMeta, T keyValue)
	{
		log.trace("Creating base composite for propertyMeta {}", propertyMeta.getPropertyName());

		Composite composite = new Composite();
		String propertyName = propertyMeta.getPropertyName();

		if (propertyMeta.isSingleKey())
		{
			Validator.validateNotNull(keyValue, "The values for the for the key of WideMap '"
					+ propertyName + "' should not be null");

			Serializer<T> keySerializer = (Serializer<T>) propertyMeta.getKeySerializer();
			composite.setComponent(0, keyValue, keySerializer, keySerializer.getComparatorType()
					.getTypeName());
		}
		else
		{
			MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
			List<Serializer<?>> componentSerializers = multiKeyProperties.getComponentSerializers();
			List<Object> keyValues = entityIntrospector.determineMultiKey(keyValue,
					multiKeyProperties.getComponentGetters());
			int srzCount = componentSerializers.size();
			int valueCount = keyValues.size();

			Validator.validateTrue(srzCount == valueCount, "There should be " + srzCount
					+ " values for the key of WideMap '" + propertyName + "'");

			for (Object value : keyValues)
			{
				Validator.validateNotNull(value, "The values for the for the key of WideMap '"
						+ propertyName + "' should not be null");
			}

			for (int i = 0; i < srzCount; i++)
			{
				Serializer<Object> srz = (Serializer<Object>) componentSerializers.get(i);
				composite.setComponent(i, keyValues.get(i), srz, srz.getComparatorType()
						.getTypeName());
			}
		}
		return composite;
	}

	@SuppressWarnings("unchecked")
	public <K, V, T> Composite createForQuery(PropertyMeta<K, V> propertyMeta, T keyValue,
			ComponentEquality equality)
	{
		log.trace("Creating query composite for propertyMeta {}", propertyMeta.getPropertyName());

		Composite composite = new Composite();
		String propertyName = propertyMeta.getPropertyName();

		if (propertyMeta.isSingleKey())
		{
			if (keyValue == null)
			{
				composite = null;
			}
			else
			{
				composite.addComponent(0, keyValue, equality);
			}
		}
		else
		{
			MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
			List<Serializer<?>> componentSerializers = multiKeyProperties.getComponentSerializers();
			List<Object> keyValues = entityIntrospector.determineMultiKey(keyValue,
					multiKeyProperties.getComponentGetters());
			int srzCount = componentSerializers.size();
			int valueCount = keyValues.size();

			Validator.validateTrue(srzCount >= valueCount, "There should be at most" + srzCount
					+ " values for the key of WideMap '" + propertyName + "'");

			int lastNotNullIndex = helper
					.findLastNonNullIndexForComponents(propertyName, keyValues);

			for (int i = 0; i <= lastNotNullIndex; i++)
			{
				Serializer<Object> srz = (Serializer<Object>) componentSerializers.get(i);
				Object value = keyValues.get(i);
				if (i < lastNotNullIndex)
				{
					composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(),
							EQUAL);
				}
				else
				{
					composite.setComponent(i, value, srz, srz.getComparatorType().getTypeName(),
							equality);
				}
			}
		}
		return composite;
	}

	public <K, V> Composite[] createForQuery(PropertyMeta<K, V> propertyMeta, K start, K end,
			WideMap.BoundingMode bounds, WideMap.OrderingMode ordering)
	{
		Composite[] queryComp = new Composite[2];

		ComponentEquality[] equalities = helper.determineEquality(bounds, ordering);

		Composite startComp = createForQuery(propertyMeta, start, equalities[0]);
		Composite endComp = createForQuery(propertyMeta, end, equalities[1]);

		queryComp[0] = startComp;
		queryComp[1] = endComp;

		return queryComp;
	}

	public <ID> Composite createKeyForCounter(String fqcn, ID key, PropertyMeta<Void, ID> idMeta)
	{
		Composite comp = new Composite();
		comp.setComponent(0, fqcn, STRING_SRZ);
		comp.setComponent(1, idMeta.writeValueToString(key), STRING_SRZ);
		return comp;
	}

	public <K, V> Composite createBaseForGet(PropertyMeta<K, V> propertyMeta)
	{
		log.trace("Creating base composite for propertyMeta {} get", propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.addComponent(0, propertyMeta.type().flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyMeta.getPropertyName(), ComponentEquality.EQUAL);
		composite.addComponent(2, 0, ComponentEquality.EQUAL);
		return composite;
	}

	public <K, V> Composite createBaseForCounterGet(PropertyMeta<K, V> propertyMeta)
	{
		log.trace("Creating base composite for propertyMeta {} get", propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.addComponent(0, propertyMeta.getPropertyName(), ComponentEquality.EQUAL);
		return composite;
	}

	public <K, V> Composite createBaseForQuery(PropertyMeta<K, V> propertyMeta,
			ComponentEquality equality)
	{
		log.trace("Creating base composite for propertyMeta {} query",
				propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.addComponent(0, propertyMeta.type().flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyMeta.getPropertyName(), equality);
		return composite;
	}

	public <K, V> Composite createForBatchInsertSingleValue(PropertyMeta<K, V> propertyMeta)
	{
		log.trace("Creating base composite for propertyMeta {} for batch insert",
				propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(2, 0, INT_SRZ, INT_SRZ.getComparatorType().getTypeName());
		return composite;
	}

	public <K, V> Composite createForBatchInsertSingleCounter(PropertyMeta<K, V> propertyMeta)
	{
		log.trace("Creating base composite for propertyMeta {} for batch insert",
				propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.setComponent(0, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		return composite;
	}

	public <K, V> Composite createForBatchInsertMultiValue(PropertyMeta<K, V> propertyMeta,
			int hashOrPosition)
	{
		log.trace("Creating base composite for propertyMeta {} for batch insert",
				propertyMeta.getPropertyName());

		Composite composite = new Composite();
		composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(2, hashOrPosition, INT_SRZ, INT_SRZ.getComparatorType()
				.getTypeName());
		return composite;
	}

	@SuppressWarnings("unchecked")
	public <K, V, T> Composite createForInsert(PropertyMeta<K, V> propertyMeta, T key)
	{

		log.trace("Creating composite for propertyMeta {} for insert",
				propertyMeta.getPropertyName());

		Composite composite = new Composite();
		PropertyType type = propertyMeta.type();
		String propertyName = propertyMeta.getPropertyName();
		Serializer<T> keySerializer = (Serializer<T>) propertyMeta.getKeySerializer();

		if (propertyMeta.isSingleKey())
		{
			composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType()
					.getTypeName());
			composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
					.getTypeName());
			composite.setComponent(2, key, keySerializer, keySerializer.getComparatorType()
					.getTypeName());
		}
		else
		{
			MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
			List<Serializer<?>> componentSerializers = multiKeyProperties.getComponentSerializers();
			List<Object> keyValues = entityIntrospector.determineMultiKey(key,
					multiKeyProperties.getComponentGetters());

			int srzCount = componentSerializers.size();
			int valueCount = keyValues.size();

			Validator.validateTrue(srzCount == valueCount, "There should be " + srzCount
					+ " values for the key of WideMap '" + propertyName + "'");

			for (Object keyValue : keyValues)
			{
				Validator.validateNotNull(keyValue, "The values for the for the key of WideMap '"
						+ propertyName + "' should not be null");
			}

			composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType()
					.getTypeName());
			composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
					.getTypeName());

			for (int i = 0; i < srzCount; i++)
			{
				Serializer<Object> srz = (Serializer<Object>) componentSerializers.get(i);
				composite.setComponent(i + 2, keyValues.get(i), srz, srz.getComparatorType()
						.getTypeName());
			}
		}
		return composite;
	}
}
