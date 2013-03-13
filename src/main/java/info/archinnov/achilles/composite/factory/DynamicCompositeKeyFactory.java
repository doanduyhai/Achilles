package info.archinnov.achilles.composite.factory;

import static info.archinnov.achilles.serializer.SerializerUtils.BYTE_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.INT_SRZ;
import static info.archinnov.achilles.serializer.SerializerUtils.STRING_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.MultiKeyProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.type.WideMap;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.validation.Validator;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DynamicCompositeKeyFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class DynamicCompositeKeyFactory
{
	private static final Logger log = LoggerFactory.getLogger(DynamicCompositeKeyFactory.class);

	private CompositeHelper compositeHelper = new CompositeHelper();
	private EntityHelper entityHelper = new EntityHelper();

	public <K, V> DynamicComposite createForBatchInsertSingleValue(PropertyMeta<K, V> propertyMeta)
	{
		log.trace("Creating base dynamic composite for propertyMeta {} for batch insert",
				propertyMeta.getPropertyName());

		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		return composite;
	}

	public <K, V> DynamicComposite createForBatchInsertMultiValue(PropertyMeta<K, V> propertyMeta,
			int hashOrPosition)
	{
		log.trace("Creating base dynamic composite for propertyMeta {} for batch insert",
				propertyMeta.getPropertyName());

		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, propertyMeta.type().flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(2, hashOrPosition, INT_SRZ, INT_SRZ.getComparatorType()
				.getTypeName());
		return composite;
	}

	@SuppressWarnings("unchecked")
	public <K, V, T> DynamicComposite createForInsert(PropertyMeta<K, V> propertyMeta, T key)
	{

		log.trace("Creating dynamic composite for propertyMeta {} for insert",
				propertyMeta.getPropertyName());

		DynamicComposite composite = new DynamicComposite();
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
			List<Object> keyValues = entityHelper.determineMultiKey(key,
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

	public <K, V> DynamicComposite createBaseForQuery(PropertyMeta<K, V> propertyMeta,
			ComponentEquality equality)
	{
		log.trace("Creating base dynamic composite for propertyMeta {} query",
				propertyMeta.getPropertyName());

		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, propertyMeta.type().flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyMeta.getPropertyName(), equality);

		return composite;
	}

	@SuppressWarnings("unchecked")
	public <K, V, T> DynamicComposite createForQuery(PropertyMeta<K, V> propertyMeta, T value,
			ComponentEquality equality)
	{
		log.trace("Creating dynamic composite for propertyMeta {} query",
				propertyMeta.getPropertyName());

		DynamicComposite composite = new DynamicComposite();
		PropertyType type = propertyMeta.type();
		String propertyName = propertyMeta.getPropertyName();

		if (propertyMeta.isSingleKey())
		{
			composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);

			if (value != null)
			{
				composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
				composite.addComponent(2, value, equality);
			}
			else
			{
				composite.addComponent(1, propertyName, equality);
			}
		}
		else
		{
			MultiKeyProperties multiKeyProperties = propertyMeta.getMultiKeyProperties();
			List<Serializer<?>> componentSerializers = multiKeyProperties.getComponentSerializers();
			List<Method> componentGetters = multiKeyProperties.getComponentGetters();

			List<Object> keyValues = entityHelper.determineMultiKey(value, componentGetters);
			int srzCount = componentSerializers.size();
			int valueCount = keyValues.size();

			Validator.validateTrue(srzCount >= valueCount, "There should be at most" + srzCount
					+ " values for the key of WideMap '" + propertyName + "'");

			int lastNotNullIndex = compositeHelper.findLastNonNullIndexForComponents(propertyName,
					keyValues);

			composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType()
					.getTypeName(), EQUAL);
			composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
					.getTypeName(), EQUAL);

			if (lastNotNullIndex >= 0)
			{
				composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
						.getTypeName(), EQUAL);

				for (int i = 0; i <= lastNotNullIndex; i++)
				{
					Serializer<Object> srz = (Serializer<Object>) componentSerializers.get(i);
					Object keyValue = keyValues.get(i);
					if (i < lastNotNullIndex)
					{
						composite.setComponent(i + 2, keyValue, srz, srz.getComparatorType()
								.getTypeName(), EQUAL);
					}
					else
					{
						composite.setComponent(i + 2, keyValue, srz, srz.getComparatorType()
								.getTypeName(), equality);
					}
				}
			}
			else
			{
				composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
						.getTypeName(), equality);
			}
		}
		return composite;
	}

	public <K, V> DynamicComposite[] createForQuery(PropertyMeta<K, V> propertyMeta, K start,
			K end, WideMap.BoundingMode bounds, WideMap.OrderingMode ordering)
	{
		DynamicComposite[] queryComp = new DynamicComposite[2];

		ComponentEquality[] equalities = compositeHelper.determineEquality(bounds, ordering);

		DynamicComposite startComp = this.createForQuery(propertyMeta, start, equalities[0]);
		DynamicComposite endComp = this.createForQuery(propertyMeta, end, equalities[1]);

		queryComp[0] = startComp;
		queryComp[1] = endComp;

		return queryComp;
	}
}
