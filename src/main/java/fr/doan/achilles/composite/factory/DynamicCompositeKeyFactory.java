package fr.doan.achilles.composite.factory;

import static fr.doan.achilles.serializer.Utils.BYTE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.validation.Validator;

/**
 * DynamicCompositeKeyFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class DynamicCompositeKeyFactory
{

	private CompositeHelper helper = new CompositeHelper();
	private EntityWrapperUtil util = new EntityWrapperUtil();

	public <K, V> DynamicComposite createBaseForInsert(PropertyMeta<K, V> propertyMeta,
			int hashOrPosition)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, propertyMeta.propertyType().flag(), BYTE_SRZ, BYTE_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(1, propertyMeta.getPropertyName(), STRING_SRZ, STRING_SRZ
				.getComparatorType().getTypeName());
		composite.setComponent(2, hashOrPosition, INT_SRZ, INT_SRZ.getComparatorType()
				.getTypeName());
		return composite;
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	public <K, V, T> DynamicComposite createForInsert(PropertyMeta<K, V> propertyMeta, T key)
	{
		DynamicComposite composite = new DynamicComposite();
		PropertyType type = propertyMeta.propertyType();
		String propertyName = propertyMeta.getPropertyName();
		Serializer keySerializer = propertyMeta.getKeySerializer();

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
			List<Serializer<?>> componentSerializers = propertyMeta.getComponentSerializers();
			int srzCount = componentSerializers.size();
			List<Object> keyValues = util
					.determineMultiKey(key, propertyMeta.getComponentGetters());
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
				Serializer srz = componentSerializers.get(i);
				composite.setComponent(i + 2, keyValues.get(i), srz, srz.getComparatorType()
						.getTypeName());
			}
		}
		return composite;
	}

	public <K, V> DynamicComposite createBaseForQuery(PropertyMeta<K, V> propertyMeta,
			ComponentEquality equality)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, propertyMeta.propertyType().flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyMeta.getPropertyName(), equality);

		return composite;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public <K, V, T> DynamicComposite createForQuery(PropertyMeta<K, V> propertyMeta, T value,
			ComponentEquality equality)
	{
		DynamicComposite composite = new DynamicComposite();
		PropertyType type = propertyMeta.propertyType();
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

			List<Serializer<?>> componentSerializers = propertyMeta.getComponentSerializers();
			List<Object> keyValues = (List<Object>) value;

			int srzCount = componentSerializers.size();
			int valueCount = keyValues.size();

			Validator.validateTrue(srzCount >= valueCount, "There should be at most" + srzCount
					+ " values for the key of WideMap '" + propertyName + "'");

			int lastNotNullIndex = helper
					.findLastNonNullIndexForComponents(propertyName, keyValues);

			composite.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType()
					.getTypeName(), EQUAL);
			composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
					.getTypeName(), EQUAL);

			for (int i = 0; i <= lastNotNullIndex; i++)
			{
				Serializer srz = componentSerializers.get(i);
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
		return composite;
	}

	public <K, V> DynamicComposite[] createForQuery(PropertyMeta<K, V> propertyMeta, K start,
			boolean inclusiveStart, K end, boolean inclusiveEnd, boolean reverse)
	{
		DynamicComposite[] queryComp = new DynamicComposite[2];

		ComponentEquality[] equalities = helper.determineEquality(inclusiveStart, inclusiveEnd,
				reverse);

		DynamicComposite startComp;
		DynamicComposite endComp;

		if (propertyMeta.isSingleKey())
		{
			startComp = this.createForQuery(propertyMeta, start, equalities[0]);
			endComp = this.createForQuery(propertyMeta, end, equalities[1]);
		}
		else
		{

			List<Method> componentGetters = propertyMeta.getComponentGetters();

			List<Object> startComponentValues = util.determineMultiKey(start, componentGetters);
			List<Object> endComponentValues = util.determineMultiKey(end, componentGetters);

			startComp = this.createForQuery(propertyMeta, startComponentValues, equalities[0]);
			endComp = this.createForQuery(propertyMeta, endComponentValues, equalities[1]);
		}

		queryComp[0] = startComp;
		queryComp[1] = endComp;

		return queryComp;
	}
}
