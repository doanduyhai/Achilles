package fr.doan.achilles.wrapper.factory;

import static fr.doan.achilles.serializer.Utils.BYTE_SRZ;
import static fr.doan.achilles.serializer.Utils.INT_SRZ;
import static fr.doan.achilles.serializer.Utils.STRING_SRZ;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;

import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.entity.metadata.PropertyType;
import fr.doan.achilles.validation.Validator;

/**
 * DynamicCompositeKeyFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class DynamicCompositeKeyFactory
{

	public DynamicComposite buildForProperty(String propertyName, PropertyType type,
			int hashOrPosition)
	{
		DynamicComposite composite = new DynamicComposite();
		composite
				.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());
		composite.setComponent(2, hashOrPosition, INT_SRZ, INT_SRZ.getComparatorType()
				.getTypeName());
		return composite;
	}

	public <T> DynamicComposite buildForProperty(String propertyName, PropertyType type, T value,
			Serializer<T> valueSerializer)
	{
		DynamicComposite composite = new DynamicComposite();
		composite
				.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());
		composite.setComponent(2, value, valueSerializer, valueSerializer.getComparatorType()
				.getTypeName());
		return composite;
	}

	public DynamicComposite buildQueryComparator(String propertyName, PropertyType type,
			ComponentEquality equality)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, equality);

		return composite;
	}

	public DynamicComposite buildQueryComparator(String propertyName, PropertyType type,
			Object value, ComponentEquality equality)
	{
		DynamicComposite composite = new DynamicComposite();
		composite.addComponent(0, type.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, propertyName, ComponentEquality.EQUAL);
		composite.addComponent(2, value, equality);

		return composite;
	}

	public DynamicComposite buildQueryComparatorStart(String propertyName, PropertyType type,
			int hashOrPosition, boolean inclusive)
	{
		ComponentEquality equality = inclusive ? EQUAL : GREATER_THAN_EQUAL;
		return buildQueryComparator(propertyName, type, hashOrPosition, equality);
	}

	public DynamicComposite buildQueryComparatorEnd(String propertyName, PropertyType type,
			int hashOrPosition, boolean inclusive)
	{
		ComponentEquality equality = inclusive ? EQUAL : LESS_THAN_EQUAL;
		return buildQueryComparator(propertyName, type, hashOrPosition, equality);
	}

	public DynamicComposite buildQueryComparatorStart(String propertyName, PropertyType type,
			Object value, boolean inclusive)
	{
		ComponentEquality equality = inclusive ? EQUAL : GREATER_THAN_EQUAL;
		return buildQueryComparator(propertyName, type, value, equality);
	}

	public DynamicComposite buildQueryComparatorEnd(String propertyName, PropertyType type,
			Object value, boolean inclusive)
	{
		ComponentEquality equality = inclusive ? EQUAL : LESS_THAN_EQUAL;
		return buildQueryComparator(propertyName, type, value, equality);
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public DynamicComposite buildForProperty(String propertyName, PropertyType type,
			List<Object> keyValues, List<Serializer<?>> serializers)
	{
		int srzCount = serializers.size();
		int valueCount = keyValues.size();

		Validator.validateTrue(srzCount == valueCount, "There should be " + srzCount
				+ " values for the key of WideMap '" + propertyName + "'");

		for (Object keyValue : keyValues)
		{
			Validator.validateNotNull(keyValue, "The values for the for the key of WideMap '"
					+ propertyName + "' should not be null");
		}

		DynamicComposite composite = new DynamicComposite();
		composite
				.setComponent(0, type.flag(), BYTE_SRZ, BYTE_SRZ.getComparatorType().getTypeName());
		composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName());

		for (int i = 0; i < srzCount; i++)
		{
			Serializer srz = serializers.get(i);
			composite.setComponent(i + 2, keyValues.get(i), srz, srz.getComparatorType()
					.getTypeName());
		}

		return composite;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public DynamicComposite buildQueryComparator(String propertyName, PropertyType type,
			List<Object> keyValues, List<Serializer<?>> serializers, ComponentEquality equality)
	{
		int srzCount = serializers.size();
		int valueCount = keyValues.size();

		Validator.validateTrue(srzCount >= valueCount, "There should be at most" + srzCount
				+ " values for the key of WideMap '" + propertyName + "'");

		int lastNotNullIndex = validateNoHole(propertyName, keyValues);

		DynamicComposite composite = new DynamicComposite();
		composite.setComponent(0, type.flag(), BYTE_SRZ,
				BYTE_SRZ.getComparatorType().getTypeName(), EQUAL);
		composite.setComponent(1, propertyName, STRING_SRZ, STRING_SRZ.getComparatorType()
				.getTypeName(), EQUAL);

		for (int i = 0; i <= lastNotNullIndex; i++)
		{
			Serializer srz = serializers.get(i);
			Object keyValue = keyValues.get(i);
			if (i < lastNotNullIndex)
			{
				composite.setComponent(i + 2, keyValue, srz, srz.getComparatorType().getTypeName(),
						EQUAL);
			}
			else
			{
				composite.setComponent(i + 2, keyValue, srz, srz.getComparatorType().getTypeName(),
						equality);
			}
		}

		return composite;
	}

	public DynamicComposite buildQueryComparatorStart(String propertyName, PropertyType type,
			List<Object> keyValues, List<Serializer<?>> serializers, boolean inclusive)
	{
		ComponentEquality equality = inclusive ? EQUAL : GREATER_THAN_EQUAL;
		return buildQueryComparator(propertyName, type, keyValues, serializers, equality);

	}

	public DynamicComposite buildQueryComparatorEnd(String propertyName, PropertyType type,
			List<Object> keyValues, List<Serializer<?>> serializers, boolean inclusive)
	{
		ComponentEquality equality = inclusive ? GREATER_THAN_EQUAL : LESS_THAN_EQUAL;
		return buildQueryComparator(propertyName, type, keyValues, serializers, equality);

	}

	public int validateNoHole(String propertyName, List<Object> keyValues)
	{
		boolean nullFlag = false;
		int lastNotNullIndex = 0;
		for (Object keyValue : keyValues)
		{
			if (keyValue != null)
			{
				if (nullFlag)
				{
					throw new IllegalArgumentException(
							"There should not be any null value between two non-null keys of WideMap '"
									+ propertyName + "'");
				}
				lastNotNullIndex++;
			}
			else
			{
				nullFlag = true;
			}
		}

		if (lastNotNullIndex > 0)
		{
			return --lastNotNullIndex;
		}
		else
		{
			throw new IllegalArgumentException("The keys of WideMap '" + propertyName
					+ "' should not be all null");
		}
	}
}
