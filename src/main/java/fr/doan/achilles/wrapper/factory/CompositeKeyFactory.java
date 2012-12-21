package fr.doan.achilles.wrapper.factory;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;

import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;
import fr.doan.achilles.helper.CompositeHelper;
import fr.doan.achilles.validation.Validator;

/**
 * CompositeKeyFactory
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompositeKeyFactory
{

	private CompositeHelper helper = new CompositeHelper();

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public Composite buildForInsert(String propertyName, List<Object> keyValues,
			List<Serializer<?>> serializers)
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

		Composite composite = new Composite();

		for (int i = 0; i < srzCount; i++)
		{
			Serializer srz = serializers.get(i);
			composite.setComponent(i, keyValues.get(i), srz, srz.getComparatorType().getTypeName());
		}

		return composite;
	}

	@SuppressWarnings(
	{
			"unchecked",
			"rawtypes"
	})
	public Composite buildQueryComparator(String propertyName, List<Object> keyValues,
			List<Serializer<?>> serializers, ComponentEquality equality)
	{
		int srzCount = serializers.size();
		int valueCount = keyValues.size();

		Validator.validateTrue(srzCount >= valueCount, "There should be at most" + srzCount
				+ " values for the key of WideMap '" + propertyName + "'");

		int lastNotNullIndex = helper.findLastNonNullIndexForComponents(propertyName, keyValues);

		Composite composite = new Composite();
		for (int i = 0; i <= lastNotNullIndex; i++)
		{
			Serializer srz = serializers.get(i);
			Object keyValue = keyValues.get(i);
			if (i < lastNotNullIndex)
			{
				composite.setComponent(i, keyValue, srz, srz.getComparatorType().getTypeName(),
						EQUAL);
			}
			else
			{
				composite.setComponent(i, keyValue, srz, srz.getComparatorType().getTypeName(),
						equality);
			}
		}

		return composite;
	}

	public Composite buildQueryComparatorStart(String propertyName, List<Object> keyValues,
			List<Serializer<?>> serializers, boolean inclusive)
	{
		ComponentEquality equality = inclusive ? EQUAL : GREATER_THAN_EQUAL;
		return buildQueryComparator(propertyName, keyValues, serializers, equality);

	}

	public Composite buildQueryComparatorEnd(String propertyName, List<Object> keyValues,
			List<Serializer<?>> serializers, boolean inclusive)
	{
		ComponentEquality equality = inclusive ? GREATER_THAN_EQUAL : LESS_THAN_EQUAL;
		return buildQueryComparator(propertyName, keyValues, serializers, equality);

	}
}
