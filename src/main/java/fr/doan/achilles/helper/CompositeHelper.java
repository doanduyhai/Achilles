package fr.doan.achilles.helper;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.LESS_THAN_EQUAL;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.validation.Validator;

/**
 * CompositeHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CompositeHelper
{
	private EntityWrapperUtil util = new EntityWrapperUtil();

	public int findLastNonNullIndexForComponents(String propertyName, List<Object> keyValues)
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
		lastNotNullIndex--;
		return lastNotNullIndex;
	}

	@SuppressWarnings("unchecked")
	public <K, V> void checkBounds(PropertyMeta<K, V> wideMapMeta, K start, K end, boolean reverse)
	{

		if (start != null && end != null)
		{
			if (wideMapMeta.isSingleKey())
			{
				Comparable<K> startComp = (Comparable<K>) start;

				if (reverse)
				{
					Validator
							.validateTrue(startComp.compareTo(end) >= 0,
									"For reverse range query, start value should be greater or equal to end value");
				}
				else
				{
					Validator.validateTrue(startComp.compareTo(end) <= 0,
							"For range query, start value should be lesser or equal to end value");
				}
			}
			else
			{
				List<Method> componentGetters = wideMapMeta.getComponentGetters();
				String propertyName = wideMapMeta.getPropertyName();

				List<Object> startComponentValues = util.determineMultiKey(start, componentGetters);
				List<Object> endComponentValues = util.determineMultiKey(end, componentGetters);

				this.findLastNonNullIndexForComponents(propertyName, startComponentValues);
				this.findLastNonNullIndexForComponents(propertyName, endComponentValues);

				for (int i = 0; i < startComponentValues.size(); i++)
				{

					Comparable<Object> startValue = (Comparable<Object>) startComponentValues
							.get(i);
					Object endValue = endComponentValues.get(i);

					if (reverse)
					{
						if (startValue != null && endValue != null)
						{
							Validator
									.validateTrue(startValue.compareTo(endValue) >= 0,
											"For multiKey descending range query, startKey value should be greater or equal to end endKey");
						}

					}
					else
					{
						if (startValue != null && endValue != null)
						{
							Validator
									.validateTrue(startValue.compareTo(endValue) <= 0,
											"For multiKey ascending range query, startKey value should be lesser or equal to end endKey");
						}
					}
				}
			}
		}
	}

	public ComponentEquality[] determineEquality(boolean inclusiveStart, boolean inclusiveEnd,
			boolean reverse)
	{
		ComponentEquality[] result = new ComponentEquality[2];
		ComponentEquality start;
		ComponentEquality end;
		if (reverse)
		{
			start = inclusiveStart ? GREATER_THAN_EQUAL : LESS_THAN_EQUAL;
			end = inclusiveEnd ? EQUAL : GREATER_THAN_EQUAL;
		}
		else
		{
			start = inclusiveStart ? EQUAL : GREATER_THAN_EQUAL;
			end = inclusiveEnd ? GREATER_THAN_EQUAL : LESS_THAN_EQUAL;
		}

		result[0] = start;
		result[1] = end;
		return result;
	}
}
