package fr.doan.achilles.wrapper;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.DynamicComposite;
import fr.doan.achilles.exception.ValidationException;
import fr.doan.achilles.proxy.EntityWrapperUtil;
import fr.doan.achilles.validation.Validator;

/**
 * MultiKeyWideMapWrapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideMapWrapper<ID, K, V> extends WideMapWrapper<ID, K, V>
{

	private List<Serializer<?>> componentSerializers;
	private List<Method> componentGetters;

	private EntityWrapperUtil util = new EntityWrapperUtil();

	@Override
	protected DynamicComposite buildComposite(K key)
	{

		List<Object> componentValues = util.determineMultiKey(key, componentGetters);

		return keyFactory.buildForProperty(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers);

	}

	@Override
	protected DynamicComposite buildQueryCompositeStart(K start, boolean inclusiveStart)
	{
		List<Object> componentValues = util.determineMultiKey(start, componentGetters);

		return keyFactory.buildQueryComparatorStart(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers, inclusiveStart);
	}

	@Override
	protected DynamicComposite buildQueryCompositeEnd(K end, boolean inclusiveEnd)
	{
		List<Object> componentValues = util.determineMultiKey(end, componentGetters);

		return keyFactory.buildQueryComparatorEnd(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers, inclusiveEnd);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void validateBounds(K start, K end, boolean reverse)
	{

		if (start != null && end != null)
		{

			List<Object> startComponentValues = util.determineMultiKey(start, componentGetters);
			List<Object> endComponentValues = util.determineMultiKey(end, componentGetters);

			for (int i = 0; i < startComponentValues.size(); i++)
			{

				Comparable<Object> startValue = (Comparable<Object>) startComponentValues.get(i);
				Object endValue = endComponentValues.get(i);

				if (reverse)
				{
					if (endValue == null && startValue != null)
					{
						throw new ValidationException(
								"For multiKey descending range query, endKey cannot be null and startKey not null");
					}
					else if (startValue != null && endValue != null)
					{
						Validator
								.validateTrue(startValue.compareTo(endValue) >= 0,
										"For multiKey descending range query, startKey value should be greater or equal to end endKey");
					}

				}
				else
				{
					if (startValue == null && endValue != null)
					{
						throw new ValidationException(
								"For multiKey ascending range query, startKey cannot be null and endKey not null");
					}
					else if (startValue != null && endValue != null)
					{
						Validator
								.validateTrue(startValue.compareTo(endValue) <= 0,
										"For multiKey ascending range query, startKey value should be lesser or equal to end endKey");
					}
				}
			}
		}
	}

	public void setComponentSerializers(List<Serializer<?>> componentSerializers)
	{
		this.componentSerializers = componentSerializers;
	}

	public void setComponentGetters(List<Method> componentGetters)
	{
		this.componentGetters = componentGetters;
	}
}
