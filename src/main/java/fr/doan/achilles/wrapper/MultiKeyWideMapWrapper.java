package fr.doan.achilles.wrapper;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import fr.doan.achilles.entity.metadata.MultiKeyWideMapMeta;
import fr.doan.achilles.entity.type.KeyValue;
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
	private List<Method> componentSetters;

	private EntityWrapperUtil util = new EntityWrapperUtil();

	@Override
	protected DynamicComposite buildComposite(K key)
	{

		List<Object> componentValues = util.determineMultiKey(key, componentGetters);

		return keyFactory.buildForProperty(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers);

	}

	@Override
	protected DynamicComposite buildQueryComposite(K start, ComponentEquality equality)
	{
		List<Object> componentValues = util.determineMultiKey(start, componentGetters);

		return keyFactory.buildQueryComparator(wideMapMeta.getPropertyName(),
				wideMapMeta.propertyType(), componentValues, componentSerializers, equality);
	}

	@Override
	public List<KeyValue<K, V>> findValues(K start, boolean inclusiveStart, K end,
			boolean inclusiveEnd, boolean reverse, int count)
	{

		validateBounds(start, end, reverse);

		DynamicComposite[] queryComps = buildQueryComposites(start, inclusiveStart, end,
				inclusiveEnd, reverse);

		List<HColumn<DynamicComposite, Object>> hColumns = dao.findRawColumnsRange(id,
				queryComps[0], queryComps[1], reverse, count);

		return util.buildMultiKey(wideMapMeta.getKeyClass(),
				(MultiKeyWideMapMeta<K, V>) wideMapMeta, hColumns, componentSetters);
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void validateBounds(K start, K end, boolean reverse)
	{

		if (start != null && end != null)
		{

			List<Object> startComponentValues = util.determineMultiKey(start, componentGetters);
			List<Object> endComponentValues = util.determineMultiKey(end, componentGetters);

			keyFactory.validateNoHole(wideMapMeta.getPropertyName(), startComponentValues);
			keyFactory.validateNoHole(wideMapMeta.getPropertyName(), endComponentValues);

			for (int i = 0; i < startComponentValues.size(); i++)
			{

				Comparable<Object> startValue = (Comparable<Object>) startComponentValues.get(i);
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

	public void setComponentSerializers(List<Serializer<?>> componentSerializers)
	{
		this.componentSerializers = componentSerializers;
	}

	public void setComponentGetters(List<Method> componentGetters)
	{
		this.componentGetters = componentGetters;
	}

	public void setComponentSetters(List<Method> componentSetters)
	{
		this.componentSetters = componentSetters;
	}

}
