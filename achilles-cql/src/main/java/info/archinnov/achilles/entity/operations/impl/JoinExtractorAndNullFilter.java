package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.MethodInvoker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.Pair;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

/**
 * JoinValueTransformer
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinExtractorAndNullFilter implements
		Function<PropertyMeta<?, ?>, Pair<List<?>, PropertyMeta<?, ?>>>,
		Predicate<Pair<List<?>, PropertyMeta<?, ?>>>
{
	private MethodInvoker invoker = new MethodInvoker();

	private Object entity;

	public JoinExtractorAndNullFilter(Object entity) {
		this.entity = entity;
	}

	@Override
	public Pair<List<?>, PropertyMeta<?, ?>> apply(PropertyMeta<?, ?> pm)
	{
		List<Object> joinValues = new ArrayList<Object>();
		Object joinValue = invoker.getValueFromField(entity, pm.getGetter());
		if (joinValue != null)
		{
			if (pm.isJoinCollection())
			{
				joinValues.addAll((Collection<Object>) joinValue);
			}
			else if (pm.isJoinMap())
			{
				Map<?, ?> joinMap = (Map<?, ?>) joinValue;
				joinValues.addAll((Collection<Object>) joinMap.values());
			}
			else
			{
				joinValues.add(joinValue);
			}
		}
		return Pair.<List<?>, PropertyMeta<?, ?>> create(joinValues, pm);
	}

	@Override
	public boolean apply(Pair<List<?>, PropertyMeta<?, ?>> joinValuesPair)
	{
		return !joinValuesPair.left.isEmpty();
	}
}
