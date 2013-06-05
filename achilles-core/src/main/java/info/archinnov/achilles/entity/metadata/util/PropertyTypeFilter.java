package info.archinnov.achilles.entity.metadata.util;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

/**
 * PropertyTypeFilter
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyTypeFilter implements Predicate<PropertyMeta<?, ?>>
{
	private final Set<PropertyType> types;

	public PropertyTypeFilter(PropertyType... types) {
		this.types = Sets.newHashSet(types);
	}

	@Override
	public boolean apply(PropertyMeta<?, ?> pm)
	{
		return types.contains(pm.type());
	}
};