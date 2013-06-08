package info.archinnov.achilles.entity.metadata.util;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;

import java.util.Set;

import com.google.common.base.Predicate;
import com.google.common.collect.Sets;

/**
 * PropertyTypeExclude
 * 
 * @author DuyHai DOAN
 * 
 */
public class PropertyTypeExclude implements Predicate<PropertyMeta<?, ?>>
{
	private final Set<PropertyType> typesToExclude;

	public PropertyTypeExclude(PropertyType... types) {
		this.typesToExclude = Sets.newHashSet(types);
	}

	@Override
	public boolean apply(PropertyMeta<?, ?> pm)
	{
		return !typesToExclude.contains(pm.type());
	}
};