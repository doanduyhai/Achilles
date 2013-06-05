package info.archinnov.achilles.entity.metadata.util;

import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import com.google.common.base.Predicate;

/**
 * CascadeMergeFilter
 * 
 * @author DuyHai DOAN
 * 
 */
public class CascadeMergeFilter implements Predicate<PropertyMeta<?, ?>>
{

	@Override
	public boolean apply(PropertyMeta<?, ?> pm)
	{
		return pm.hasAnyCascadeType(MERGE, ALL);
	}

}
