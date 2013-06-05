package info.archinnov.achilles.entity.metadata.util;

import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.entity.metadata.PropertyMeta;

import com.google.common.base.Predicate;

/**
 * CascadePersistFilter
 * 
 * @author DuyHai DOAN
 * 
 */
public class CascadePersistFilter implements Predicate<PropertyMeta<?, ?>>
{

	@Override
	public boolean apply(PropertyMeta<?, ?> pm)
	{
		return pm.hasAnyCascadeType(PERSIST, ALL);
	}

}
