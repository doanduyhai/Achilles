package info.archinnov.achilles.helper;

import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.CQLRowMethodInvoker;

import com.datastax.driver.core.Row;

/**
 * CQLEntityMapper
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityMapper extends AchillesEntityMapper
{

	private CQLRowMethodInvoker cqlRowInvoker = new CQLRowMethodInvoker();

	public void setEagerPropertiesToEntity(Row row, EntityMeta entityMeta, Object entity)
	{
		for (PropertyMeta<?, ?> pm : entityMeta.getEagerMetas())
		{
			setPropertyToEntity(row, pm, entity);
		}
	}

	public void setPropertyToEntity(Row row, PropertyMeta<?, ?> pm, Object entity)
	{
		if (row != null)
		{
			String propertyName = pm.getPropertyName();
			if (!row.isNull(propertyName))
			{
				Object value = cqlRowInvoker.invokeOnRowForEagerFields(row, pm);
				invoker.setValueToField(entity, pm.getSetter(), value);
			}
		}
	}

	public void setPropertyToEntity(Object value, PropertyMeta<?, ?> pm, Object entity)
	{
		invoker.setValueToField(entity, pm.getSetter(), value);
	}
}
