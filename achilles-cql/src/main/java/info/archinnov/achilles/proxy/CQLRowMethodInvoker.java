package info.archinnov.achilles.proxy;

import static info.archinnov.achilles.helper.CQLTypeHelper.getRowMethod;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.exception.AchillesException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.datastax.driver.core.Row;

/**
 * CQLResultMethodInvoker
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLRowMethodInvoker
{

	public Object invokeOnRowForEagerFields(Row row, PropertyMeta<?, ?> pm)
	{
		String propertyName = pm.getPropertyName();
		Object value = null;
		if (!row.isNull(propertyName))
		{
			switch (pm.type())
			{
				case LIST:
					value = invokeOnRowForList(row, pm.getPropertyName(), pm.getValueClass());
					break;
				case SET:
					value = invokeOnRowForSet(row, pm.getPropertyName(), pm.getValueClass());
					break;
				case MAP:
					Class<?> keyClass = pm.getKeyClass();
					Class<?> valueClass = pm
							.getJoinProperties()
							.getEntityMeta()
							.getIdMeta()
							.getValueClass();
					value = invokeOnRowForMap(row, pm.getPropertyName(), keyClass, valueClass);
					break;
				case SIMPLE:
					value = invokeOnRowForProperty(row, pm.getPropertyName(), pm.getValueClass());
					break;
				default:
					break;
			}
		}
		return value;
	}

	public Object invokeOnRowForProperty(Row row, String propertyName, Class<?> valueClass)
	{
		try
		{
			return getRowMethod(valueClass).invoke(row, propertyName);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot retrieve property '" + propertyName
					+ "' from CQL Row", e);
		}
	}

	public <V> List<V> invokeOnRowForList(Row row, String propertyName, Class<V> valueClass)
	{
		try
		{
			return row.getList(propertyName, valueClass);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot retrieve list property '" + propertyName
					+ "' from CQL Row", e);
		}
	}

	public <V> Set<V> invokeOnRowForSet(Row row, String propertyName, Class<V> valueClass)
	{
		try
		{
			return row.getSet(propertyName, valueClass);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot retrieve set property '" + propertyName
					+ "' from CQL Row", e);
		}
	}

	public <K, V> Map<K, V> invokeOnRowForMap(Row row, String propertyName, Class<K> keyClass,
			Class<V> valueClass)
	{
		try
		{
			return row.getMap(propertyName, keyClass, valueClass);
		}
		catch (Exception e)
		{
			throw new AchillesException("Cannot retrieve map property '" + propertyName
					+ "' from CQL Row", e);
		}
	}
}
