package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.wrapper.AbstractWrapper;

import java.lang.reflect.Method;
import java.util.Map;

/**
 * AbstractWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
@SuppressWarnings("unchecked")
public abstract class AbstractWrapperBuilder<T extends AbstractWrapperBuilder<T, K, V>, K, V>
{
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;
	private Method setter;
	private PropertyMeta<K, V> propertyMeta;
	private EntityHelper helper;

	public T dirtyMap(Map<Method, PropertyMeta<?, ?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
		return (T) this;
	}

	public T setter(Method setter)
	{
		this.setter = setter;
		return (T) this;
	}

	public T propertyMeta(PropertyMeta<K, V> propertyMeta)
	{
		this.propertyMeta = propertyMeta;
		return (T) this;
	}

	public T helper(EntityHelper helper)
	{
		this.helper = helper;
		return (T) this;
	}

	public void build(AbstractWrapper<K, V> wrapper)
	{
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setHelper(helper);
	}
}
