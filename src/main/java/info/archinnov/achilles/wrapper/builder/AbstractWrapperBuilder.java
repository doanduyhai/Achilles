package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
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
public abstract class AbstractWrapperBuilder<ID, T extends AbstractWrapperBuilder<ID, T, K, V>, K, V>
{
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;
	private Method setter;
	private PropertyMeta<K, V> propertyMeta;
	private EntityProxifier proxifier;
	protected PersistenceContext<ID> context;

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

	public T proxifier(EntityProxifier proxifier)
	{
		this.proxifier = proxifier;
		return (T) this;
	}

	public T context(PersistenceContext<ID> context)
	{
		this.context = context;
		return (T) this;
	}

	public void build(AbstractWrapper<ID, K, V> wrapper)
	{
		wrapper.setDirtyMap(dirtyMap);
		wrapper.setSetter(setter);
		wrapper.setPropertyMeta(propertyMeta);
		wrapper.setProxifier(proxifier);
		wrapper.setContext(context);
	}
}
