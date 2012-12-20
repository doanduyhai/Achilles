package fr.doan.achilles.wrapper.builder;

import java.lang.reflect.Method;
import java.util.List;

import me.prettyprint.hector.api.Serializer;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.wrapper.MultiKeyWideMapWrapper;

/**
 * MultiKeyWideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class MultiKeyWideMapWrapperBuilder<ID, K, V> extends WideMapWrapperBuilder<ID, K, V>
{

	private List<Serializer<?>> componentSerializers;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	public MultiKeyWideMapWrapperBuilder(ID id, GenericEntityDao<ID> dao, PropertyMeta<K, V> wideMapMeta)
	{
		super(id, dao, wideMapMeta);
	}

	public static <ID, K, V> MultiKeyWideMapWrapperBuilder<ID, K, V> builder(ID id,
			GenericEntityDao<ID> dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new MultiKeyWideMapWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public MultiKeyWideMapWrapper<ID, K, V> build()
	{
		MultiKeyWideMapWrapper<ID, K, V> wrapper = new MultiKeyWideMapWrapper<ID, K, V>();
		super.build(wrapper);

		wrapper.setComponentGetters(componentGetters);
		wrapper.setComponentSetters(componentSetters);
		wrapper.setComponentSerializers(componentSerializers);

		return wrapper;

	}

	public MultiKeyWideMapWrapperBuilder<ID, K, V> componentSerializers(
			List<Serializer<?>> componentSerializers)
	{
		this.componentSerializers = componentSerializers;
		return this;
	}

	public MultiKeyWideMapWrapperBuilder<ID, K, V> componentGetters(List<Method> componentGetters)
	{
		this.componentGetters = componentGetters;
		return this;
	}

	public MultiKeyWideMapWrapperBuilder<ID, K, V> componentSetters(List<Method> componentSetters)
	{
		this.componentSetters = componentSetters;
		return this;
	}
}
