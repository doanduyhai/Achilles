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
public class MultiKeyWideMapWrapperBuilder<ID, K, V>
{
	private ID id;
	private GenericEntityDao<ID> dao;
	private PropertyMeta<K, V> wideMapMeta;

	private List<Serializer<?>> componentSerializers;
	private List<Method> componentGetters;
	private List<Method> componentSetters;

	public MultiKeyWideMapWrapperBuilder(ID id, GenericEntityDao<ID> dao,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.id = id;
		this.dao = dao;
		this.wideMapMeta = wideMapMeta;
	}

	public static <ID, K, V> MultiKeyWideMapWrapperBuilder<ID, K, V> builder(ID id,
			GenericEntityDao<ID> dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new MultiKeyWideMapWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public MultiKeyWideMapWrapper<ID, K, V> build()
	{
		MultiKeyWideMapWrapper<ID, K, V> wrapper = new MultiKeyWideMapWrapper<ID, K, V>();
		wrapper.setId(id);
		wrapper.setDao(dao);
		wrapper.setWideMapMeta(wideMapMeta);

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
