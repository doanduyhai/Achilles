package info.archinnov.achilles.proxy.interceptor;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.wrapper.builder.ExternalWideMapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.JoinExternalWideMapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.JoinWideMapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.ListWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.MapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.SetWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.WideMapWrapperBuilder;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.mutation.Mutator;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

/**
 * JpaEntityInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public class JpaEntityInterceptor<ID, T> implements MethodInterceptor, AchillesInterceptor
{

	private EntityLoader loader = new EntityLoader();
	private CompositeHelper compositeHelper = new CompositeHelper();
	private KeyValueFactory keyValueFactory = new KeyValueFactory();
	private IteratorFactory iteratorFactory = new IteratorFactory();
	private CompositeKeyFactory compositeKeyFactory = new CompositeKeyFactory();
	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();
	private EntityPersister persister = new EntityPersister();
	private EntityHelper entityHelper = new EntityHelper();

	private GenericDynamicCompositeDao<ID> entityDao;
	private GenericCompositeDao<ID, ?> columnFamilyDao;
	private Boolean directColumnFamilyMapping;

	private T target;
	private ID key;
	private Method idGetter;
	private Method idSetter;
	private Map<Method, PropertyMeta<?, ?>> getterMetas;
	private Map<Method, PropertyMeta<?, ?>> setterMetas;
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;
	private Set<Method> lazyLoaded;
	private Mutator<ID> mutator;
	private Map<String, Mutator<?>> mutatorMap;

	@Override
	public Object getTarget()
	{
		return this.target;
	}

	@Override
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		if (this.idGetter == method)
		{
			return this.key;
		}
		else if (this.idSetter == method)
		{
			throw new IllegalAccessException("Cannot change id value for existing entity ");
		}

		Object result = null;
		if (this.getterMetas.containsKey(method))
		{
			result = interceptGetter(method, args, proxy);
		}
		else if (this.setterMetas.containsKey(method))
		{
			result = interceptSetter(method, args, proxy);
		}
		else
		{
			result = proxy.invoke(target, args);
		}
		return result;
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	private Object interceptGetter(Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		Object result;
		PropertyMeta propertyMeta = this.getterMetas.get(method);
		if (propertyMeta.type().isLazy() //
				&& !this.lazyLoaded.contains(method))
		{
			this.loader.loadPropertyIntoObject(target, key, entityDao, propertyMeta);
			this.lazyLoaded.add(method);
		}

		switch (propertyMeta.type())
		{
			case JOIN_SIMPLE:
				result = entityHelper.buildProxy(proxy.invoke(target, args),
						propertyMeta.joinMeta());
				break;
			case LIST:
			case LAZY_LIST:
			case JOIN_LIST:
				List<?> list = (List<?>) proxy.invoke(target, args);
				result = ListWrapperBuilder.builder(list) //
						.dirtyMap(dirtyMap) //
						.setter(propertyMeta.getSetter()) //
						.propertyMeta(propertyMeta) //
						.helper(entityHelper) //
						.build();
				break;
			case SET:
			case LAZY_SET:
			case JOIN_SET:
				Set<?> set = (Set<?>) proxy.invoke(target, args);
				result = SetWrapperBuilder.builder(set).dirtyMap(dirtyMap) //
						.setter(propertyMeta.getSetter())//
						.propertyMeta(propertyMeta) //
						.helper(entityHelper) //
						.build();
				break;
			case MAP:
			case LAZY_MAP:
			case JOIN_MAP:
				Map<?, ?> map = (Map<?, ?>) proxy.invoke(target, args);
				result = MapWrapperBuilder.builder(map)//
						.dirtyMap(dirtyMap) //
						.setter(propertyMeta.getSetter()) //
						.propertyMeta(propertyMeta) //
						.helper(entityHelper) //
						.build();
				break;
			case WIDE_MAP:
				if (directColumnFamilyMapping)
				{
					result = buildColumnFamilyWrapper(propertyMeta);
				}
				else
				{
					result = buildWideMapWrapper(propertyMeta);
				}
				break;
			case EXTERNAL_WIDE_MAP:
				result = buildExternalWideMapWrapper(propertyMeta);
				break;
			case JOIN_WIDE_MAP:
				result = buildJoinWideMapWrapper(propertyMeta);
				break;

			case EXTERNAL_JOIN_WIDE_MAP:
				result = buildExternalJoinWideMapWrapper(propertyMeta);
				break;
			default:
				result = proxy.invoke(target, args);
				break;
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private <K extends Comparable<K>, V> Object buildExternalWideMapWrapper(
			PropertyMeta<K, V> propertyMeta)
	{
		return ExternalWideMapWrapperBuilder
				.builder(
						key,
						(GenericCompositeDao<ID, V>) propertyMeta.getExternalWideMapProperties()
								.getExternalWideMapDao(), propertyMeta) //
				.interceptor(this) //
				.compositeHelper(compositeHelper) //
				.keyValueFactory(keyValueFactory)//
				.iteratorFactory(iteratorFactory)//
				.compositeKeyFactory(compositeKeyFactory) //
				.build();
	}

	@SuppressWarnings("unchecked")
	private <K extends Comparable<K>, V> Object buildExternalJoinWideMapWrapper(
			PropertyMeta<K, V> propertyMeta)
	{
		return JoinExternalWideMapWrapperBuilder
				.builder(
						key,
						(GenericCompositeDao<ID, ?>) propertyMeta.getExternalWideMapProperties()
								.getExternalWideMapDao(), propertyMeta) //
				.interceptor(this) //
				.compositeHelper(compositeHelper) //
				.compositeKeyFactory(compositeKeyFactory) //
				.entityHelper(entityHelper) //
				.iteratorFactory(iteratorFactory) //
				.keyValueFactory(keyValueFactory) //
				.loader(loader) //
				.persister(persister) //
				.build();
	}

	private <K extends Comparable<K>, V> Object buildWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		return WideMapWrapperBuilder.builder(key, entityDao, propertyMeta) //
				.interceptor(this) //
				.entityHelper(entityHelper) //
				.compositeHelper(compositeHelper) //
				.keyFactory(keyFactory) //
				.keyValueFactory(keyValueFactory) //
				.iteratorFactory(iteratorFactory) //
				.build();
	}

	private <K extends Comparable<K>, V> Object buildJoinWideMapWrapper(
			PropertyMeta<K, V> propertyMeta)
	{
		return JoinWideMapWrapperBuilder.builder(key, entityDao, propertyMeta) //
				.loader(loader) //
				.persister(persister) //
				.interceptor(this) //
				.entityHelper(entityHelper) //
				.compositeHelper(compositeHelper) //
				.keyFactory(keyFactory) //
				.keyValueFactory(keyValueFactory) //
				.iteratorFactory(iteratorFactory) //
				.build();
	}

	@SuppressWarnings("unchecked")
	private <K extends Comparable<K>, V> Object buildColumnFamilyWrapper(
			PropertyMeta<K, V> propertyMeta)
	{
		return ExternalWideMapWrapperBuilder
				.builder(key, (GenericCompositeDao<ID, V>) columnFamilyDao, propertyMeta) //
				.interceptor(this) //
				.compositeHelper(compositeHelper) //
				.keyValueFactory(keyValueFactory)//
				.iteratorFactory(iteratorFactory)//
				.compositeKeyFactory(compositeKeyFactory) //
				.build();
	}

	private Object interceptSetter(Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		PropertyMeta<?, ?> propertyMeta = this.setterMetas.get(method);
		Object result = null;
		switch (propertyMeta.type())
		{
			case WIDE_MAP:
			case JOIN_WIDE_MAP:
			case EXTERNAL_WIDE_MAP:
				throw new UnsupportedOperationException(
						"Cannot set value directly to a WideMap structure. Please call the getter first to get handle on the wrapper");
			default:

				this.dirtyMap.put(method, propertyMeta);
				result = proxy.invoke(target, args);
		}

		return result;
	}

	public Map<Method, PropertyMeta<?, ?>> getDirtyMap()
	{
		return dirtyMap;
	}

	public Set<Method> getLazyLoaded()
	{
		return lazyLoaded;
	}

	@Override
	public ID getKey()
	{
		return key;
	}

	public void setTarget(T target)
	{
		this.target = target;
	}

	void setEntityDao(GenericDynamicCompositeDao<ID> dao)
	{
		this.entityDao = dao;
	}

	public <V> void setColumnFamilyDao(GenericCompositeDao<ID, V> columnFamilyDao)
	{
		this.columnFamilyDao = columnFamilyDao;
	}

	void setKey(ID key)
	{
		this.key = key;
	}

	void setIdGetter(Method idGetter)
	{
		this.idGetter = idGetter;
	}

	void setIdSetter(Method idSetter)
	{
		this.idSetter = idSetter;
	}

	void setGetterMetas(Map<Method, PropertyMeta<?, ?>> getterMetas)
	{
		this.getterMetas = getterMetas;
	}

	void setSetterMetas(Map<Method, PropertyMeta<?, ?>> setterMetas)
	{
		this.setterMetas = setterMetas;
	}

	void setDirtyMap(Map<Method, PropertyMeta<?, ?>> dirtyMap)
	{
		this.dirtyMap = dirtyMap;
	}

	void setLazyLoaded(Set<Method> lazyLoaded)
	{
		this.lazyLoaded = lazyLoaded;
	}

	void setLoader(EntityLoader loader)
	{
		this.loader = loader;
	}

	public void setDirectColumnFamilyMapping(Boolean directColumnFamilyMapping)
	{
		this.directColumnFamilyMapping = directColumnFamilyMapping;
	}

	public Boolean getDirectColumnFamilyMapping()
	{
		return directColumnFamilyMapping;
	}

	public Mutator<ID> getMutator()
	{
		return mutator;
	}

	public void setMutator(Mutator<ID> mutator)
	{
		this.mutator = mutator;
	}

	public Map<String, Mutator<?>> getMutatorMap()
	{
		return mutatorMap;
	}

	@Override
	public Mutator<?> getMutatorForProperty(String property)
	{
		if (mutatorMap != null)
		{
			return mutatorMap.get(property);
		}
		else
		{
			return null;
		}
	}

	public void setMutatorMap(Map<String, Mutator<?>> mutatorMap)
	{
		this.mutatorMap = mutatorMap;
	}

	@Override
	public boolean isBatchMode()
	{
		return this.mutator != null;
	}
}
