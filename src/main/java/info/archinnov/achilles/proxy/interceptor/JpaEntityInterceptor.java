package info.archinnov.achilles.proxy.interceptor;

import info.archinnov.achilles.composite.factory.CompositeKeyFactory;
import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.AbstractDao;
import info.archinnov.achilles.dao.GenericCompositeDao;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.dao.Pair;
import info.archinnov.achilles.entity.EntityHelper;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.wrapper.builder.CounterWideMapWrapperBuilder;
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
	private DynamicCompositeKeyFactory dynamicCompositeKeyFactory = new DynamicCompositeKeyFactory();
	private EntityPersister persister = new EntityPersister();
	private EntityHelper entityHelper = new EntityHelper();

	private T target;
	private ID key;
	private Method idGetter;
	private Method idSetter;
	private Map<Method, PropertyMeta<?, ?>> getterMetas;
	private Map<Method, PropertyMeta<?, ?>> setterMetas;
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;
	private Set<Method> lazyAlreadyLoaded;
	private PersistenceContext<ID> context;
	private Mutator<ID> mutator;
	private Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> mutatorMap;

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
	private <JOIN_ID> Object interceptGetter(Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		Object result = null;
		PropertyMeta propertyMeta = this.getterMetas.get(method);

		// Load lazy into target object
		if (propertyMeta.type().isLazy() && !this.lazyAlreadyLoaded.contains(method))
		{
			this.loader.loadPropertyIntoObject(target, key, context, propertyMeta);
			this.lazyAlreadyLoaded.add(method);
		}

		Object rawValue = proxy.invoke(target, args);

		// Build proxy when necessary
		switch (propertyMeta.type())
		{
			case JOIN_SIMPLE:
				if (rawValue != null)
				{
					PersistenceContext<JOIN_ID> joinContext = context.newPersistenceContext(
							propertyMeta.joinMeta(), rawValue);
					result = entityHelper.buildProxy(rawValue, joinContext);
				}
				break;
			case LIST:
			case LAZY_LIST:
			case JOIN_LIST:
				if (rawValue != null)
				{
					List<?> list = (List<?>) rawValue;
					result = ListWrapperBuilder //
							.builder(context, list) //
							.dirtyMap(dirtyMap) //
							.setter(propertyMeta.getSetter()) //
							.propertyMeta(propertyMeta) //
							.helper(entityHelper) //
							.build();
				}
				break;
			case SET:
			case LAZY_SET:
			case JOIN_SET:
				if (rawValue != null)
				{
					Set<?> set = (Set<?>) rawValue;
					result = SetWrapperBuilder //
							.builder(context, set) //
							.dirtyMap(dirtyMap) //
							.setter(propertyMeta.getSetter())//
							.propertyMeta(propertyMeta) //
							.helper(entityHelper) //
							.build();
				}
				break;
			case MAP:
			case LAZY_MAP:
			case JOIN_MAP:
				if (rawValue != null)
				{
					Map<?, ?> map = (Map<?, ?>) rawValue;
					result = MapWrapperBuilder //
							.builder(context, map)//
							.dirtyMap(dirtyMap) //
							.setter(propertyMeta.getSetter()) //
							.propertyMeta(propertyMeta) //
							.helper(entityHelper) //
							.build();
				}
				break;
			case WIDE_MAP:
				if (context.isDirectColumnFamilyMapping())
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
			case WIDE_MAP_COUNTER:
				result = buildCounterWideMapWrapper(propertyMeta);
				break;
			case JOIN_WIDE_MAP:
				result = buildJoinWideMapWrapper(propertyMeta);
				break;
			case EXTERNAL_JOIN_WIDE_MAP:
				result = buildExternalJoinWideMapWrapper(propertyMeta);
				break;
			default:
				result = rawValue;
				break;
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private <K, V> Object buildExternalWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		String columnFamilyName = context.isDirectColumnFamilyMapping() ? context.getEntityMeta()
				.getColumnFamilyName() : propertyMeta.getExternalWideMapProperties()
				.getExternalColumnFamilyName();

		GenericCompositeDao<ID, V> columnFamilyDao = (GenericCompositeDao<ID, V>) context
				.findColumnFamilyDao(columnFamilyName);

		return ExternalWideMapWrapperBuilder //
				.builder(key, columnFamilyDao, propertyMeta) //
				.context(context) //
				.interceptor(this) //
				.compositeHelper(compositeHelper) //
				.keyValueFactory(keyValueFactory)//
				.iteratorFactory(iteratorFactory)//
				.compositeKeyFactory(compositeKeyFactory) //
				.build();
	}

	@SuppressWarnings(
	{
			"rawtypes",
			"unchecked"
	})
	private <K> Object buildCounterWideMapWrapper(PropertyMeta<K, Long> propertyMeta)
	{
		return CounterWideMapWrapperBuilder //
				.builder(key, context.getCounterDao(), propertyMeta)//
				.interceptor(this) //
				.context(context) //
				.fqcn(propertyMeta.fqcn()) //
				.idMeta((PropertyMeta) propertyMeta.counterIdMeta()) //
				.compositeHelper(compositeHelper) //
				.keyValueFactory(keyValueFactory) //
				.iteratorFactory(iteratorFactory) //
				.compositeKeyFactory(compositeKeyFactory) //
				.dynamicCompositeKeyFactory(dynamicCompositeKeyFactory) //
				.build();
	}

	@SuppressWarnings("unchecked")
	private <K, JOIN_ID, V> Object buildExternalJoinWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		String columnFamilyName = context.isDirectColumnFamilyMapping() ? context.getEntityMeta()
				.getColumnFamilyName() : propertyMeta.getExternalWideMapProperties()
				.getExternalColumnFamilyName();
		GenericCompositeDao<ID, JOIN_ID> columnFamilyDao = (GenericCompositeDao<ID, JOIN_ID>) context
				.findColumnFamilyDao(columnFamilyName);

		return JoinExternalWideMapWrapperBuilder //
				.builder(key, columnFamilyDao, propertyMeta) //
				.interceptor(this) //
				.context(context) //
				.compositeHelper(compositeHelper) //
				.compositeKeyFactory(compositeKeyFactory) //
				.entityHelper(entityHelper) //
				.iteratorFactory(iteratorFactory) //
				.keyValueFactory(keyValueFactory) //
				.loader(loader) //
				.persister(persister) //
				.build();
	}

	private <K, V> Object buildWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		GenericDynamicCompositeDao<ID> entityDao = context.findEntityDao(context.getEntityMeta()
				.getColumnFamilyName());

		return WideMapWrapperBuilder //
				.builder(key, entityDao, propertyMeta) //
				.interceptor(this) //
				.context(context) //
				.entityHelper(entityHelper) //
				.compositeHelper(compositeHelper) //
				.keyFactory(dynamicCompositeKeyFactory) //
				.keyValueFactory(keyValueFactory) //
				.iteratorFactory(iteratorFactory) //
				.build();
	}

	private <K, V> Object buildJoinWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		GenericDynamicCompositeDao<ID> entityDao = context.findEntityDao(context.getEntityMeta()
				.getColumnFamilyName());

		return JoinWideMapWrapperBuilder //
				.builder(context, key, entityDao, propertyMeta) //
				.loader(loader) //
				.persister(persister) //
				.interceptor(this) //
				.entityHelper(entityHelper) //
				.compositeHelper(compositeHelper) //
				.keyFactory(dynamicCompositeKeyFactory) //
				.keyValueFactory(keyValueFactory) //
				.iteratorFactory(iteratorFactory) //
				.build();
	}

	@SuppressWarnings("unchecked")
	private <K, V> Object buildColumnFamilyWrapper(PropertyMeta<K, V> propertyMeta)
	{
		GenericCompositeDao<ID, V> columnFamilyDao = (GenericCompositeDao<ID, V>) context
				.findColumnFamilyDao(context.getEntityMeta().getColumnFamilyName());

		return ExternalWideMapWrapperBuilder.builder(key, columnFamilyDao, propertyMeta) //
				.interceptor(this) //
				.context(context) //
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

		if (propertyMeta.type().isWideMap())
		{
			throw new UnsupportedOperationException(
					"Cannot set value directly to a WideMap structure. Please call the getter first to get handle on the wrapper");
		}

		if (propertyMeta.type().isLazy())
		{
			this.lazyAlreadyLoaded.add(propertyMeta.getGetter());
		}
		this.dirtyMap.put(method, propertyMeta);
		result = proxy.invoke(target, args);
		return result;
	}

	public Map<Method, PropertyMeta<?, ?>> getDirtyMap()
	{
		return dirtyMap;
	}

	public Set<Method> getLazyAlreadyLoaded()
	{
		return lazyAlreadyLoaded;
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
		this.lazyAlreadyLoaded = lazyLoaded;
	}

	void setLoader(EntityLoader loader)
	{
		this.loader = loader;
	}

	@Override
	public Mutator<ID> getMutator()
	{
		return mutator;
	}

	public void setMutator(Mutator<ID> mutator)
	{
		this.mutator = mutator;
	}

	public Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> getMutatorMap()
	{
		return mutatorMap;
	}

	@Override
	public Mutator<?> getMutatorForProperty(String property)
	{
		Mutator<?> mutator = null;
		if (mutatorMap != null)
		{
			Pair<Mutator<?>, AbstractDao<?, ?, ?>> pair = mutatorMap.get(property);
			if (pair != null)
			{
				mutator = mutatorMap.get(property).left;
			}
		}
		return mutator;
	}

	public AbstractDao<?, ?, ?> getDaoForProperty(String property)
	{
		AbstractDao<?, ?, ?> dao = null;
		if (mutatorMap != null)
		{
			Pair<Mutator<?>, AbstractDao<?, ?, ?>> pair = mutatorMap.get(property);
			if (pair != null)
			{
				dao = mutatorMap.get(property).right;
			}
		}
		return dao;
	}

	public void setMutatorMap(Map<String, Pair<Mutator<?>, AbstractDao<?, ?, ?>>> mutatorMap)
	{
		this.mutatorMap = mutatorMap;
	}

	@Override
	public boolean isBatchMode()
	{
		return this.mutator != null;
	}

	public PersistenceContext<ID> getContext()
	{
		return context;
	}

	public void setContext(PersistenceContext<ID> context)
	{
		this.context = context;
	}
}
