package info.archinnov.achilles.proxy.interceptor;

import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.GenericColumnFamilyDao;
import info.archinnov.achilles.entity.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.CounterProperties;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityLoader;
import info.archinnov.achilles.entity.operations.EntityPersister;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.entity.type.Counter;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.wrapper.builder.CounterWideMapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.CounterWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.JoinWideMapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.ListWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.MapWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.SetWrapperBuilder;
import info.archinnov.achilles.wrapper.builder.WideMapWrapperBuilder;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;
import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JpaEntityInterceptor
 * 
 * @author DuyHai DOAN
 * 
 */
public class JpaEntityInterceptor<ID, T> implements MethodInterceptor, AchillesInterceptor<ID>
{
	private static final Logger log = LoggerFactory.getLogger(JpaEntityInterceptor.class);

	private EntityLoader loader = new EntityLoader();
	private CompositeHelper compositeHelper = new CompositeHelper();
	private KeyValueFactory keyValueFactory = new KeyValueFactory();
	private IteratorFactory iteratorFactory = new IteratorFactory();
	private CompositeFactory compositeFactory = new CompositeFactory();
	private EntityPersister persister = new EntityPersister();
	private EntityProxifier proxifier = new EntityProxifier();

	private T target;
	private ID key;
	private Method idGetter;
	private Method idSetter;
	private Map<Method, PropertyMeta<?, ?>> getterMetas;
	private Map<Method, PropertyMeta<?, ?>> setterMetas;
	private Map<Method, PropertyMeta<?, ?>> dirtyMap;
	private Set<Method> lazyAlreadyLoaded;
	private PersistenceContext<ID> context;

	@Override
	public Object getTarget()
	{
		return this.target;
	}

	@Override
	public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		log.trace("Method {} called for entity of class {}", method.getName(), target.getClass()
				.getCanonicalName());

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
		Object result = null;
		PropertyMeta propertyMeta = this.getterMetas.get(method);

		// Load lazy into target object
		if (propertyMeta.type().isLazy() && !this.lazyAlreadyLoaded.contains(method))
		{
			log.trace("Loading property {}", propertyMeta.getPropertyName());

			this.loader.loadPropertyIntoObject(target, key, context, propertyMeta);
			this.lazyAlreadyLoaded.add(method);
		}

		log.trace("Invoking getter {} on real object", method.getName());
		Object rawValue = proxy.invoke(target, args);

		// Build proxy when necessary
		switch (propertyMeta.type())
		{
			case COUNTER:
				log.trace("Build counter wrapper for property {} of entity of class {} ",
						propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());
				result = buildCounterWrapper(propertyMeta);
				break;
			case JOIN_SIMPLE:
				if (rawValue != null)
				{
					log.trace(
							"Build proxy on returned join entity for property {} of entity of class {} ",
							propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

					PersistenceContext<?> joinContext = context.newPersistenceContext(
							propertyMeta.joinMeta(), rawValue);
					result = proxifier.buildProxy(rawValue, joinContext);
				}
				break;
			case LIST:
			case LAZY_LIST:
			case JOIN_LIST:
				if (rawValue != null)
				{
					log.trace("Build list wrapper for property {} of entity of class {} ",
							propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

					List<?> list = (List<?>) rawValue;
					result = ListWrapperBuilder //
							.builder(context, list) //
							.dirtyMap(dirtyMap) //
							.setter(propertyMeta.getSetter()) //
							.propertyMeta(propertyMeta) //
							.proxifier(proxifier)//
							.build();
				}
				break;
			case SET:
			case LAZY_SET:
			case JOIN_SET:
				if (rawValue != null)
				{
					log.trace("Build set wrapper for property {} of entity of class {} ",
							propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

					Set<?> set = (Set<?>) rawValue;
					result = SetWrapperBuilder //
							.builder(context, set) //
							.dirtyMap(dirtyMap) //
							.setter(propertyMeta.getSetter())//
							.propertyMeta(propertyMeta) //
							.proxifier(proxifier)//
							.build();
				}
				break;
			case MAP:
			case LAZY_MAP:
			case JOIN_MAP:
				if (rawValue != null)
				{
					log.trace("Build map wrapper for property {} of entity of class {} ",
							propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

					Map<?, ?> map = (Map<?, ?>) rawValue;
					result = MapWrapperBuilder //
							.builder(context, map)//
							.dirtyMap(dirtyMap) //
							.setter(propertyMeta.getSetter()) //
							.propertyMeta(propertyMeta) //
							.proxifier(proxifier)//
							.build();
				}
				break;
			case WIDE_MAP:
				if (context.isDirectColumnFamilyMapping())
				{
					log.trace(
							"Build direct column family wide map wrapper for property {} of entity of class {} ",
							propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

					result = buildColumnFamilyWrapper(propertyMeta);
				}
				else
				{
					log.trace("Build wide map wrapper for property {} of entity of class {} ",
							propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

					result = buildWideMapWrapper(propertyMeta);
				}
				break;
			case COUNTER_WIDE_MAP:

				log.trace("Build counter wide wrapper for property {} of entity of class {} ",
						propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

				result = buildCounterWideMapWrapper(propertyMeta);
				break;
			case JOIN_WIDE_MAP:

				log.trace("Build join wide wrapper for property {} of entity of class {} ",
						propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

				result = buildJoinWideMapWrapper(propertyMeta);
				break;
			default:
				log.trace("Return un-mapped raw value {} for property {} of entity of class {} ",
						propertyMeta.getPropertyName(), propertyMeta.getEntityClassName());

				result = rawValue;
				break;
		}
		return result;
	}

	@SuppressWarnings("unchecked")
	private Object buildCounterWrapper(PropertyMeta<ID, ?> propertyMeta)
	{
		Object result;
		CounterProperties counterProperties = propertyMeta.getCounterProperties();
		Composite keyComp = compositeFactory.createKeyForCounter(counterProperties.getFqcn(), key,
				(PropertyMeta<Void, ID>) counterProperties.getIdMeta());
		Composite comp = compositeFactory.createBaseForCounterGet(propertyMeta);
		result = CounterWrapperBuilder.builder(keyComp) //
				.counterDao(context.getCounterDao()) //
				.columnName(comp) //
				.readLevel(propertyMeta.getReadConsistencyLevel()) //
				.writeLevel(propertyMeta.getWriteConsistencyLevel()) //
				.context(context) //
				.build();
		return result;
	}

	@SuppressWarnings("unchecked")
	private <K, V> Object buildWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		String columnFamilyName = context.isDirectColumnFamilyMapping() ? context.getEntityMeta()
				.getColumnFamilyName() : propertyMeta.getExternalCFName();

		GenericColumnFamilyDao<ID, V> columnFamilyDao = (GenericColumnFamilyDao<ID, V>) context
				.findColumnFamilyDao(columnFamilyName);

		return WideMapWrapperBuilder //
				.builder(key, columnFamilyDao, propertyMeta) //
				.context(context) //
				.interceptor(this) //
				.compositeHelper(compositeHelper) //
				.keyValueFactory(keyValueFactory)//
				.iteratorFactory(iteratorFactory)//
				.compositeFactory(compositeFactory) //
				.build();
	}

	@SuppressWarnings("unchecked")
	private <K> Object buildCounterWideMapWrapper(PropertyMeta<K, Counter> propertyMeta)
	{
		GenericColumnFamilyDao<ID, Long> counterWideMapDao = (GenericColumnFamilyDao<ID, Long>) context
				.findColumnFamilyDao(propertyMeta.getExternalCFName());

		return CounterWideMapWrapperBuilder //
				.builder(key, counterWideMapDao, propertyMeta)//
				.interceptor(this) //
				.context(context) //
				.compositeHelper(compositeHelper) //
				.keyValueFactory(keyValueFactory) //
				.iteratorFactory(iteratorFactory) //
				.compositeFactory(compositeFactory) //
				.build();
	}

	@SuppressWarnings("unchecked")
	private <K, JOIN_ID, V> Object buildJoinWideMapWrapper(PropertyMeta<K, V> propertyMeta)
	{
		String columnFamilyName = context.isDirectColumnFamilyMapping() ? context.getEntityMeta()
				.getColumnFamilyName() : propertyMeta.getExternalCFName();
		GenericColumnFamilyDao<ID, JOIN_ID> columnFamilyDao = (GenericColumnFamilyDao<ID, JOIN_ID>) context
				.findColumnFamilyDao(columnFamilyName);

		return JoinWideMapWrapperBuilder //
				.builder(key, columnFamilyDao, propertyMeta) //
				.interceptor(this) //
				.context(context) //
				.compositeHelper(compositeHelper) //
				.compositeFactory(compositeFactory) //
				.proxifier(proxifier)//
				.iteratorFactory(iteratorFactory) //
				.keyValueFactory(keyValueFactory) //
				.loader(loader) //
				.persister(persister) //
				.build();
	}

	@SuppressWarnings("unchecked")
	private <K, V> Object buildColumnFamilyWrapper(PropertyMeta<K, V> propertyMeta)
	{
		GenericColumnFamilyDao<ID, V> columnFamilyDao = (GenericColumnFamilyDao<ID, V>) context
				.findColumnFamilyDao(context.getEntityMeta().getColumnFamilyName());

		return WideMapWrapperBuilder.builder(key, columnFamilyDao, propertyMeta) //
				.interceptor(this) //
				.context(context) //
				.compositeHelper(compositeHelper) //
				.keyValueFactory(keyValueFactory)//
				.iteratorFactory(iteratorFactory)//
				.compositeFactory(compositeFactory) //
				.build();
	}

	private Object interceptSetter(Method method, Object[] args, MethodProxy proxy)
			throws Throwable
	{
		PropertyMeta<?, ?> propertyMeta = this.setterMetas.get(method);
		Object result = null;

		switch (propertyMeta.type())
		{
			case COUNTER:
				throw new UnsupportedOperationException(
						"Cannot set value directly to a Counter type. Please call the getter first to get handle on the wrapper");
			case WIDE_MAP:
				throw new UnsupportedOperationException(
						"Cannot set value directly to a WideMap structure. Please call the getter first to get handle on the wrapper");
			default:
				break;
		}

		if (propertyMeta.type().isLazy())
		{
			this.lazyAlreadyLoaded.add(propertyMeta.getGetter());
		}
		log.trace("Flaging property {}", propertyMeta.getPropertyName());

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

	public PersistenceContext<ID> getContext()
	{
		return context;
	}

	public void setContext(PersistenceContext<ID> context)
	{
		this.context = context;
	}
}
