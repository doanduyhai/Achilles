package info.archinnov.achilles.wrapper.builder;

import info.archinnov.achilles.composite.factory.DynamicCompositeKeyFactory;
import info.archinnov.achilles.dao.GenericDynamicCompositeDao;
import info.archinnov.achilles.entity.manager.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityProxifier;
import info.archinnov.achilles.helper.CompositeHelper;
import info.archinnov.achilles.iterator.factory.IteratorFactory;
import info.archinnov.achilles.iterator.factory.KeyValueFactory;
import info.archinnov.achilles.proxy.interceptor.AchillesInterceptor;
import info.archinnov.achilles.wrapper.WideMapWrapper;

/**
 * WideMapWrapperBuilder
 * 
 * @author DuyHai DOAN
 * 
 */
public class WideMapWrapperBuilder<ID, K, V>
{
	private ID id;
	protected GenericDynamicCompositeDao<ID> entityDao;
	private PropertyMeta<K, V> wideMapMeta;
	private AchillesInterceptor interceptor;
	private EntityProxifier proxifier;
	private CompositeHelper compositeHelper;
	private KeyValueFactory keyValueFactory;
	private IteratorFactory iteratorFactory;
	private DynamicCompositeKeyFactory keyFactory;
	protected PersistenceContext<ID> context;

	public WideMapWrapperBuilder(ID id, GenericDynamicCompositeDao<ID> dao,
			PropertyMeta<K, V> wideMapMeta)
	{
		this.entityDao = dao;
		this.id = id;
		this.wideMapMeta = wideMapMeta;
	}

	public static <ID, K, V> WideMapWrapperBuilder<ID, K, V> builder(ID id,
			GenericDynamicCompositeDao<ID> dao, PropertyMeta<K, V> wideMapMeta)
	{
		return new WideMapWrapperBuilder<ID, K, V>(id, dao, wideMapMeta);
	}

	public WideMapWrapperBuilder<ID, K, V> context(PersistenceContext<ID> context)
	{
		this.context = context;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> interceptor(AchillesInterceptor interceptor)
	{
		this.interceptor = interceptor;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> proxifier(EntityProxifier proxifier)
	{
		this.proxifier = proxifier;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> compositeHelper(CompositeHelper compositeHelper)
	{
		this.compositeHelper = compositeHelper;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> keyValueFactory(KeyValueFactory keyValueFactory)
	{
		this.keyValueFactory = keyValueFactory;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> iteratorFactory(IteratorFactory iteratorFactory)
	{
		this.iteratorFactory = iteratorFactory;
		return this;
	}

	public WideMapWrapperBuilder<ID, K, V> keyFactory(DynamicCompositeKeyFactory keyFactory)
	{
		this.keyFactory = keyFactory;
		return this;
	}

	public WideMapWrapper<ID, K, V> build()
	{
		WideMapWrapper<ID, K, V> wrapper = new WideMapWrapper<ID, K, V>();
		build(wrapper);
		return wrapper;
	}

	protected void build(WideMapWrapper<ID, K, V> wrapper)
	{
		wrapper.setId(id);
		wrapper.setEntityDao(entityDao);
		wrapper.setWideMapMeta(wideMapMeta);
		wrapper.setInterceptor(interceptor);
		wrapper.setEntityProxifier(proxifier);
		wrapper.setCompositeHelper(compositeHelper);
		wrapper.setIteratorFactory(iteratorFactory);
		wrapper.setKeyFactory(keyFactory);
		wrapper.setKeyValueFactory(keyValueFactory);
		wrapper.setContext(context);
	}

}
