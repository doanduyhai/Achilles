package info.archinnov.achilles.entity.operations.impl;

import static info.archinnov.achilles.helper.ThriftLoggerHelper.format;
import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.*;
import info.archinnov.achilles.composite.factory.CompositeFactory;
import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.AchillesEntityIntrospector;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.metadata.PropertyType;
import info.archinnov.achilles.entity.operations.ThriftEntityLoader;
import info.archinnov.achilles.entity.type.KeyValue;
import info.archinnov.achilles.entity.type.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.Composite;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftLoaderImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftLoaderImpl
{
	private static final Logger log = LoggerFactory.getLogger(ThriftLoaderImpl.class);

	private ThriftEntityMapper mapper = new ThriftEntityMapper();
	private AchillesEntityIntrospector introspector = new AchillesEntityIntrospector();
	private CompositeFactory compositeFactory = new CompositeFactory();

	public <T> T load(ThriftPersistenceContext context, Class<T> entityClass) throws Exception
	{
		log.trace("Loading entity of class {} with primary key {}", context.getEntityClass()
				.getCanonicalName(), context.getPrimaryKey());
		EntityMeta entityMeta = context.getEntityMeta();
		Object primaryKey = context.getPrimaryKey();

		List<Pair<Composite, String>> columns = context.getEntityDao().eagerFetchEntity(primaryKey);
		T entity = null;
		if (columns.size() > 0)
		{
			log.trace("Mapping data from Cassandra columns to entity");

			entity = entityClass.newInstance();
			mapper.setEagerPropertiesToEntity(primaryKey, columns, entityMeta, entity);
			introspector.setValueToField(entity, entityMeta.getIdMeta().getSetter(), primaryKey);
		}

		return (T) entity;
	}

	public <V> Long loadVersionSerialUID(Object key, ThriftGenericEntityDao dao)
	{
		Composite composite = new Composite();
		composite.addComponent(0, PropertyType.SERIAL_VERSION_UID.flag(), ComponentEquality.EQUAL);
		composite.addComponent(1, PropertyType.SERIAL_VERSION_UID.name(), ComponentEquality.EQUAL);
		composite.addComponent(2, 0, ComponentEquality.EQUAL);

		String serialVersionUIDString = dao.getValue(key, composite);
		if (StringUtils.isNotBlank(serialVersionUIDString))
		{
			log.trace("Serial version UID {} found for column family {} and primary key {}",
					serialVersionUIDString, dao.getColumnFamily(), key);
			return Long.parseLong(serialVersionUIDString);
		}
		else
		{
			log.trace("No serial version UID found for column family {} and primary key {}",
					dao.getColumnFamily(), key);
			return null;
		}
	}

	public <V> V loadSimpleProperty(ThriftPersistenceContext context,
			PropertyMeta<?, V> propertyMeta)
	{
		Composite composite = compositeFactory.createBaseForGet(propertyMeta);
		if (log.isTraceEnabled())
		{
			log.trace(
					"Loading simple property {} of class {} from column family {} with primary key {} and composite column name {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), context
							.getEntityMeta().getTableName(), context.getPrimaryKey(),
					format(composite));
		}
		return propertyMeta.getValueFromString(context.getEntityDao().getValue(
				context.getPrimaryKey(), composite));
	}

	public <V> List<V> loadListProperty(ThriftPersistenceContext context,
			PropertyMeta<?, V> propertyMeta)
	{
		log.trace("Loading list property {} of class {} from column family {} with primary key {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), context
						.getEntityMeta().getTableName(), context.getPrimaryKey());
		List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
		List<V> list = null;
		if (columns.size() > 0)
		{
			list = new ArrayList<V>();
			for (Pair<Composite, String> pair : columns)
			{
				list.add(propertyMeta.getValueFromString(pair.right));
			}
		}
		return list;
	}

	public <V> Set<V> loadSetProperty(ThriftPersistenceContext context,
			PropertyMeta<?, V> propertyMeta)
	{
		log.trace("Loading set property {} of class {} from column family {} with primary key {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), context
						.getEntityMeta().getTableName(), context.getPrimaryKey());
		List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
		Set<V> set = null;
		if (columns.size() > 0)
		{
			set = new HashSet<V>();
			for (Pair<Composite, String> pair : columns)
			{
				set.add(propertyMeta.getValueFromString(pair.right));
			}
		}
		return set;
	}

	public <K, V> Map<K, V> loadMapProperty(ThriftPersistenceContext context,
			PropertyMeta<K, V> propertyMeta)
	{
		log.trace("Loading map property {} of class {} from column family {} with primary key {}",
				propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), context
						.getEntityMeta().getTableName(), context.getPrimaryKey());
		List<Pair<Composite, String>> columns = fetchColumns(context, propertyMeta);
		Class<K> keyClass = propertyMeta.getKeyClass();
		Map<K, V> map = null;
		if (columns.size() > 0)
		{
			map = new HashMap<K, V>();
			for (Pair<Composite, String> pair : columns)
			{
				KeyValue<K, V> holder = propertyMeta.getKeyValueFromString(pair.right);

				map.put(keyClass.cast(holder.getKey()), propertyMeta.castValue(holder.getValue()));
			}
		}
		return map;
	}

	private <V> List<Pair<Composite, String>> fetchColumns(ThriftPersistenceContext context,
			PropertyMeta<?, V> propertyMeta)
	{

		Composite start = compositeFactory.createBaseForQuery(propertyMeta, EQUAL);
		Composite end = compositeFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		if (log.isTraceEnabled())
		{
			log.trace("Fetching columns from Cassandra with column names {} / {}", format(start),
					format(end));
		}
		List<Pair<Composite, String>> columns = context.getEntityDao().findColumnsRange(
				context.getPrimaryKey(), start, end, false, Integer.MAX_VALUE);
		return columns;
	}

	public <V> V loadJoinSimple(ThriftPersistenceContext context, PropertyMeta<?, V> propertyMeta,
			ThriftEntityLoader loader)
	{
		EntityMeta joinMeta = propertyMeta.joinMeta();
		PropertyMeta<?, ?> joinIdMeta = propertyMeta.joinIdMeta();

		Composite composite = compositeFactory.createBaseForGet(propertyMeta);

		if (log.isTraceEnabled())
		{
			log.trace(
					"Loading join primary key for property {} of class {} from column family {} with primary key {} and column name {}",
					propertyMeta.getPropertyName(), propertyMeta.getEntityClassName(), context
							.getEntityMeta().getTableName(), context.getPrimaryKey(),
					format(composite));
		}
		String stringJoinId = context.getEntityDao().getValue(context.getPrimaryKey(), composite);

		if (stringJoinId != null)
		{
			Object joinId = joinIdMeta.getValueFromString(stringJoinId);
			AchillesPersistenceContext joinContext = context.newPersistenceContext(
					propertyMeta.getValueClass(), joinMeta, joinId);
			return loader.<V> load(joinContext, propertyMeta.getValueClass());

		}
		else
		{
			return (V) null;
		}
	}
}
