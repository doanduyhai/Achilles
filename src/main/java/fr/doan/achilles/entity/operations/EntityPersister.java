package fr.doan.achilles.entity.operations;

import static me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality.GREATER_THAN_EQUAL;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.AbstractComposite.ComponentEquality;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.Validate;

import fr.doan.achilles.composite.factory.DynamicCompositeKeyFactory;
import fr.doan.achilles.dao.GenericEntityDao;
import fr.doan.achilles.entity.EntityHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ListMeta;
import fr.doan.achilles.entity.metadata.MapMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SetMeta;
import fr.doan.achilles.holder.KeyValueHolder;

public class EntityPersister
{

	private EntityHelper entityHelper = new EntityHelper();

	private DynamicCompositeKeyFactory keyFactory = new DynamicCompositeKeyFactory();

	public <ID> void persist(Object entity, EntityMeta<ID> entityMeta)
	{

		ID key = entityHelper.getKey(entity, entityMeta.getIdMeta());
		Validate.notNull(key, "key value for entity '" + entityMeta.getCanonicalClassName() + "'");
		GenericEntityDao<ID> dao = entityMeta.getEntityDao();

		Mutator<ID> mutator = dao.buildMutator();

		for (Entry<String, PropertyMeta<?, ?>> entry : entityMeta.getPropertyMetas().entrySet())
		{
			PropertyMeta<?, ?> propertyMeta = entry.getValue();
			switch (propertyMeta.propertyType())
			{
				case SIMPLE:
				case LAZY_SIMPLE:
					this.batchSimpleProperty(entity, key, dao, propertyMeta, mutator);
					break;
				case LIST:
				case LAZY_LIST:
					this.batchListProperty(entity, key, dao, propertyMeta, mutator);
					break;
				case SET:
				case LAZY_SET:
					this.batchSetProperty(entity, key, dao, propertyMeta, mutator);
					break;
				case MAP:
				case LAZY_MAP:
					this.batchMapProperty(entity, key, dao, propertyMeta, mutator);
					break;
				default:
					break;
			}
		}
		mutator.execute();

	}

	public <ID> void remove(Object entity, EntityMeta<ID> entityMeta)
	{
		ID key = entityHelper.getKey(entity, entityMeta.getIdMeta());
		Validate.notNull(key, "key value for entity '" + entityMeta.getCanonicalClassName() + "'");
		GenericEntityDao<ID> dao = entityMeta.getEntityDao();
		dao.removeRow(key);
	}

	public <ID, V> void removeProperty(ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{
		Validate.notNull(key, "key value");
		DynamicComposite start = keyFactory.createBaseForQuery(propertyMeta,
				ComponentEquality.EQUAL);
		DynamicComposite end = keyFactory.createBaseForQuery(propertyMeta, GREATER_THAN_EQUAL);
		dao.removeColumnRange(key, start, end);
	}

	private <ID> void batchSimpleProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		DynamicComposite name = keyFactory.createForBatchInsert(propertyMeta, 0);
		Object value = entityHelper.getValueFromField(entity, propertyMeta.getGetter());
		if (value != null)
		{
			dao.insertColumn(key, name, value, mutator);
		}
	}

	public <ID> void persistSimpleProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta)
	{
		this.batchSimpleProperty(entity, key, dao, propertyMeta, null);
	}

	private <ID> void batchListProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{

		List<?> list = (List<?>) entityHelper.getValueFromField(entity, propertyMeta.getGetter());
		int count = 0;
		if (list != null)
		{
			for (Object value : list)
			{
				DynamicComposite name = keyFactory.createForBatchInsert(propertyMeta, count);
				if (value != null)
				{
					dao.insertColumn(key, name, value, mutator);
				}
				count++;
			}
		}
	}

	public <ID> void persistListProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchListProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	private <ID> void batchSetProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{
		Set<?> set = (Set<?>) entityHelper.getValueFromField(entity, propertyMeta.getGetter());
		if (set != null)
		{
			for (Object value : set)
			{
				DynamicComposite name = keyFactory.createForBatchInsert(propertyMeta, value.hashCode());
				if (value != null)
				{
					dao.insertColumn(key, name, value, mutator);
				}
			}
		}
	}

	public <ID> void persistSetProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchSetProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	private <ID> void batchMapProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta, Mutator<ID> mutator)
	{

		Map<?, ?> map = (Map<?, ?>) entityHelper
				.getValueFromField(entity, propertyMeta.getGetter());
		if (map != null)
		{
			for (Entry<?, ?> entry : map.entrySet())
			{
				DynamicComposite name = keyFactory.createForBatchInsert(propertyMeta, entry.getKey()
						.hashCode());

				KeyValueHolder value = new KeyValueHolder(entry.getKey(), entry.getValue());
				dao.insertColumn(key, name, value, mutator);
			}
		}
	}

	public <ID> void persistMapProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, ?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchMapProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	@SuppressWarnings("unchecked")
	public <ID, V> void persistProperty(Object entity, ID key, GenericEntityDao<ID> dao,
			PropertyMeta<?, V> propertyMeta)
	{

		switch (propertyMeta.propertyType())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				this.persistSimpleProperty(entity, key, dao, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				this.persistListProperty(entity, key, dao, (ListMeta<V>) propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				this.persistSetProperty(entity, key, dao, (SetMeta<V>) propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				this.persistMapProperty(entity, key, dao, (MapMeta<?, V>) propertyMeta);
				break;
			default:
				break;
		}
	}
}
