package fr.doan.achilles.operations;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.Validate;

import fr.doan.achilles.bean.BeanPropertyHelper;
import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.holder.KeyValueHolder;
import fr.doan.achilles.metadata.EntityMeta;
import fr.doan.achilles.metadata.ListPropertyMeta;
import fr.doan.achilles.metadata.MapPropertyMeta;
import fr.doan.achilles.metadata.PropertyMeta;
import fr.doan.achilles.metadata.SetPropertyMeta;

public class EntityPersister
{

	private BeanPropertyHelper beanPropertyHelper = new BeanPropertyHelper();

	public <ID extends Serializable> void persist(Object entity, EntityMeta<ID> entityMeta)
	{

		ID key = beanPropertyHelper.getKey(entity, entityMeta.getIdMeta());
		Validate.notNull(key, "key value for entity '" + entityMeta.getCanonicalClassName() + "'");
		GenericDao<ID> dao = entityMeta.getDao();

		Mutator<ID> mutator = dao.buildMutator();

		for (Entry<String, PropertyMeta<?>> entry : entityMeta.getPropertyMetas().entrySet())
		{
			PropertyMeta<?> propertyMeta = entry.getValue();
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

	public <ID extends Serializable> void remove(Object entity, EntityMeta<ID> entityMeta)
	{
		ID key = beanPropertyHelper.getKey(entity, entityMeta.getIdMeta());
		Validate.notNull(key, "key value for entity '" + entityMeta.getCanonicalClassName() + "'");
		GenericDao<ID> dao = entityMeta.getDao();
		dao.removeRow(key);

	}

	private <ID extends Serializable> void batchSimpleProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta,
			Mutator<ID> mutator)
	{
		Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), propertyMeta.propertyType(), 0);
		Object value = beanPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		dao.insertColumn(key, name, value, mutator);
	}

	public <ID extends Serializable> void persistSimpleProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta)
	{
		this.batchSimpleProperty(entity, key, dao, propertyMeta, null);
	}

	private <ID extends Serializable> void batchListProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta,
			Mutator<ID> mutator)
	{

		List<?> list = (List<?>) beanPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		int count = 0;
		if (list != null)
		{
			for (Object value : list)
			{
				Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), propertyMeta.propertyType(), count);
				dao.insertColumn(key, name, value, mutator);
				count++;
			}
		}
	}

	public <ID extends Serializable> void persistListProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchListProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	private <ID extends Serializable> void batchSetProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta,
			Mutator<ID> mutator)
	{
		Set<?> set = (Set<?>) beanPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		if (set != null)
		{
			for (Object value : set)
			{
				Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), propertyMeta.propertyType(), value.hashCode());
				dao.insertColumn(key, name, value, mutator);
			}
		}
	}

	public <ID extends Serializable> void persistSetProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchSetProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	private <ID extends Serializable> void batchMapProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta,
			Mutator<ID> mutator)
	{

		Map<?, ?> map = (Map<?, ?>) beanPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		if (map != null)
		{
			for (Entry<?, ?> entry : map.entrySet())
			{
				Composite name = dao
						.buildCompositeForProperty(propertyMeta.getPropertyName(), propertyMeta.propertyType(), entry.getKey().hashCode());

				KeyValueHolder value = new KeyValueHolder(entry.getKey(), entry.getValue());
				dao.insertColumn(key, name, value, mutator);
			}
		}
	}

	public <ID extends Serializable> void persistMapProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchMapProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	public <ID extends Serializable, V extends Serializable> void persistProperty(Object entity, ID key, GenericDao<ID> dao,
			PropertyMeta<V> propertyMeta)
	{

		switch (propertyMeta.propertyType())
		{
			case SIMPLE:
			case LAZY_SIMPLE:
				this.persistSimpleProperty(entity, key, dao, propertyMeta);
				break;
			case LIST:
			case LAZY_LIST:
				this.persistListProperty(entity, key, dao, (ListPropertyMeta<V>) propertyMeta);
				break;
			case SET:
			case LAZY_SET:
				this.persistSetProperty(entity, key, dao, (SetPropertyMeta<V>) propertyMeta);
				break;
			case MAP:
			case LAZY_MAP:
				this.persistMapProperty(entity, key, dao, (MapPropertyMeta<V>) propertyMeta);
				break;
			default:
				break;
		}
	}
}
