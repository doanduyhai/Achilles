package fr.doan.achilles.entity.operations;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.Validate;

import fr.doan.achilles.dao.GenericDao;
import fr.doan.achilles.entity.EntityPropertyHelper;
import fr.doan.achilles.entity.metadata.EntityMeta;
import fr.doan.achilles.entity.metadata.ListPropertyMeta;
import fr.doan.achilles.entity.metadata.MapPropertyMeta;
import fr.doan.achilles.entity.metadata.PropertyMeta;
import fr.doan.achilles.entity.metadata.SetPropertyMeta;
import fr.doan.achilles.holder.KeyValueHolder;

public class EntityPersister
{

	private EntityPropertyHelper entityPropertyHelper = new EntityPropertyHelper();

	public <ID> void persist(Object entity, EntityMeta<ID> entityMeta)
	{

		ID key = entityPropertyHelper.getKey(entity, entityMeta.getIdMeta());
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

	public <ID> void remove(Object entity, EntityMeta<ID> entityMeta)
	{
		ID key = entityPropertyHelper.getKey(entity, entityMeta.getIdMeta());
		Validate.notNull(key, "key value for entity '" + entityMeta.getCanonicalClassName() + "'");
		GenericDao<ID> dao = entityMeta.getDao();
		dao.removeRow(key);

	}

	private <ID> void batchSimpleProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta, Mutator<ID> mutator)
	{
		Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), propertyMeta.propertyType(), 0);
		Object value = entityPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		dao.insertColumn(key, name, value, mutator);
	}

	public <ID> void persistSimpleProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta)
	{
		this.batchSimpleProperty(entity, key, dao, propertyMeta, null);
	}

	private <ID> void batchListProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta, Mutator<ID> mutator)
	{

		List<?> list = (List<?>) entityPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
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

	public <ID> void persistListProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchListProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	private <ID> void batchSetProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta, Mutator<ID> mutator)
	{
		Set<?> set = (Set<?>) entityPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		if (set != null)
		{
			for (Object value : set)
			{
				Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), propertyMeta.propertyType(), value.hashCode());
				dao.insertColumn(key, name, value, mutator);
			}
		}
	}

	public <ID> void persistSetProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchSetProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	private <ID> void batchMapProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta, Mutator<ID> mutator)
	{

		Map<?, ?> map = (Map<?, ?>) entityPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
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

	public <ID> void persistMapProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<?> propertyMeta)
	{
		Mutator<ID> mutator = dao.buildMutator();
		this.batchMapProperty(entity, key, dao, propertyMeta, mutator);
		mutator.execute();
	}

	@SuppressWarnings("unchecked")
	public <ID, V> void persistProperty(Object entity, ID key, GenericDao<ID> dao, PropertyMeta<V> propertyMeta)
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
				this.persistMapProperty(entity, key, dao, (MapPropertyMeta<?, V>) propertyMeta);
				break;
			default:
				break;
		}
	}
}
