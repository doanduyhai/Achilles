package fr.doan.achilles.operations;

import static fr.doan.achilles.metadata.PropertyType.LIST;
import static fr.doan.achilles.metadata.PropertyType.MAP;
import static fr.doan.achilles.metadata.PropertyType.SET;
import static fr.doan.achilles.metadata.PropertyType.SIMPLE;

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
import fr.doan.achilles.metadata.PropertyMeta;

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
			if (propertyMeta.propertyType() == SIMPLE)
			{
				this.batchSimpleProperty(entity, key, dao, mutator, propertyMeta);
			}
			else if (propertyMeta.propertyType() == LIST)
			{
				this.batchListProperty(entity, key, dao, mutator, propertyMeta);
			}
			else if (propertyMeta.propertyType() == SET)
			{
				this.batchSetProperty(entity, key, dao, mutator, propertyMeta);
			}
			else if (propertyMeta.propertyType() == MAP)
			{
				this.batchMapProperty(entity, key, dao, mutator, propertyMeta);
			}
			mutator.execute();
		}

	}

	public <ID extends Serializable> void remove(Object entity, EntityMeta<ID> entityMeta)
	{
		ID key = beanPropertyHelper.getKey(entity, entityMeta.getIdMeta());
		Validate.notNull(key, "key value for entity '" + entityMeta.getCanonicalClassName() + "'");
		GenericDao<ID> dao = entityMeta.getDao();
		dao.removeRow(key);

	}

	private <ID extends Serializable> void batchSimpleProperty(Object entity, ID key, GenericDao<ID> dao, Mutator<ID> mutator,
			PropertyMeta<?> propertyMeta)
	{

		Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), SIMPLE, 0);
		Object value = beanPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		dao.insertColumnBatch(key, name, value, mutator);
	}

	private <ID extends Serializable> void batchListProperty(Object entity, ID key, GenericDao<ID> dao, Mutator<ID> mutator,
			PropertyMeta<?> propertyMeta)
	{

		List<?> list = (List<?>) beanPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		int count = 0;
		for (Object value : list)
		{
			Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), LIST, count);
			dao.insertColumnBatch(key, name, value, mutator);
			count++;
		}
	}

	private <ID extends Serializable> void batchSetProperty(Object entity, ID key, GenericDao<ID> dao, Mutator<ID> mutator,
			PropertyMeta<?> propertyMeta)
	{

		Set<?> set = (Set<?>) beanPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		for (Object value : set)
		{
			Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), SET, value.hashCode());
			dao.insertColumnBatch(key, name, value, mutator);
		}
	}

	private <ID extends Serializable> void batchMapProperty(Object entity, ID key, GenericDao<ID> dao, Mutator<ID> mutator,
			PropertyMeta<?> propertyMeta)
	{

		Map<?, ?> map = (Map<?, ?>) beanPropertyHelper.getValueFromField(entity, propertyMeta.getGetter());
		for (Entry<?, ?> entry : map.entrySet())
		{

			Composite name = dao.buildCompositeForProperty(propertyMeta.getPropertyName(), MAP, entry.getKey().hashCode());

			KeyValueHolder value = new KeyValueHolder(entry.getKey(), entry.getValue());
			dao.insertColumnBatch(key, name, value, mutator);
		}
	}
}
