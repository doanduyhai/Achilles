package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * CQLPersisterImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityPersister implements AchillesEntityPersister
{
	private Session session;
	private AchillesMethodInvoker invoker = new AchillesMethodInvoker();

	public CQLEntityPersister(Session session) {
		this.session = session;
	}

	@Override
	public void persist(AchillesPersistenceContext context)
	{
		Object entity = context.getEntity();
		Object primaryKey = context.getPrimaryKey();
		EntityMeta entityMeta = context.getEntityMeta();
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

		Insert insert = QueryBuilder.insertInto(entityMeta.getTableName());
		insert.value(idMeta.getPropertyName(), primaryKey);

		for (PropertyMeta pm : entityMeta.getPropertyMetas().values())
		{
			Object value = invoker.getValueFromField(entity, pm.getGetter());
			if (value != null)
			{
				insert.value(pm.getPropertyName(), pm.writeValueAsSupportedTypeOrString(value));
			}
		}

		session.execute(insert);
	}

	@Override
	public void remove(AchillesPersistenceContext context)
	{
		// TODO Auto-generated method stub

	}

}
