package info.archinnov.achilles.entity.operations;

import static info.archinnov.achilles.context.CQLDaoContext.ACHILLES_COUNTER_TABLE;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.context.CQLDaoContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.AchillesMethodInvoker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
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
		List<Statement> batchPersist = new ArrayList<Statement>();
		persist(batchPersist, context);
		if (batchPersist.size() > 1)
		{
			Batch batchQuery = QueryBuilder.batch(batchPersist.toArray(new Statement[batchPersist
					.size()]));
			session.execute(batchQuery);
		}
		else
		{
			session.execute(batchPersist.get(0));
		}
	}

	private void persist(List<Statement> batchPersist, AchillesPersistenceContext context)
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
				switch (pm.type())
				{
					case JOIN_SIMPLE:

						cascadePersistOrEnsureExist(batchPersist,
								context.newPersistenceContext(pm.joinMeta(), value));
						break;

					case JOIN_LIST:
					case JOIN_SET:
						Collection<?> collection = (Collection<?>) value;

						for (Object joinEntity : collection)
						{
							cascadePersistOrEnsureExist(batchPersist,
									context.newPersistenceContext(pm.joinMeta(), context
											.newPersistenceContext(pm.joinMeta(), joinEntity)));
						}

						break;

					case JOIN_MAP:

						Map<?, ?> map = (Map<?, ?>) value;
						for (Object joinEntity : map.values())
						{
							cascadePersistOrEnsureExist(batchPersist,
									context.newPersistenceContext(pm.joinMeta(), context
											.newPersistenceContext(pm.joinMeta(), joinEntity)));
						}
					default:
						break;
				}
			}

		}

		batchPersist.add(insert);
	}

	protected void cascadePersistOrEnsureExist(List<Statement> cascadePersist,
			AchillesPersistenceContext joinContext)
	{

	}

	@Override
	public void remove(AchillesPersistenceContext context)
	{
		Object primaryKey = context.getPrimaryKey();
		EntityMeta entityMeta = context.getEntityMeta();
		PropertyMeta<?, ?> idMeta = entityMeta.getIdMeta();

		Statement entityDelete = QueryBuilder
				.delete()
				.from(entityMeta.getTableName())
				.where(QueryBuilder.eq(idMeta.getPropertyName(), primaryKey));

		List<Statement> extraDeleteQueries = new ArrayList<Statement>();

		for (PropertyMeta<?, ?> pm : entityMeta.getPropertyMetas().values())
		{
			switch (pm.type())
			{
				case COUNTER:

					extraDeleteQueries.add(QueryBuilder
							.delete()
							.from(ACHILLES_COUNTER_TABLE)
							.where(QueryBuilder.eq(CQLDaoContext.ACHILLES_COUNTER_FQCN,
									entityMeta.getClassName()))
							.and(QueryBuilder.eq(CQLDaoContext.ACHILLES_COUNTER_PK,
									idMeta.writeValueToString(primaryKey))));
					break;

				case WIDE_MAP:
				case COUNTER_WIDE_MAP:
					extraDeleteQueries.add(QueryBuilder
							.delete()
							.from(pm.getExternalCFName())
							.where(QueryBuilder.eq(idMeta.getPropertyName(), primaryKey)));
				default:
					break;
			}
		}

		Batch batchDelete = QueryBuilder.batch(entityDelete);
		if (extraDeleteQueries.size() > 0)
		{
			for (Statement delete : extraDeleteQueries)
			{
				batchDelete.add(delete);
			}
		}

		session.execute(batchDelete);
	}
}
