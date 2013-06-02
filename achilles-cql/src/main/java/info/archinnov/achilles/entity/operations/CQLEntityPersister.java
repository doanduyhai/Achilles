package info.archinnov.achilles.entity.operations;

import static javax.persistence.CascadeType.*;
import info.archinnov.achilles.context.AchillesPersistenceContext;
import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.entity.metadata.JoinProperties;
import info.archinnov.achilles.entity.operations.impl.CQLPersisterImpl;
import info.archinnov.achilles.validation.Validator;

import java.util.Set;

import javax.persistence.CascadeType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CQLPersisterImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityPersister implements AchillesEntityPersister
{
	private static final Logger log = LoggerFactory.getLogger(CQLEntityPersister.class);

	private CQLPersisterImpl persisterImpl;

	@Override
	public void persist(AchillesPersistenceContext context)
	{
		EntityMeta entityMeta = context.getEntityMeta();
		CQLPersistenceContext cqlContext = (CQLPersistenceContext) context;

		if (!entityMeta.isWideRow())
		{
			log.debug("Persisting transient entity {}", context.getEntity());

			persisterImpl.persist(this, cqlContext);
		}
	}

	public Object cascadePersistOrEnsureExist(CQLPersistenceContext context,
			JoinProperties joinProperties)
	{
		Object joinId = context.getPrimaryKey();

		Set<CascadeType> cascadeTypes = joinProperties.getCascadeTypes();
		if (cascadeTypes.contains(ALL) || cascadeTypes.contains(PERSIST))
		{
			log.debug("Cascade-persisting entity of class {} and primary key {} ", context
					.getEntityClass()
					.getCanonicalName(), context.getPrimaryKey());

			persist(context);
		}
		else if (context.getConfigContext().isEnsureJoinConsistency())
		{

			log.debug("Consistency check for join entity of class {} and primary key {} ", context
					.getEntityClass()
					.getCanonicalName(), context.getPrimaryKey());

			boolean entityExist = persisterImpl.doesEntityExist(context);
			Validator
					.validateTrue(
							entityExist,
							"The entity '"
									+ joinProperties.getEntityMeta().getClassName()
									+ "' with id '"
									+ joinId
									+ "' cannot be found. Maybe you should persist it first or enable CascadeType.PERSIST/CascadeType.ALL");
		}

		return joinId;
	}

	@Override
	public void remove(AchillesPersistenceContext context)
	{
		CQLPersistenceContext cqlContext = (CQLPersistenceContext) context;
		persisterImpl.remove(cqlContext);
	}
}
