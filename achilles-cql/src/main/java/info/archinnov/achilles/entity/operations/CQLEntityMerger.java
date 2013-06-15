package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.operations.impl.CQLMergerImpl;

/**
 * CQLEntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLEntityMerger extends EntityMerger<CQLPersistenceContext>
{
	public CQLEntityMerger() {
		super.merger = new CQLMergerImpl();
		super.persister = new CQLEntityPersister();
		super.proxifier = new CQLEntityProxifier();
	}

}
