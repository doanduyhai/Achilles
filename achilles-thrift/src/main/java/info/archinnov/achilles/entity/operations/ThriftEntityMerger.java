package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.context.ThriftPersistenceContext;
import info.archinnov.achilles.entity.operations.impl.ThriftMergerImpl;

/**
 * ThriftEntityMerger
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftEntityMerger extends EntityMerger<ThriftPersistenceContext>
{
	public ThriftEntityMerger() {
		super.merger = new ThriftMergerImpl();
		super.persister = new ThriftEntityPersister();
		super.proxifier = new ThriftEntityProxifier();

	}
}
