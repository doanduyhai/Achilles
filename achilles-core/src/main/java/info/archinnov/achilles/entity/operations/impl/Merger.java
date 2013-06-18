package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.context.PersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityMerger;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

/**
 * EntityMergerImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public interface Merger<CONTEXT extends PersistenceContext>
{

	public void merge(CONTEXT context, Map<Method, PropertyMeta<?, ?>> dirtyMap);

	public void cascadeMerge(EntityMerger<CONTEXT> entityMerger, CONTEXT context,
			List<PropertyMeta<?, ?>> joinPMs);
}
