package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.context.CQLPersistenceContext;
import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.entity.operations.EntityMerger;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * CQLMergerImpl
 * 
 * @author DuyHai DOAN
 * 
 */
public class CQLMergerImpl implements Merger<CQLPersistenceContext>
{
    private ReflectionInvoker invoker = new ReflectionInvoker();
    private PropertyMetaComparator comparator = new PropertyMetaComparator();

    @Override
    public void merge(CQLPersistenceContext context, Map<Method, PropertyMeta> dirtyMap)
    {
        if (dirtyMap.size() > 0)
        {
            List<PropertyMeta> sortedDirtyMetas = new ArrayList<PropertyMeta>(
                    dirtyMap.values());
            Collections.sort(sortedDirtyMetas, comparator);
            context.pushUpdateStatement(sortedDirtyMetas);
            dirtyMap.clear();
        }
    }

    @Override
    public void cascadeMerge(EntityMerger<CQLPersistenceContext> entityMerger,
            CQLPersistenceContext context, List<PropertyMeta> joinPMs)
    {
        Object entity = context.getEntity();
        for (PropertyMeta pm : joinPMs)
        {
            Object joinValue = invoker.getValueFromField(entity, pm.getGetter());
            if (joinValue != null)
            {
                if (pm.isJoinCollection())
                {
                    doCascadeCollection(entityMerger, context, pm, (Collection<?>) joinValue);
                }
                else if (pm.isJoinMap())
                {
                    Map<?, ?> joinMap = (Map<?, ?>) joinValue;
                    doCascadeCollection(entityMerger, context, pm, joinMap.values());
                }
                else
                {
                    doCascade(entityMerger, context, pm, joinValue);
                }
            }
        }
    }

    private void doCascadeCollection(EntityMerger<CQLPersistenceContext> entityMerger,
            CQLPersistenceContext context, PropertyMeta pm, Collection<?> joinCollection)
    {
        for (Object joinEntity : joinCollection)
        {
            doCascade(entityMerger, context, pm, joinEntity);
        }
    }

    private void doCascade(EntityMerger<CQLPersistenceContext> entityMerger,
            CQLPersistenceContext context, PropertyMeta pm, Object joinEntity)
    {
        if (joinEntity != null)
        {
            CQLPersistenceContext joinContext = context.createContextForJoin(pm.joinMeta(),
                    joinEntity);
            entityMerger.merge(joinContext, joinEntity);
        }
    }

    public static class PropertyMetaComparator implements Comparator<PropertyMeta>
    {
        @Override
        public int compare(PropertyMeta arg0, PropertyMeta arg1)
        {
            return arg0.getPropertyName().compareTo(arg1.getPropertyName());
        }

    }
}
