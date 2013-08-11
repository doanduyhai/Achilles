package info.archinnov.achilles.entity.operations.impl;

import info.archinnov.achilles.entity.metadata.PropertyMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.utils.Pair;
import com.google.common.base.Function;

/**
 * JoinValueTransformer
 * 
 * @author DuyHai DOAN
 * 
 */
public class JoinValuesExtractor implements Function<PropertyMeta, Pair<List<?>, PropertyMeta>>
{
    private ReflectionInvoker invoker = new ReflectionInvoker();

    private Object entity;

    public JoinValuesExtractor(Object entity) {
        this.entity = entity;
    }

    @Override
    public Pair<List<?>, PropertyMeta> apply(PropertyMeta pm)
    {
        List<Object> joinValues = new ArrayList<Object>();
        Object joinValue = invoker.getValueFromField(entity, pm.getGetter());
        if (joinValue != null)
        {
            if (pm.isJoinCollection())
            {
                joinValues.addAll((Collection) joinValue);
            }
            else if (pm.isJoinMap())
            {
                Map<?, ?> joinMap = (Map<?, ?>) joinValue;
                joinValues.addAll(joinMap.values());
            }
            else
            {
                joinValues.add(joinValue);
            }
        }
        return Pair.<List<?>, PropertyMeta> create(joinValues, pm);
    }

}
