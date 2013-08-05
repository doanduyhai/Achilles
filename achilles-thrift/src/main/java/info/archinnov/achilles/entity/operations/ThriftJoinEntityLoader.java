package info.archinnov.achilles.entity.operations;

import info.archinnov.achilles.dao.ThriftGenericEntityDao;
import info.archinnov.achilles.entity.ThriftEntityMapper;
import info.archinnov.achilles.entity.metadata.EntityMeta;
import info.archinnov.achilles.proxy.ReflectionInvoker;
import info.archinnov.achilles.validation.Validator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import me.prettyprint.hector.api.beans.Composite;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftJoinEntityHelper
 * 
 * @author DuyHai DOAN
 * 
 */
public class ThriftJoinEntityLoader
{
    private static final Logger log = LoggerFactory.getLogger(ThriftJoinEntityLoader.class);

    private ThriftEntityMapper mapper = new ThriftEntityMapper();
    private ReflectionInvoker invoker = new ReflectionInvoker();

    public <T, ID> Map<ID, T> loadJoinEntities(Class<T> entityClass, List<ID> keys,
            EntityMeta entityMeta, ThriftGenericEntityDao joinEntityDao)
    {
        if (log.isTraceEnabled())
        {
            log.trace("Load join entities of class {} with primary keys {}",
                    entityClass.getCanonicalName(), StringUtils.join(keys, ","));
        }

        Validator.validateNotNull(entityClass, "Entity class should not be null");
        Validator.validateNotEmpty(keys, "List of join primary keys '%s' should not be empty", keys);
        Validator.validateNotNull(entityMeta, "Entity meta for '%s' should not be null",
                entityClass.getCanonicalName());

        Map<ID, T> entitiesByKey = new HashMap<ID, T>();
        Map<ID, List<Pair<Composite, String>>> rows = joinEntityDao.eagerFetchEntities(keys);

        for (Entry<ID, List<Pair<Composite, String>>> entry : rows.entrySet())
        {
            T entity;
            entity = invoker.instanciate(entityClass);

            ID key = entry.getKey();
            List<Pair<Composite, String>> columns = entry.getValue();
            if (columns.size() > 0)
            {
                mapper.setEagerPropertiesToEntity(key, columns, entityMeta, entity);
                invoker.setValueToField(entity, entityMeta.getIdMeta().getSetter(), key);
                entitiesByKey.put(key, entity);
            }
        }
        return entitiesByKey;
    }
}
